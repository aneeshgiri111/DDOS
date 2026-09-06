# DDOS

⚠️ STRICT WARNING

DO NOT USE THIS SCRIPT ON REAL WEBSITES, SERVERS, OR SYSTEMS WITHOUT EXPLICIT AUTHORIZATION.

Use it strictly for educational purposes and only on systems you own or have permission to test, such as localhost, a private lab, or a dedicated test server.

Uncontrolled/high-volume requests can overload a server, disrupt legitimate users, or be treated as a denial-of-service attack.

---

## 📋 Prerequisites

⚠️ COMPULSORY: Run this before running the script

```powershell
pip install aiohttp
```

---

## 🛠️ Installation & Usage

Open Windows PowerShell

**Step 1:** Create a folder

```powershell
mkdir bot
```

**Step 2:** Enter the folder

```powershell
cd bot
```

**Step 3:** Create the Python file

```powershell
notepad swarm.py
```

Save the `swarm.py` file.

**Step 4:** Create payload file

```powershell
notepad p.bin
```

Or generate payload file using PowerShell:

```powershell
$bytes = New-Object byte[] (10MB)
$stream = [System.IO.File]::OpenWrite("$PWD\p.bin")
$stream.Write($bytes, 0, $bytes.Length)
$stream.Close()
```

---

## 🚀 Basic Usage

**For single website testing:**

```powershell
python swarm.py https://A --profile burst --workers 4
```



**For multiple websites:**

```powershell
python swarm.py https://A https://B https://C --profile burst --workers 8
```

**For aggressive testing:**

```powershell
python swarm.py https://A --profile burst --workers 16 --max-reqs 200000
```

**Save results to JSON file:**

```powershell
python swarm.py https://A --profile sustained --json results.json
```

**Custom bot count and duration:**

```powershell
python swarm.py https://A --profile burst --bots 1000 --duration 600
```

**All options combined:**

```powershell
python swarm.py https://A https://B --profile burst --bots 750 --duration 120 --json results.json --workers 8
```

## 🎯 Purpose

The purpose is to determine how the application behaves under controlled load, rather than to make a service unavailable.

---

## ⚖️ Legal & Ethical Notice

This project is intended for education, defensive security testing, and authorized performance testing only.

Never test a website, server, API, or network that you do not own or have explicit permission to test.

Unauthorized denial-of-service testing can cause real service disruption and may have legal consequences.

---

## ⚠️ STRICT WARNING

**Use this only on localhost, a private lab, or infrastructure you have explicit permission to test. Do not run it against real/third-party websites without authorization.**
