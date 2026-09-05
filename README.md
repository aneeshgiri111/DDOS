# DDOS


⚠️ STRICT WARNING

DO NOT USE THIS SCRIPT ON REAL WEBSITES, SERVERS, OR SYSTEMS WITHOUT EXPLICIT AUTHORIZATION.

Use it strictly for educational purposes and only on systems you own or have permission to test, such as localhost, a private lab, or a dedicated test server.

Uncontrolled/high-volume requests can overload a server, disrupt legitimate users, or be treated as a denial-of-service attack.

⚠️ COMPULSORY: Run this before running the script
pip install aiohttp

Open Windows PowerShell

1. Create a folder:

mkdir bot
2. Enter the folder:
cd bot

3. Create the Python file:

notepad  multi.py
and save the multi.py file 

in same folder create a file as p.bin

$bytes = New-Object byte[] (10MB)
$stream = [System.IO.File]::OpenWrite("$PWD\p.bin")
$stream.Write($bytes, 0, $bytes.Length)
$stream.Close()

 for single website pentesting
 
 python  multi.py https://A --workers 4 --cache-buster
 
 for multiple website and more aggressive
 
 python multi.py https://A https://B --max-reqs 200000 -k --method POST 
 
 python multi.py https://A https://B https://C --profile burst --workers 8

 

The purpose is to determine how the application behaves under controlled load, rather than to make a service unavailable.

⚖️ Legal & Ethical Notice

This project is intended for education, defensive security testing, and authorized performance testing only.

Never test a website, server, API, or network that you do not own or have explicit permission to test.

Unauthorized denial-of-service testing can cause real service disruption and may have legal consequences.

