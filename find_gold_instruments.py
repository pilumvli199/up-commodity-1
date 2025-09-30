#!/usr/bin/env python3
"""
Find GOLD contracts in Upstox MCX instruments.
"""

import requests, gzip, io, json

MCX_URL = "https://assets.upstox.com/market-quote/instruments/exchange/MCX.json.gz"

def main():
    print("📥 Downloading MCX instruments...")
    r = requests.get(MCX_URL, timeout=60)
    r.raise_for_status()
    gz = gzip.GzipFile(fileobj=io.BytesIO(r.content))
    data_text = gz.read().decode('utf-8', errors='ignore').strip()

    try:
        items = json.loads(data_text)
    except Exception:
        # fallback: newline JSON
        items = [json.loads(line) for line in data_text.splitlines() if line.strip()]

    print("🔎 Searching for GOLD contracts...\n")
    for it in items:
        exch = str(it.get("exchange") or "").upper()
        if "MCX" not in exch:
            continue
        ts = it.get("trading_symbol") or ""
        name = it.get("name") or ""
        ik = it.get("instrument_key") or ""
        expiry = it.get("expiry") or it.get("expiry_date") or ""
        if "GOLD" in ts.upper() or "GOLD" in name.upper():
            print(f"{ik} | symbol='{ts}' | name='{name}' | expiry='{expiry}'")

if __name__ == "__main__":
    main()
