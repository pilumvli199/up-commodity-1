#!/usr/bin/env python3
"""
Helper script: Find Upstox instrument_key for commodities (GOLD, SILVER, CRUDEOIL, NG, COPPER etc.)
It downloads complete.csv.gz from Upstox and searches by keywords.
"""

import os, gzip, io, csv, requests, sys

CSV_URL = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"

# You can change COMMODITY_KEYWORDS in .env or directly edit below
KEYWORDS_RAW = os.getenv('COMMODITY_KEYWORDS') or "GOLD,SILVER,CRUDE,OIL,NATURALGAS,NG,COPPER"
KEYWORDS = [k.strip() for k in KEYWORDS_RAW.split(",") if k.strip()]

def download_rows():
    print("📥 Downloading instruments CSV ... (this may take a few seconds)")
    r = requests.get(CSV_URL, timeout=60)
    r.raise_for_status()
    gz = gzip.GzipFile(fileobj=io.BytesIO(r.content))
    text = io.TextIOWrapper(gz, encoding='utf-8', errors='ignore')
    reader = csv.DictReader(text)
    rows = [row for row in reader]
    print("✅ Rows loaded:", len(rows))
    return rows

def normalize(s):
    return (s or "").upper().replace(" ", "").replace(".", "").replace("&","AND").replace("-","")

def find_candidates(rows, keyword):
    k = normalize(keyword)
    candidates = []
    for row in rows:
        ts = normalize(row.get('trading_symbol') or row.get('symbol') or "")
        name = normalize(row.get('name') or row.get('instrument_name') or "")
        ik = (row.get('instrument_key') or row.get('instrumentKey') or "").strip()
        exch = (row.get('exchange') or row.get('exchange_segment') or "").strip()
        if k in ts or k in name or k in ik.upper():
            candidates.append((ik, row.get('trading_symbol') or row.get('symbol') or "",
                               row.get('name') or "", exch))
    return candidates

def main():
    rows = download_rows()
    for kw in KEYWORDS:
        print("\n=== Candidates for keyword:", kw, "===\n")
        cands = find_candidates(rows, kw)
        if not cands:
            print("  (no matches found)")
            continue
        seen = set()
        out = []
        for ik, ts, name, exch in cands:
            if ik and ik not in seen:
                seen.add(ik)
                out.append((ik, ts, name, exch))
        for ik, ts, name, exch in out[:40]:  # show only top 40 to avoid spam
            print(f"{ik} | trading_symbol='{ts}' | name='{name}' | exchange='{exch}'")
        if len(out) > 40:
            print(f"... ({len(out)} total candidates; showing first 40)")

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print("❌ Error:", e)
        sys.exit(1)
