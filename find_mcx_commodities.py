#!/usr/bin/env python3
"""
More aggressive helper: Search Upstox instruments CSV for MCX commodity contracts
Looks for keywords anywhere in trading_symbol, name or instrument_key, filters exchange=='MCX'
Prints up to N matches per keyword with helpful columns.
Set COMMODITY_KEYWORDS env to override defaults.
"""
import os, gzip, io, csv, requests, sys

CSV_URL = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"
KEYWORDS_RAW = os.getenv('COMMODITY_KEYWORDS') or "GOLD,SILVER,CRUDE,NATURAL GAS,NATURALGAS,NG,COPPER"
KEYWORDS = [k.strip() for k in KEYWORDS_RAW.split(",") if k.strip()]
MAX_PER_KEY = 200  # max lines per keyword to print

def download_rows():
    print("Downloading instruments CSV ...")
    r = requests.get(CSV_URL, timeout=60)
    r.raise_for_status()
    gz = gzip.GzipFile(fileobj=io.BytesIO(r.content))
    text = io.TextIOWrapper(gz, encoding='utf-8', errors='ignore')
    reader = csv.DictReader(text)
    rows = [row for row in reader]
    print("Loaded rows:", len(rows))
    return rows

def norm(s):
    return (s or "").upper()

def matches_row(row, kw):
    # check exchange equals MCX (case-insensitive) OR instrument_key contains MCX_COM etc
    exch = (row.get('exchange') or row.get('exchange_segment') or "").upper()
    ik = (row.get('instrument_key') or row.get('instrumentKey') or "").upper()
    ts = (row.get('trading_symbol') or row.get('symbol') or "").upper()
    name = (row.get('name') or row.get('instrument_name') or "").upper()
    kwu = kw.upper()
    # require MCX in exchange or instrument_key to focus on commodities
    if 'MCX' not in exch and 'MCX' not in ik:
        return False
    # match keyword anywhere in ts, name or ik (loose)
    if kwu in ts or kwu in name or kwu in ik:
        return True
    # also try splitting numeric/alpha: e.g. 'GOLDM' matching 'GOLD'
    if any(kwu in part for part in [ts, name, ik]):
        return True
    return False

def print_matches(rows):
    for kw in KEYWORDS:
        print("\n=== Matches for keyword:", kw, "===\n")
        count = 0
        for row in rows:
            if matches_row(row, kw):
                ik = row.get('instrument_key') or row.get('instrumentKey') or ""
                ts = row.get('trading_symbol') or row.get('symbol') or ""
                name = row.get('name') or row.get('instrument_name') or ""
                exch = row.get('exchange') or row.get('exchange_segment') or ""
                expiry = row.get('expiry') or row.get('expiry_date') or row.get('expiryMonth') or ""
                print(f"{ik} | symbol='{ts}' | name='{name}' | exchange='{exch}' | expiry='{expiry}'")
                count += 1
                if count >= MAX_PER_KEY:
                    print(f"... printed {MAX_PER_KEY} matches, stop for this keyword.")
                    break
        if count == 0:
            print("  (no MCX matches found for this keyword)")

def main():
    rows = download_rows()
    print_matches(rows)

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print("Error:", e)
        sys.exit(1)
