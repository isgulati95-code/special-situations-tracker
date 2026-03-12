"""
Special Situations Scanner — NSE + BSE
Runs daily via GitHub Actions.
Filters announcements by keyword + market cap,
appends results to results.csv in the repo.
Google Sheets pulls from this CSV automatically via IMPORTDATA formula.
"""

import requests
import csv
import datetime
import time
import os

# ─── CONFIG ───────────────────────────────────────────────────────────────────

MARKET_CAP_MIN_CR = 1000  # Drop companies below this (INR Crores)

KEYWORDS = [
    "demerger", "de-merger", "spin off", "spin-off", "spinoff",
    "hive off", "scheme of arrangement", "scheme of demerger",
    "merger", "amalgamation", "resulting company",
    "open offer", "delisting", "takeover", "substantial acquisition",
    "partly paid", "call money", "call notice",
    "nclt", "national company law tribunal",
    "business transfer", "slump sale", "slump exchange",
    "restructur", "post-merger listing", "fresh listing",
]

CATEGORY_MAP = {
    "Spin-off / Demerger":   ["demerger", "de-merger", "spin off", "spin-off", "spinoff", "hive off", "scheme of arrangement", "scheme of demerger"],
    "Merger / Amalgamation": ["merger", "amalgamation", "resulting company", "post-merger"],
    "Open Offer / Takeover": ["open offer", "takeover", "substantial acquisition", "delisting"],
    "Partly Paid / Rights":  ["partly paid", "call money", "call notice", "rights issue"],
    "NCLT / Post-Reorg":     ["nclt", "national company law tribunal", "business transfer", "slump sale"],
}

CSV_FILE = "results.csv"

HEADER = [
    "Date", "Exchange", "Ticker", "Company",
    "Category", "Headline", "Market Cap (Cr)", "Source URL"
]

# ─── HELPERS ──────────────────────────────────────────────────────────────────

def is_special_situation(text: str) -> bool:
    t = text.lower()
    return any(kw in t for kw in KEYWORDS)

def categorize(text: str) -> str:
    t = text.lower()
    for category, keys in CATEGORY_MAP.items():
        if any(k in t for k in keys):
            return category
    return "Other Special Situation"

def get_market_cap_cr(ticker: str) -> float | None:
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, headers=headers, timeout=10)
        data = r.json()
        mcap = data["chart"]["result"][0]["meta"].get("marketCap")
        if mcap:
            return round(mcap / 1e7, 0)
    except Exception:
        pass
    return None

# ─── BSE ──────────────────────────────────────────────────────────────────────

def fetch_bse(days_back: int = 1) -> list[dict]:
    today   = datetime.date.today()
    from_dt = today - datetime.timedelta(days=days_back)
    url     = "https://api.bseindia.com/BseIndiaAPI/api/AnnGetData/w"
    params  = {
        "strCat": "-1", "strType": "C", "strScrip": "",
        "strSearch": "", "strTodt": today.strftime("%Y%m%d"),
        "strFromdt": from_dt.strftime("%Y%m%d"), "bseliveFlag": "0"
    }
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.bseindia.com/",
        "Accept": "application/json",
    }
    try:
        r = requests.get(url, params=params, headers=headers, timeout=20)
        r.raise_for_status()
        return r.json().get("Table", [])
    except Exception as e:
        print(f"  ⚠️  BSE fetch error: {e}")
        return []

def process_bse(raw: list[dict]) -> list[dict]:
    results = []
    for ann in raw:
        headline = (ann.get("HEADLINE") or ann.get("NEWSSUB") or "").strip()
        if not headline or not is_special_situation(headline):
            continue

        scrip_code = str(ann.get("SCRIP_CD", "")).strip()
        company    = (ann.get("SLONGNAME") or scrip_code).strip()
        ann_date   = (ann.get("NEWS_DT") or ann.get("DT_TM") or "")[:10]

        mcap = get_market_cap_cr(f"{scrip_code}.BO")
        if mcap is not None and mcap < MARKET_CAP_MIN_CR:
            continue

        results.append({
            "date":     ann_date or str(datetime.date.today()),
            "exchange": "BSE",
            "ticker":   scrip_code,
            "company":  company,
            "category": categorize(headline),
            "headline": headline,
            "mcap":     str(int(mcap)) if mcap else "N/A",
            "url":      f"https://www.bseindia.com/corporates/ann.html?scrip={scrip_code}",
        })
        time.sleep(0.3)
    return results

# ─── NSE ──────────────────────────────────────────────────────────────────────

def fetch_nse() -> list[dict]:
    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120",
        "Accept":     "application/json, text/plain, */*",
        "Referer":    "https://www.nseindia.com/",
    }
    try:
        session.get("https://www.nseindia.com", headers=headers, timeout=15)
        time.sleep(2)
        r = session.get(
            "https://www.nseindia.com/api/corporate-announcements?index=equities",
            headers=headers, timeout=20
        )
        r.raise_for_status()
        data = r.json()
        return data if isinstance(data, list) else []
    except Exception as e:
        print(f"  ⚠️  NSE fetch error: {e}")
        return []

def process_nse(raw: list[dict]) -> list[dict]:
    results = []
    for ann in raw:
        subject = (ann.get("subject") or ann.get("desc") or "").strip()
        if not subject or not is_special_situation(subject):
            continue

        symbol   = (ann.get("symbol") or "").strip()
        company  = (ann.get("company") or symbol).strip()
        ann_date = (ann.get("an_dt") or ann.get("date") or "")[:10]

        mcap = get_market_cap_cr(f"{symbol}.NS")
        if mcap is not None and mcap < MARKET_CAP_MIN_CR:
            continue

        results.append({
            "date":     ann_date or str(datetime.date.today()),
            "exchange": "NSE",
            "ticker":   symbol,
            "company":  company,
            "category": categorize(subject),
            "headline": subject,
            "mcap":     str(int(mcap)) if mcap else "N/A",
            "url":      f"https://www.nseindia.com/get-quotes/equity?symbol={symbol}",
        })
        time.sleep(0.3)
    return results

# ─── CSV WRITE ────────────────────────────────────────────────────────────────

def ensure_csv_exists():
    """Always create the CSV with a header if it doesn't exist yet."""
    if not os.path.isfile(CSV_FILE):
        with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(HEADER)
        print(f"  📄 Created fresh {CSV_FILE} with header row")

def append_to_csv(rows: list[dict]):
    with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for r in rows:
            writer.writerow([
                r["date"], r["exchange"], r["ticker"], r["company"],
                r["category"], r["headline"], r["mcap"], r["url"],
            ])
    print(f"  ✅ Appended {len(rows)} row(s) to {CSV_FILE}")

# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    today     = datetime.date.today()
    days_back = 3 if today.weekday() == 0 else 1  # Monday = look back 3 days

    print(f"\n{'='*55}")
    print(f"  SPECIAL SITUATIONS SCAN — {today}  (lookback: {days_back}d)")
    print(f"{'='*55}")

    # Always make sure the CSV file exists before anything else
    ensure_csv_exists()

    all_rows = []

    print("\n📡 Fetching BSE...")
    bse_raw  = fetch_bse(days_back=days_back)
    print(f"   Raw announcements : {len(bse_raw)}")
    bse_hits = process_bse(bse_raw)
    print(f"   After filters     : {len(bse_hits)}")
    all_rows.extend(bse_hits)

    print("\n📡 Fetching NSE...")
    nse_raw  = fetch_nse()
    print(f"   Raw announcements : {len(nse_raw)}")
    print("\n   --- NSE RAW SUBJECTS ---")
    for ann in nse_raw[:20]:
        subject = (ann.get("subject") or ann.get("desc") or "NO SUBJECT").strip()
        print(f"   {subject}")
    print("   --- END ---\n")
    nse_hits = process_nse(nse_raw)
    print(f"   After filters     : {len(nse_hits)}")
    all_rows.extend(nse_hits)

    print(f"\n{'─'*55}")
    print(f"  TOTAL FOUND: {len(all_rows)}")
    print(f"{'─'*55}\n")

    if all_rows:
        for r in all_rows:
            print(f"  [{r['exchange']}] {r['ticker']:12s} | {r['category']:25s} | ₹{r['mcap']} Cr")
        append_to_csv(all_rows)
    else:
        print("  ✅ No special situations today. CSV unchanged.")

    print("\nDone.\n")

if __name__ == "__main__":
    main()
