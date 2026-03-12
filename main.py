"""
Special Situations Scanner — NSE + BSE
Uses BSE's public XML announcement feed + NSE's public corporate actions API.
Runs daily via GitHub Actions → appends to results.csv.
Google Sheets reads the CSV via IMPORTDATA formula.
"""

import requests
import csv
import datetime
import time
import os
import xml.etree.ElementTree as ET

# ─── CONFIG ───────────────────────────────────────────────────────────────────

MARKET_CAP_MIN_CR = 1000

KEYWORDS = [
    "demerger", "de-merger", "spin off", "spin-off", "spinoff",
    "hive off", "scheme of arrangement", "scheme of demerger",
    "merger", "amalgamation", "resulting company",
    "open offer", "delisting", "takeover", "substantial acquisition",
    "partly paid", "call money", "call notice",
    "nclt", "national company law tribunal",
    "business transfer", "slump sale", "slump exchange",
    "restructur", "post-merger listing", "fresh listing",
    "record date", "rights issue", "buyback", "buy-back",
]

CATEGORY_MAP = {
    "Spin-off / Demerger":   ["demerger", "de-merger", "spin off", "spin-off", "spinoff", "hive off", "scheme of arrangement", "scheme of demerger"],
    "Merger / Amalgamation": ["merger", "amalgamation", "resulting company", "post-merger"],
    "Open Offer / Takeover": ["open offer", "takeover", "substantial acquisition", "delisting"],
    "Partly Paid / Rights":  ["partly paid", "call money", "call notice", "rights issue"],
    "NCLT / Post-Reorg":     ["nclt", "national company law tribunal", "business transfer", "slump sale"],
    "Record Date / Buyback": ["record date", "buyback", "buy-back"],
}

CSV_FILE = "results.csv"
HEADER   = ["Date", "Exchange", "Ticker", "Company", "Category", "Headline", "Market Cap (Cr)", "Source URL"]

# ─── HELPERS ──────────────────────────────────────────────────────────────────

def is_special_situation(text: str) -> bool:
    return any(kw in text.lower() for kw in KEYWORDS)

def categorize(text: str) -> str:
    t = text.lower()
    for category, keys in CATEGORY_MAP.items():
        if any(k in t for k in keys):
            return category
    return "Other Special Situation"

def get_market_cap_cr(ticker: str) -> float | None:
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        r   = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        mcap = r.json()["chart"]["result"][0]["meta"].get("marketCap")
        if mcap:
            return round(mcap / 1e7, 0)
    except Exception:
        pass
    return None

# ─── BSE XML FEED ─────────────────────────────────────────────────────────────

def fetch_bse_xml() -> list[dict]:
    urls = [
        "https://www.bseindia.com/xml-data/corpfiling/annexp/annexure.xml",
        "https://www.bseindia.com/xml-data/corpfiling/annexp/annexure1.xml",
    ]
    results = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/xml, text/xml, */*",
        "Referer": "https://www.bseindia.com/",
    }
    for url in urls:
        try:
            r = requests.get(url, headers=headers, timeout=20)
            r.raise_for_status()
            root = ET.fromstring(r.content)
            for item in root.iter("row"):
                results.append({
                    "headline":   item.findtext("HEADLINE") or item.findtext("NEWSSUB") or "",
                    "scrip_code": item.findtext("SCRIP_CD") or "",
                    "company":    item.findtext("SLONGNAME") or item.findtext("SCRIP_CD") or "",
                    "date":       (item.findtext("NEWS_DT") or item.findtext("DT_TM") or "")[:10],
                })
        except Exception as e:
            print(f"  ⚠️  BSE XML feed error ({url}): {e}")
    return results

def process_bse(raw: list[dict]) -> list[dict]:
    results = []
    today   = str(datetime.date.today())
    cutoff  = str(datetime.date.today() - datetime.timedelta(days=3))

    for ann in raw:
        headline = ann["headline"].strip()
        date     = ann["date"] or today
        if date < cutoff:
            continue
        if not headline or not is_special_situation(headline):
            continue

        scrip_code = ann["scrip_code"].strip()
        company    = ann["company"].strip()

        mcap = get_market_cap_cr(f"{scrip_code}.BO")
        if mcap is not None and mcap < MARKET_CAP_MIN_CR:
            continue

        results.append({
            "date":     date,
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

# ─── NSE CORPORATE ACTIONS ────────────────────────────────────────────────────

def fetch_nse_actions() -> list[dict]:
    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120",
        "Accept":     "application/json",
        "Referer":    "https://www.nseindia.com/",
        "Accept-Language": "en-US,en;q=0.9",
    }
    try:
        session.get("https://www.nseindia.com/companies-listing/corporate-filings-actions",
                    headers=headers, timeout=15)
        time.sleep(3)

        today   = datetime.date.today()
        from_dt = today - datetime.timedelta(days=7)
        url = (
            f"https://www.nseindia.com/api/corporates-corporateActions"
            f"?index=equities"
            f"&from_date={from_dt.strftime('%d-%m-%Y')}"
            f"&to_date={today.strftime('%d-%m-%Y')}"
        )
        r = session.get(url, headers=headers, timeout=20)
        r.raise_for_status()
        data = r.json()
        results = data.get("data", data) if isinstance(data, dict) else data
        print(f"   NSE corporate actions returned: {len(results)} rows")
        return results if isinstance(results, list) else []
    except Exception as e:
        print(f"  ⚠️  NSE corporate actions error: {e}")
        return []

def process_nse(raw: list[dict]) -> list[dict]:
    results = []
    for ann in raw:
        subject = (
            ann.get("subject") or ann.get("desc") or
            ann.get("purpose") or ann.get("type") or ""
        ).strip()
        print(f"   NSE row: {subject[:80]}")
        if not subject or not is_special_situation(subject):
            continue

        symbol   = (ann.get("symbol") or "").strip()
        company  = (ann.get("company") or ann.get("companyName") or symbol).strip()
        ann_date = (ann.get("exDate") or ann.get("an_dt") or ann.get("date") or "")[:10]

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

# ─── CSV ──────────────────────────────────────────────────────────────────────

def ensure_csv_exists():
    if not os.path.isfile(CSV_FILE):
        with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(HEADER)
        print(f"  📄 Created {CSV_FILE}")

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
    today = datetime.date.today()

    print(f"\n{'='*55}")
    print(f"  SPECIAL SITUATIONS SCAN — {today}")
    print(f"{'='*55}")

    ensure_csv_exists()
    all_rows = []

    print("\n📡 Fetching BSE XML feed...")
    bse_raw  = fetch_bse_xml()
    print(f"   Raw announcements : {len(bse_raw)}")
    bse_hits = process_bse(bse_raw)
    print(f"   After filters     : {len(bse_hits)}")
    all_rows.extend(bse_hits)

    print("\n📡 Fetching NSE corporate actions...")
    nse_raw  = fetch_nse_actions()
    print(f"   Raw rows          : {len(nse_raw)}")
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
