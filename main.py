#!/usr/bin/env python3
"""
myneta_scraper.py
=================
Scrapes candidate details from https://www.myneta.info/LokSabha2024/
Extracts for every candidate:
  - Basic info (name, party, age, constituency, etc.)
  - Criminal cases  (IPC sections + charges)
  - Immovable assets  (NOT movable)
  - Liabilities

Usage examples
--------------
# Single candidate
python myneta_scraper.py --candidate_id 6163

# All candidates in one constituency (e.g. KADAPA = 22)
python myneta_scraper.py --constituency_id 22

# All constituencies in one state (e.g. ANDHRA PRADESH = 2)
python myneta_scraper.py --state_id 2

# Entire Lok Sabha 2024 (all states/UTs)
python myneta_scraper.py --all

Output: CSV  →  output_candidates.csv
        JSON →  output_candidates.json
"""

# from matplotlib.pylab import record
import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import time
import re
import argparse
import sys
from pathlib import Path

# ──────────────────────────────────────────────
#  CONFIG
# ──────────────────────────────────────────────
BASE_URL   = "https://www.myneta.info/LokSabha2024"
HEADERS    = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
}
DELAY_SEC  = 1.5   # polite crawl delay between requests
OUTPUT_CSV  = "output_candidates.csv"
OUTPUT_JSON = "output_candidates.json"


# ──────────────────────────────────────────────
#  HELPER: clean text
# ──────────────────────────────────────────────
def clean(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text.strip())


def safe_get(url: str, retries: int = 3) -> requests.Response | None:
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=20)
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            print(f"  [WARN] Attempt {attempt+1} failed for {url}: {e}")
            time.sleep(2 * (attempt + 1))
    print(f"  [ERROR] Could not fetch: {url}")
    return None


# ──────────────────────────────────────────────
#  STEP 1: Discover all constituency IDs
# ──────────────────────────────────────────────
def get_all_constituencies() -> list[dict]:
    """Scrape homepage to get all constituency IDs."""
    url  = f"{BASE_URL}/"
    resp = safe_get(url)
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    constituencies = []

    for a in soup.find_all("a", href=True):
        href = a["href"]
        m = re.search(r"constituency_id=(\d+)", href)
        if m and "show_candidates" in href:
            constituencies.append({
                "constituency_id": int(m.group(1)),
                "constituency_name": clean(a.get_text())
            })

    # deduplicate
    seen = set()
    unique = []
    for c in constituencies:
        if c["constituency_id"] not in seen:
            seen.add(c["constituency_id"])
            unique.append(c)

    print(f"[INFO] Found {len(unique)} constituencies")
    return unique


# ──────────────────────────────────────────────
#  STEP 2: Get constituency IDs for a state
# ──────────────────────────────────────────────
def get_constituencies(state_id: int) -> list[dict]:
    url  = f"{BASE_URL}/index.php?action=show_constituencies&state_id={state_id}"
    resp = safe_get(url)
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    constituencies = []

    for a in soup.find_all("a", href=True):
        href = a["href"]
        m = re.search(r"constituency_id=(\d+)", href)
        if m and "show_candidates" in href:
            constituencies.append({
                "constituency_id": int(m.group(1)),
                "constituency_name": clean(a.get_text()),
                "state_id": state_id,
            })

    # deduplicate
    seen = set()
    unique = []
    for c in constituencies:
        if c["constituency_id"] not in seen:
            seen.add(c["constituency_id"])
            unique.append(c)

    print(f"  [INFO] State {state_id}: {len(unique)} constituencies")
    return unique


# ──────────────────────────────────────────────
#  STEP 3: Get candidate IDs for a constituency
# ──────────────────────────────────────────────
def get_candidate_ids(constituency_id: int) -> list[dict]:
    url  = f"{BASE_URL}/index.php?action=show_candidates&constituency_id={constituency_id}"
    resp = safe_get(url)
    if not resp:
        return []

    soup       = BeautifulSoup(resp.text, "html.parser")
    candidates = []

    for a in soup.find_all("a", href=True):
        href = a["href"]
        m = re.search(r"candidate_id=(\d+)", href)
        if m and "candidate.php" in href:
            candidates.append({
                "candidate_id"     : int(m.group(1)),
                "name_from_list"   : clean(a.get_text()),
                "constituency_id"  : constituency_id,
            })

    # deduplicate
    seen   = set()
    unique = []
    for c in candidates:
        if c["candidate_id"] not in seen:
            seen.add(c["candidate_id"])
            unique.append(c)

    print(f"    [INFO] Constituency {constituency_id}: {len(unique)} candidates")
    return unique


# ──────────────────────────────────────────────
#  STEP 4: Parse a single candidate page
# ──────────────────────────────────────────────
def parse_candidate(candidate_id: int) -> dict:
    url  = f"{BASE_URL}/candidate.php?candidate_id={candidate_id}"
    resp = safe_get(url)
    if not resp:
        return {"candidate_id": candidate_id, "error": "fetch_failed"}

    soup = BeautifulSoup(resp.text, "html.parser")

    record = {
        "candidate_id"       : candidate_id,
        "source_url"         : url,
        # ── Basic info ──────────────────────────
        "name"               : "",
        "constituency"       : "",
        "state"              : "",
        "party"              : "",
        "parentage"          : "",
        "age"                : "",
        "voter_enrolled_in"  : "",
        "self_profession"    : "",
        "spouse_profession"  : "",
        "education"          : "",
        "status"             : "",
        # ── Criminal ────────────────────────────
        "criminal_cases_count"  : 0,
        "convictions_count"     : 0,
        "criminal_cases_detail" : "",   # JSON string of list
        # ── Immovable assets ────────────────────
        "immovable_assets_total_self"    : "",
        "immovable_assets_total_spouse"  : "",
        "immovable_assets_grand_total"   : "",
        "immovable_assets_detail"        : "",   # JSON string of list
        # ── Liabilities ─────────────────────────
        "liabilities_total"  : "",
        "liabilities_detail" : "",   # JSON string
    }

    # ── NAME + CONSTITUENCY + STATE ──────────────────────────────────────
    h2 = soup.find("h2")
    if h2:
        record["name"] = clean(h2.get_text())

    h5 = soup.find("h5")
    if h5:
        parts = h5.get_text(separator="|").split("|")
        if len(parts) >= 2:
            record["constituency"] = clean(parts[0])
            record["state"]        = clean(parts[1].strip("()"))
        elif len(parts) == 1:
            record["constituency"] = clean(parts[0])

    # ── BASIC FIELDS from bold labels ────────────────────────────────────
    for b_tag in soup.find_all("b"):
        label = clean(b_tag.get_text()).rstrip(":")

        # safer value extraction
        parent_text = clean(b_tag.parent.get_text())
        value = clean(parent_text.replace(b_tag.get_text(), "", 1))

        if label == "Party":
            record["party"] = value
        elif label in ("S/o", "D/o", "W/o"):
            record["parentage"] = value
        elif label == "Age":
            record["age"] = value
        elif label == "Name Enrolled as Voter in":
            record["voter_enrolled_in"] = value
        elif label == "Self Profession":
            record["self_profession"] = value
        elif label == "Spouse Profession":
            record["spouse_profession"] = value
        elif label == "Status":
            record["status"] = value


    # ── EDUCATION ─────────────────────────────────────────────────────────
    edu_header = soup.find(
        lambda t: t.name in ("h3", "h4") and "Educational" in t.get_text()
    )
    if edu_header:
        edu_text_parts = []
        for sib in edu_header.find_next_siblings():
            if sib.name in ("h3", "h4"):
                break
            t = clean(sib.get_text())
            if t:
                edu_text_parts.append(t)
        record["education"] = " | ".join(edu_text_parts)

    # ── CRIMINAL CASES ────────────────────────────────────────────────────
    criminal_section = soup.find(
        lambda t: t.name in ("h3", "h4") and "Criminal Cases" in t.get_text()
    )
    cases = []

    if criminal_section:
        # "No criminal cases" check
        no_crime_text = ""
        for sib in criminal_section.find_next_siblings():
            if sib.name in ("h3", "h4"):
                break
            no_crime_text += sib.get_text()

        if "No criminal cases" in no_crime_text:
            record["criminal_cases_count"]  = 0
            record["criminal_cases_detail"] = "[]"
        else:
            # Look for table rows describing cases
            crime_table = criminal_section.find_next("table")
            if crime_table:
                rows = crime_table.find_all("tr")
                current_case = {}
                for row in rows:
                    cells = [clean(c.get_text()) for c in row.find_all(["td", "th"])]
                    if not any(cells):
                        continue
                    text = " | ".join(cells)

                    # Common patterns in MyNeta crime tables
                    if re.search(r"(case\s*no|f\.?i\.?r\.?|crime\s*no)", text, re.I):
                        if current_case:
                            cases.append(current_case)
                        current_case = {"raw": text, "ipc_sections": [], "charges": []}
                    elif current_case:
                        # Try to extract IPC sections
                        ipc_matches = re.findall(
                            r"(?:IPC\s*(?:Section|Sec\.?|Sections)?\s*|u/s\s*|Section\s*)"
                            r"([\dA-Za-z/,\-\s]+)",
                            text,
                            re.IGNORECASE
                        )

                        for m in ipc_matches:
                            parts = re.split(r"[,\s/]+", m)
                            current_case["ipc_sections"].extend(p for p in parts if p)


                        # Charge descriptions (lines that don't look like numbers)
                        if len(text) > 10 and not re.match(r"^\d+[\s|]+$", text):
                            current_case["charges"].append(text)

                if current_case:
                    cases.append(current_case)
            convictions = 0
            conviction_text = soup.get_text(" ", strip=True)

            m = re.search(r"(\d+)\s+conviction", conviction_text, re.I)
            if m:
                convictions = int(m.group(1))

            record["convictions_count"] = convictions
            record["criminal_cases_count"]  = len(cases)
            record["criminal_cases_detail"] = json.dumps(cases, ensure_ascii=False)

    else:
        record["criminal_cases_detail"] = "[]"

    # ── IMMOVABLE ASSETS ──────────────────────────────────────────────────
    immovable_section = soup.find(
        lambda t: t.name in ("h3", "h4") and "Immovable" in t.get_text()
    )
    immovable_rows = []

    if immovable_section:
        imm_table = immovable_section.find_next("table")
        if imm_table:
            headers = []
            all_rows = imm_table.find_all("tr")

            for i, row in enumerate(all_rows):
                cells = [clean(c.get_text()) for c in row.find_all(["td", "th"])]
                if not any(cells):
                    continue

                # Capture header row
                if i == 0 or all(c == "" or re.match(r"^(Sr|Description|self|spouse|huf|dependent|Total)", c, re.I) for c in cells if c):
                    headers = cells
                    continue

                row_dict = {}
                for j, cell in enumerate(cells):
                    key = headers[j] if j < len(headers) else f"col_{j}"
                    row_dict[key] = cell

                # Skip empty or pure-total rows
                desc = row_dict.get("Description", "") or (cells[1] if len(cells) > 1 else "")
                if desc and not re.match(r"^(Sr\s*No|Total|Grand)", desc, re.I):
                    immovable_rows.append(row_dict)

                # Capture grand total row
                if any(re.search(r"Grand\s*Total", str(v), re.I) for v in cells):
                    # Last column or the one labeled self/spouse
                    for k, v in row_dict.items():
                        if "self" in k.lower() and v:
                            record["immovable_assets_total_self"] = v
                        if "spouse" in k.lower() and v:
                            record["immovable_assets_total_spouse"] = v
                    # Overall total is usually the last cell
                    last_val = [c for c in cells if c][-1]
                    record["immovable_assets_grand_total"] = last_val

    record["immovable_assets_detail"] = json.dumps(immovable_rows, ensure_ascii=False)

    # ── LIABILITIES ───────────────────────────────────────────────────────
    liabilities_section = soup.find(
        lambda t: t.name in ("h3", "h4") and "Liabilit" in t.get_text()
    )
    liabilities_dict = {}

    if liabilities_section:
        lib_table = liabilities_section.find_next("table")
        if lib_table:
            for row in lib_table.find_all("tr"):
                cells = [clean(c.get_text()) for c in row.find_all(["td", "th"])]
                cells = [c for c in cells if c]
                if len(cells) >= 2:
                    key = cells[0]
                    val = cells[-1]   # usually last column is total
                    liabilities_dict[key] = val

                # Grand total row
                row_text = " ".join(cells)
                if re.search(r"(Grand\s*Total|Total\s+Liabilit)", row_text, re.I):
                    record["liabilities_total"] = cells[-1]


    record["liabilities_detail"] = json.dumps(liabilities_dict, ensure_ascii=False)

    # Also capture the quick summary shown at top of page
    # (Assets: Rs X  /  Liabilities: Rs X or Nil)
    for b_tag in soup.find_all("b"):
        txt = clean(b_tag.get_text())
        if txt.startswith("Rs ") or txt == "Nil":
            prev = b_tag.find_previous(string=True)
            if prev:
                prev_clean = clean(str(prev))
                if "Assets" in prev_clean and not record["immovable_assets_grand_total"]:
                    pass   # total assets (movable+immovable combined) – skip per user request
                if "Liabilities" in prev_clean and not record["liabilities_total"]:
                    record["liabilities_total"] = txt

    print(f"      ✓ Parsed: {record['name'] or candidate_id}  "
          f"| Criminal: {record['criminal_cases_count']}  "
          f"| Immovable: {record['immovable_assets_grand_total'] or 'N/A'}"
          f"  | Liabilities: {record['liabilities_total'] or 'Nil'}")

    return record


# ──────────────────────────────────────────────
#  SAVE RESULTS
# ──────────────────────────────────────────────
def save_results(records: list[dict], csv_path: str = OUTPUT_CSV, json_path: str = OUTPUT_JSON):
    if not records:
        print("[WARN] No records to save.")
        return

    # JSON
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    print(f"\n[SAVED] JSON → {json_path}  ({len(records)} records)")

    # CSV (flatten JSON columns)
    df = pd.DataFrame(records)
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"[SAVED] CSV  → {csv_path}")

    # Quick summary
    total_criminal = sum(r.get("criminal_cases_count", 0) for r in records)
    print(f"\n── SUMMARY ─────────────────────────────────────────")
    print(f"  Total candidates scraped : {len(records)}")
    print(f"  Candidates with criminal cases : {sum(1 for r in records if r.get('criminal_cases_count', 0) > 0)}")
    print(f"  Total criminal cases     : {total_criminal}")
    print(f"────────────────────────────────────────────────────")


# ──────────────────────────────────────────────
#  ORCHESTRATORS
# ──────────────────────────────────────────────
def scrape_single(candidate_id: int) -> list[dict]:
    print(f"[MODE] Single candidate: {candidate_id}")
    record = parse_candidate(candidate_id)
    return [record]


def scrape_constituency(constituency_id: int) -> list[dict]:
    print(f"[MODE] Constituency ID: {constituency_id}")
    candidates = get_candidate_ids(constituency_id)
    records    = []
    for i, c in enumerate(candidates, 1):
        print(f"  [{i}/{len(candidates)}] candidate_id={c['candidate_id']}")
        record = parse_candidate(c["candidate_id"])
        records.append(record)
        time.sleep(DELAY_SEC)
    return records


def scrape_state(state_id: int) -> list[dict]:
    print(f"[MODE] State ID: {state_id}")
    constituencies = get_constituencies(state_id)
    records        = []
    for ci, con in enumerate(constituencies, 1):
        print(f"\n  Constituency [{ci}/{len(constituencies)}]: {con['constituency_name']}")
        candidates = get_candidate_ids(con["constituency_id"])
        for j, c in enumerate(candidates, 1):
            print(f"    [{j}/{len(candidates)}] candidate_id={c['candidate_id']}")
            record = parse_candidate(c["candidate_id"])
            record["constituency_name"] = con["constituency_name"]
            records.append(record)
            time.sleep(DELAY_SEC)
        time.sleep(DELAY_SEC)
    return records


def scrape_all() -> list[dict]:
    print("[MODE] ALL constituencies – full Lok Sabha 2024")
    constituencies = get_all_constituencies()
    records = []

    for i, con in enumerate(constituencies, 1):
        print(f"\n═══ Constituency [{i}/{len(constituencies)}]: {con['constituency_name']} ═══")
        candidates = get_candidate_ids(con["constituency_id"])

        for j, c in enumerate(candidates, 1):
            print(f"    [{j}/{len(candidates)}] candidate_id={c['candidate_id']}")
            record = parse_candidate(c["candidate_id"])
            record["constituency_name"] = con["constituency_name"]
            records.append(record)
            time.sleep(DELAY_SEC)

    return records



# ──────────────────────────────────────────────
#  CLI
# ──────────────────────────────────────────────
def main():
    global DELAY_SEC
    parser = argparse.ArgumentParser(
        description="Scrape MyNeta Lok Sabha 2024 – criminal records, immovable assets, liabilities"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--candidate_id",    type=int, help="Single candidate ID  (e.g. 6163)")
    group.add_argument("--constituency_id", type=int, help="All candidates in one constituency (e.g. 22 for KADAPA)")
    group.add_argument("--state_id",        type=int, help="All candidates in one state  (e.g. 2 for Andhra Pradesh)")
    group.add_argument("--all",             action="store_true", help="All candidates across entire Lok Sabha 2024")

    parser.add_argument("--output_csv",  default=OUTPUT_CSV,  help="Output CSV filename")
    parser.add_argument("--output_json", default=OUTPUT_JSON, help="Output JSON filename")
    parser.add_argument("--delay",       type=float, default=DELAY_SEC, help="Delay between requests in seconds")

    args = parser.parse_args()

    
    DELAY_SEC = args.delay

    # ── Run ─────────────────────────────────────
    if args.candidate_id:
        records = scrape_single(args.candidate_id)

    elif args.constituency_id:
        records = scrape_constituency(args.constituency_id)

    elif args.state_id:
        records = scrape_state(args.state_id)

    else:   # --all
        records = scrape_all()

    save_results(records, args.output_csv, args.output_json)


if __name__ == "__main__":
    main()