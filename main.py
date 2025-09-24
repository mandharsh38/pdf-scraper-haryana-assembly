import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

BASE = "https://haryanaassembly.gov.in"
AJAX_URL = BASE + "/wp-content/themes/custome-theme/digitallib_ajax.php"

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0",
    "X-Requested-With": "XMLHttpRequest"
})

def fetch_house_data(house_no):
    """Fetch proceedings table for given house number."""
    params = {
        "house": house_no,
        "syear": "",
        "ssession": "",
        "subject": "",
        "sfrm": "hp",
        "act": "loaddata"
    }
    r = session.get(AJAX_URL, params=params)
    r.raise_for_status()
    return r.text

def parse_table(html):
    """Parse table rows into structured records with metadata + pdf URL."""
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    for tr in soup.select("table#searchResTable tbody tr"):
        cols = [td.get_text(strip=True) for td in tr.find_all("td")]
        if not cols or len(cols) < 7:
            continue
        # PDF link
        pdf_a = tr.find("a", href=True)
        pdf_url = urljoin(BASE, pdf_a["href"]) if pdf_a else None
        rows.append({
            "house": cols[0],
            "year": cols[1],
            "session": cols[2],
            "sitting_date": cols[3],
            "sitting_no": cols[4],
            "subject": cols[5],
            "pdf_url": pdf_url
        })
    return rows

def sanitize_filename(name):
    """Make safe filenames."""
    return "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in name)

def download_record(rec, out_dir):
    """Download one PDF using metadata-based filename."""
    os.makedirs(out_dir, exist_ok=True)
    fname = f"{rec['year']}_{sanitize_filename(rec['session'])}_{rec['sitting_date']}_{sanitize_filename(rec['subject'])}.pdf"
    out_path = os.path.join(out_dir, fname)
    if os.path.exists(out_path):
        print(f"Skipping {fname} (already exists)")
        return
    print(f"Downloading {rec['pdf_url']} -> {out_path}")
    r = session.get(rec['pdf_url'], stream=True)
    r.raise_for_status()
    with open(out_path, "wb") as f:
        for chunk in r.iter_content(8192):
            f.write(chunk)

def main():
    for house in range(1, 16):  # 1–15
        print(f"\n=== House {house} ===")
        html = fetch_house_data(house)
        records = parse_table(html)
        print(f"Found {len(records)} entries for House {house}")
        out_dir = f"pdfs/house_{house}"
        for rec in records:
            download_record(rec, out_dir)

if __name__ == "__main__":
    main()
