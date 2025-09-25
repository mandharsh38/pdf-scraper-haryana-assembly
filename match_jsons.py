import os
import json
import pdfplumber
import pickle
import csv
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from rapidfuzz import fuzz


PDF_DIR = "pdfs_all"       # folder with PDFs
JSON_DIR = "jsons_all"     # folder with JSONs
CACHE_FILE = "pdf_cache.pkl"
CSV_FILE = "matches.csv"


def extract_pdf_text(pdf_path):
    """Extract all text from a PDF using pdfplumber."""
    text = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text() or ""
            text.append(page_text)
    return "\n".join(text)


def load_json_texts(json_path):
    """Get all 'original_text' fields from a JSON file."""
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return [item.get("original_text", "") for item in data if "original_text" in item]


def match_json_to_pdf(json_file, pdf_texts):
    json_texts = load_json_texts(json_file)
    scores = defaultdict(int)
    best_matches = defaultdict(str)  # store matched PDF text snippet

    for pdf_name, text in pdf_texts.items():
        for orig in json_texts:
            if not orig:
                continue
            if fuzz.partial_ratio(orig, text) > 80:  # threshold
                scores[pdf_name] += 1
                # store first matching snippet (for CSV)
                if pdf_name not in best_matches:
                    # grab first 100 chars around match
                    idx = text.lower().find(orig.lower())
                    if idx != -1:
                        snippet = text[max(idx - 50, 0):idx + len(orig) + 50].replace("\n", " ")
                    else:
                        snippet = text[:100].replace("\n", " ")
                    best_matches[pdf_name] = snippet

    if not scores:
        return json_file, None, 0, []

    best_pdf = max(scores, key=scores.get)
    matched_pairs = [(orig, best_matches[best_pdf]) for orig in json_texts if fuzz.partial_ratio(orig, pdf_texts[best_pdf]) > 80]

    return json_file, best_pdf, scores[best_pdf], matched_pairs


def process_json(json_file, pdf_texts):
    """Wrapper for multiprocessing."""
    return match_json_to_pdf(json_file, pdf_texts)


def main():
    # Load cached PDF texts if available
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "rb") as f:
            pdf_texts = pickle.load(f)
        print(f"Loaded cached PDF texts from {CACHE_FILE}")
    else:
        pdf_texts = {}
        pdf_files = [f for f in os.listdir(PDF_DIR) if f.lower().endswith(".pdf")]

        print(f"Extracting text from {len(pdf_files)} PDFs ...")
        with ProcessPoolExecutor() as executor:
            futures = {executor.submit(extract_pdf_text, os.path.join(PDF_DIR, f)): f for f in pdf_files}
            for future in as_completed(futures):
                fname = futures[future]
                try:
                    pdf_texts[fname] = future.result()
                    print(f"✅ Extracted {fname}")
                except Exception as e:
                    print(f"⚠️ Failed to extract {fname}: {e}")

        # Save cache
        with open(CACHE_FILE, "wb") as f:
            pickle.dump(pdf_texts, f)
        print(f"Cached PDF texts to {CACHE_FILE}")

    # Match JSONs to PDFs
    json_files = [os.path.join(JSON_DIR, f) for f in os.listdir(JSON_DIR) if f.lower().endswith(".json")]
    print(f"\nMatching {len(json_files)} JSONs ...")

    results = []
    with ProcessPoolExecutor() as executor:
        futures = {executor.submit(process_json, j, pdf_texts): j for j in json_files}
        for future in as_completed(futures):
            try:
                json_file, best_pdf, matches, matched_pairs = future.result()
                results.append((json_file, best_pdf, matches, matched_pairs))
                if best_pdf:
                    print(f"✅ {os.path.basename(json_file)} → {best_pdf} (matches: {matches})")
                else:
                    print(f"⚠️ {os.path.basename(json_file)} → No good match found")
            except Exception as e:
                print(f"⚠️ Error processing {futures[future]}: {e}")

    # Write results to CSV
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["json_file", "matched_pdf", "match_count", "json_text", "pdf_snippet"])
        for json_file, best_pdf, count, matched_pairs in results:
            if matched_pairs:
                for json_text, pdf_snippet in matched_pairs:
                    writer.writerow([json_file, best_pdf, count, json_text, pdf_snippet])
            else:
                writer.writerow([json_file, best_pdf, count, "", ""])


if __name__ == "__main__":
    main()
