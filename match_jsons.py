import os
import json
import pdfplumber
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from rapidfuzz import fuzz


PDF_DIR = "pdfs_all"       # folder with PDFs
JSON_DIR = "jsons_all"     # folder with JSONs


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


# replace inside match_json_to_pdf()
def match_json_to_pdf(json_file, pdf_texts):
    json_texts = load_json_texts(json_file)
    scores = defaultdict(int)
    for pdf_name, text in pdf_texts.items():
        for orig in json_texts:
            if not orig:
                continue
            # fuzzy match instead of exact substring
            if fuzz.partial_ratio(orig, text) > 80:   # threshold (tune 70–90)
                scores[pdf_name] += 1
    if not scores:
        return json_file, None, 0
    best_pdf = max(scores, key=scores.get)
    return json_file, best_pdf, scores[best_pdf]


def process_json(json_file, pdf_texts):
    """Wrapper for multiprocessing."""
    return match_json_to_pdf(json_file, pdf_texts)


def main():
    # Step 1: Load all PDFs once in parallel
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

    # Step 2: Match JSONs to PDFs in parallel
    json_files = [os.path.join(JSON_DIR, f) for f in os.listdir(JSON_DIR) if f.lower().endswith(".json")]
    print(f"\nMatching {len(json_files)} JSONs ...")

    results = []
    with ProcessPoolExecutor() as executor:
        futures = {executor.submit(process_json, j, pdf_texts): j for j in json_files}
        for future in as_completed(futures):
            try:
                json_file, best_pdf, matches = future.result()
                results.append((json_file, best_pdf, matches))
                if best_pdf:
                    print(f"✅ {os.path.basename(json_file)} → {best_pdf} (matches: {matches})")
                else:
                    print(f"⚠️ {os.path.basename(json_file)} → No good match found")
            except Exception as e:
                print(f"⚠️ Error processing {futures[future]}: {e}")

    # (Optional) write results to a CSV
    # import csv
    # with open("matches.csv", "w", newline="", encoding="utf-8") as f:
    #     writer = csv.writer(f)
    #     writer.writerow(["json_file", "matched_pdf", "match_count"])
    #     writer.writerows(results)


if __name__ == "__main__":
    main()
