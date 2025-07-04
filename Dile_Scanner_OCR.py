import os
import re
import json
import fitz  # PyMuPDF
import pytesseract
from collections import defaultdict
from pdf2image import convert_from_path
from concurrent.futures import ProcessPoolExecutor, as_completed

CACHE_FILE = "scan_cache.json"
OUTPUT_JSON = "word_map.json"

# Optional: specify path to tesseract.exe if not in PATH
# pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"

def extract_tokens_from_pdf(pdf_path):
    token_map = defaultdict(set)
    filename = os.path.basename(pdf_path)

    try:
        doc = fitz.open(pdf_path)
        for page_number in range(len(doc)):
            page = doc.load_page(page_number)
            text = page.get_text()
            
            tokens = re.findall(r'\b\w+\b|[^\s\w]', text.lower())
            
            # If no tokens found, treat it as image-based and apply OCR
            if not tokens:
                print(f"OCR fallback: {filename}, page {page_number + 1}")
                images = convert_from_path(pdf_path, first_page=page_number + 1, last_page=page_number + 1)
                if images:
                    text = pytesseract.image_to_string(images[0])
                    tokens = re.findall(r'\b\w+\b|[^\s\w]', text.lower())

            for token in tokens:
                token_map[token].add((filename, page_number + 1))

    except Exception as e:
        print(f"Error processing {pdf_path}: {e}")

    return token_map

def merge_token_maps(all_maps):
    merged = defaultdict(set)
    for token_map in all_maps:
        for token, locations in token_map.items():
            merged[token].update(locations)
    return merged

def load_existing_word_map_json():
    word_map = defaultdict(set)
    if not os.path.exists(OUTPUT_JSON):
        return word_map

    with open(OUTPUT_JSON, 'r', encoding='utf-8') as f:
        data = json.load(f)
        for token, file_pages in data.items():
            for file, pages in file_pages.items():
                for page in pages:
                    word_map[token].add((file, page))
    return word_map

def save_word_map_json(word_map):
    structured_map = defaultdict(lambda: defaultdict(set))
    for token, locations in word_map.items():
        for file, page in locations:
            structured_map[token][file].add(page)

    output = {
        token: {
            pdf: sorted(pages)
            for pdf, pages in pdf_map.items()
        }
        for token, pdf_map in structured_map.items()
    }

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)

def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r") as f:
            return json.load(f)
    return {}

def save_cache(cache):
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f)

def pdf_check_add(new_pdfs, word_map, cache, folder):
    if new_pdfs:
        print("New PDFs found:", new_pdfs)
        token_maps = []
        with ProcessPoolExecutor() as executor:
            futures = [executor.submit(extract_tokens_from_pdf, os.path.join(folder, pdf)) for pdf in new_pdfs]
            for future in as_completed(futures):
                token_maps.append(future.result())
        new_data = merge_token_maps(token_maps)
        for token, locs in new_data.items():
            word_map[token].update(locs)
        for pdf in new_pdfs:
            path = os.path.join(folder, pdf)
            cache[pdf] = {"modified": os.path.getmtime(path)}
    return word_map

def pdf_check_deleted(deleted_pdfs, word_map, cache):
    if deleted_pdfs:
        print("Deleted PDFs found:", deleted_pdfs)
        for token in list(word_map.keys()):
            updated_locations = {(file, page) for (file, page) in word_map[token] if file not in deleted_pdfs}
            if updated_locations:
                word_map[token] = updated_locations
            else:
                del word_map[token]
        for pdf in deleted_pdfs:
            del cache[pdf]
    return word_map

def build_word_map_with_cache(folder):
    all_pdfs = [f for f in os.listdir(folder) if f.lower().endswith(".pdf")]
    all_pdfs_set = set(all_pdfs)
    cache = load_cache()
    cached_pdfs_set = set(cache.keys())

    word_map = defaultdict(set)

    if not os.path.exists(OUTPUT_JSON):
        print("word_map.json not found. Building full index...")
        token_maps = []
        with ProcessPoolExecutor() as executor:
            futures = [executor.submit(extract_tokens_from_pdf, os.path.join(folder, pdf)) for pdf in all_pdfs]
            for future in as_completed(futures):
                token_maps.append(future.result())
        word_map = merge_token_maps(token_maps)
        for pdf in all_pdfs:
            path = os.path.join(folder, pdf)
            cache[pdf] = {"modified": os.path.getmtime(path)}
    else:
        word_map = load_existing_word_map_json()
        m = len(all_pdfs_set) - len(cached_pdfs_set)

        if m > 0:
            word_map = pdf_check_add(all_pdfs_set - cached_pdfs_set, word_map, cache, folder)
        elif m < 0:
            word_map = pdf_check_deleted(cached_pdfs_set - all_pdfs_set, word_map, cache)

        modified_pdfs = set()
        for pdf in all_pdfs:
            path = os.path.join(folder, pdf)
            mod_time = os.path.getmtime(path)
            if cache.get(pdf, {}).get("modified") != mod_time:
                modified_pdfs.add(pdf)

        if modified_pdfs:
            word_map = pdf_check_deleted(modified_pdfs, word_map, cache)
            word_map = pdf_check_add(modified_pdfs, word_map, cache, folder)
        else:
            print("No PDF files found modified.")

    save_word_map_json(word_map)
    save_cache(cache)
    return True


build_word_map_with_cache("/content/drive/MyDrive/Sample")