import csv
import requests
import time
import random
import os
from math import ceil
import sys

# --- KONFIGURACJA ---
INPUT_CSV = "imionameskie.csv"      # plik w tym samym katalogu co scraper.py
OUTPUT_DIR = "wyniki"               # katalog tymczasowy dla częściowych wyników
FINAL_OUTPUT = "wyniki_final.csv"   # ostateczny plik CSV
TOTAL_JOBS = 20                     # liczba równoległych jobów
USER_AGENT = "Mozilla/5.0"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- Wczytanie imion ---
imiona = []
with open(INPUT_CSV, newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        imiona.append(row['IMIĘ_PIERWSZE'].strip())

# --- podział na joby ---
chunk_size = ceil(len(imiona) / TOTAL_JOBS)
chunks = [imiona[i:i + chunk_size] for i in range(0, len(imiona), chunk_size)]

# --- funkcja do pobrania liczby użytkowników ---
def get_user_count(name):
    url = f"https://jbzd.com.pl/search/users?page=1&per_page=200000&phrase={name}"
    headers = {"User-Agent": USER_AGENT}
    try:
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        data = r.json()
        count = len(data.get("values", []))
        return count
    except Exception as e:
        print(f"Błąd przy imieniu {name}: {e}")
        return 0

# --- przetwarzanie jednej części (job) ---
def process_chunk(chunk_index):
    chunk = chunks[chunk_index]
    output_file = os.path.join(OUTPUT_DIR, f"wyniki_part_{chunk_index + 1}.csv")
    with open(output_file, "w", newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["IMIĘ", "LICZBA_UZYTKOWNIKOW"])
        for name in chunk:
            count = get_user_count(name)
            print(f"{name} {count}")
            writer.writerow([name, count])
            time.sleep(random.uniform(1, 2))  # losowa przerwa 1-2 sekundy
    return output_file

# --- scalanie wszystkich części w jeden plik ---
def merge_results():
    final_path = FINAL_OUTPUT
    with open(final_path, "w", newline='', encoding='utf-8') as final_csv:
        writer = csv.writer(final_csv)
        writer.writerow(["IMIĘ", "LICZBA_UZYTKOWNIKOW"])
        for part_file in sorted(os.listdir(OUTPUT_DIR)):
            if part_file.endswith(".csv"):
                with open(os.path.join(OUTPUT_DIR, part_file), newline='', encoding='utf-8') as f:
                    reader = csv.reader(f)
                    next(reader)  # pomijamy nagłówek
                    for row in reader:
                        writer.writerow(row)
    print(f"Wszystkie dane zapisane w {FINAL_OUTPUT}")

# --- główne uruchomienie ---
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Użycie: python scraper.py <numer_jobu_od_0_do_19> [--merge]")
        sys.exit(1)

    arg = sys.argv[1]

    if arg == "--merge":
        merge_results()
    else:
        job_index = int(arg)
        process_chunk(job_index)
