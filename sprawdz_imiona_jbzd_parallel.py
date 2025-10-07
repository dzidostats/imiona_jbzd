import csv
import requests
import time
import random
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

INPUT_FILE = "imionameskie.csv"
OUTPUT_FILE_TEMPLATE = "wyniki_part_{}.csv"
BASE_URL = "https://jbzd.com.pl/search/users?page=1&per_page=200000&phrase={}"

# ile równoległych zapytań na jednym jobie
THREADS_PER_JOB = 5

def sprawdz_imie(imie):
    """Sprawdza jedno imię przez API JBZD."""
    url = BASE_URL.format(imie)
    try:
        response = requests.get(url, timeout=10)
        data = response.json()
        liczba = len(data.get("values", []))
        print(f"{imie} {liczba}")
        time.sleep(random.uniform(1, 2))  # losowa pauza 1–2 sekundy
        return imie, liczba
    except Exception as e:
        print(f"{imie} - błąd ({e})")
        return imie, None

def main():
    # pobierz argument jobu (0–19)
    if len(sys.argv) < 2:
        print("Użycie: python sprawdz_imiona_jbzd_parallel.py <numer_jobu_0-19>")
        sys.exit(1)
    job_id = int(sys.argv[1])

    # wczytaj wszystkie imiona
    with open(INPUT_FILE, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        imiona = [row["IMIĘ_PIERWSZE"].strip() for row in reader]

    # podział na 20 części
    chunk_size = len(imiona) // 20
    start = job_id * chunk_size
    end = (job_id + 1) * chunk_size if job_id < 19 else len(imiona)
    imiona_chunk = imiona[start:end]

    print(f"Job {job_id}: przetwarzam {len(imiona_chunk)} imion od {start} do {end}")

    results = []

    with ThreadPoolExecutor(max_workers=THREADS_PER_JOB) as executor:
        futures = {executor.submit(sprawdz_imie, imie): imie for imie in imiona_chunk}
        for future in as_completed(futures):
            imie, liczba = future.result()
            results.append({"IMIĘ": imie, "LICZBA_WYSTĄPIEŃ_W_API": liczba})

    # zapis wyników tego joba
    output_file = OUTPUT_FILE_TEMPLATE.format(job_id)
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["IMIĘ", "LICZBA_WYSTĄPIEŃ_W_API"])
        writer.writeheader()
        writer.writerows(results)

    print(f"✅ Job {job_id} zakończony — zapisano {len(results)} rekordów do {output_file}")

if __name__ == "__main__":
    main()
