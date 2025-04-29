import requests
import json
import base64
import csv
import time
from datetime import datetime
import os
import shutil

# API configuration
API_KEY = 'jobboerse-jobsuche'
BASE_URL = 'https://rest.arbeitsagentur.de/jobboerse/jobsuche-service/pc/v4'
HEADERS = {'X-API-Key': API_KEY}

# Output CSV and backup directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_FILE = os.path.join(SCRIPT_DIR, 'job_details.csv')
BACKUP_FOLDER = os.path.join(SCRIPT_DIR, 'job_details_backups')

FIELD_MAPPING = {
    'aktuelleVeroeffentlichungsdatum': 'veroeffentlichungszeitraum',
    'angebotsart': 'stellenangebotsart',
    'arbeitgeber': 'firma',
    'branchengruppe': 'hauptberuf',
    'branche': 'branche',
    'arbeitgeberHashId': 'arbeitgeberHashId',
    'arbeitsorte': 'stellenlokationen',
    'arbeitszeitmodelle': 'arbeitszeitHeimarbeitTelearbeit',
    'befristung': 'vertragsdauer',
    'uebernahme': 'uebernahme',
    'betriebsgroesse': 'betriebsgroesse',
    'eintrittsdatum': 'eintrittszeitraum',
    'ersteVeroeffentlichungsdatum': 'ersteVeroeffentlichungsdatum',
    'allianzpartner': 'allianzpartnerName',
    'allianzpartnerUrl': 'allianzpartnerUrl',
    'titel': 'stellenangebotsTitel',
    'hashId': 'hashId',
    'beruf': 'beruf',
    'modifikationsTimestamp': 'aenderungsdatum',
    'stellenbeschreibung': 'stellenangebotsBeschreibung',
    'refnr': 'refnr',
    'fuerFluechtlingeGeeignet': 'istGeringfuegigeBeschaeftigung',
    'nurFuerSchwerbehinderte': 'istBehinderungGefordert',
    'anzahlOffeneStellen': 'anzahlOffeneStellen',
    'arbeitgeberAdresse': 'arbeitsorte',
    'fertigkeiten': 'fertigkeiten',
    'mobilitaet': 'mobilitaet',
    'fuehrungskompetenzen': 'fuehrungskompetenzen',
    'verguetung': 'verguetung',
    'arbeitgeberdarstellungUrl': 'arbeitgeberdarstellungUrl',
    'arbeitgeberdarstellung': 'arbeitgeberdarstellung',
    'hauptDkz': 'hauptDkz',
    'istBetreut': 'istBetreut',
    'istGoogleJobsRelevant': 'istGoogleJobsRelevant',
    'anzeigeAnonym': 'anzeigeAnonym',
    'scraping_date': 'scraping_date'
}

ALL_FIELDS = list(FIELD_MAPPING.values())

def encode_refnr(refnr):
    return base64.b64encode(refnr.encode('utf-8')).decode('utf-8')

def get_with_retries(url, params=None, headers=None, retries=3, backoff=5):
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=10)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            print(f"[!] Attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                sleep_time = backoff * (2 ** attempt)
                print(f"[i] Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
            else:
                print(f"[x] Max retries reached. Failed to fetch from {url}")
                return None

def fetch_job_ids():
    page = 1
    job_ids = []
    total_jobs = 0
    while True:
        params = {
            'was': 'data',
            'angebotsart': '1',
            'page': page,
            'size': '50',
            'sort': 'veroeffdatum',
            'veroeffentlichtseit': '7'
        }
        resp = get_with_retries(f"{BASE_URL}/jobs", params=params, headers=HEADERS)
        if not resp:
            break
        jobs = resp.json().get('stellenangebote', [])
        if not jobs:
            break
        job_ids.extend([j['refnr'] for j in jobs if 'refnr' in j])
        total_jobs += len(jobs)
        if total_jobs >= int(resp.json().get('maxErgebnisse', 0)):
            break
        page += 1
        time.sleep(0.2)
    return job_ids

def fetch_job_details(refnr):
    encoded_refnr = encode_refnr(refnr)
    url = f"{BASE_URL}/jobdetails/{encoded_refnr}"
    resp = get_with_retries(url, headers=HEADERS)
    if resp:
        job = resp.json()
        job['refnr'] = job.get('refnr') or job.get('referenznummer', '')
        return job
    return None

def load_existing_refnrs():
    if not os.path.exists(CSV_FILE):
        return set()
    with open(CSV_FILE, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        return set(row['refnr'].strip() for row in reader)

def backup_csv():
    if not os.path.exists(BACKUP_FOLDER):
        os.makedirs(BACKUP_FOLDER)
    if os.path.exists(CSV_FILE):
        current_date = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        backup_file = os.path.join(BACKUP_FOLDER, f"job_details_{current_date}.csv")
        shutil.copy(CSV_FILE, backup_file)
        print(f"[✓] Backup saved as {backup_file}")
        return backup_file
    return None

def extract_sort_key(job):
    period = job.get('veroeffentlichungszeitraum')
    if isinstance(period, dict):
        return period.get('von', '')
    return job.get('scraping_date', '')

def append_to_csv(new_jobs):
    if not new_jobs:
        print("No new jobs found.")
        return
    new_jobs.sort(key=extract_sort_key, reverse=True)
    with open(CSV_FILE, mode='a', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=ALL_FIELDS)
        if f.tell() == 0:
            writer.writeheader()
        for job in new_jobs:
            filtered_job = {k: v for k, v in job.items() if k in ALL_FIELDS}
            filtered_job['scraping_date'] = datetime.utcnow().isoformat()
            writer.writerow(filtered_job)
    print(f"[✓] Added {len(new_jobs)} new jobs to {CSV_FILE}")

def main():
    print("[→] Starting job scraping script...")
    print("[•] Loading existing refnrn from CSV...")
    existing_refnrs = load_existing_refnrs()
    print("[•] Fetching today's refnrn from API...")
    all_refnrs = fetch_job_ids()
    new_jobs = []

    for i, refnr in enumerate(all_refnrs or []):
        refnr = refnr.strip()
        if refnr in existing_refnrs:
            print(f"[i] Skipping existing job {refnr}")
            continue
        print(f"[→] Fetching job {i+1}: {refnr}")
        job = fetch_job_details(refnr)
        if job:
            job['scraping_date'] = datetime.utcnow().isoformat()
            for field in ALL_FIELDS:
                job.setdefault(field, '')
            new_jobs.append(job)
        time.sleep(0.2)

    print("[•] Creating CSV backup before writing new data...")
    backup_path = backup_csv()

    if new_jobs:
        print(f"[✓] Total new jobs found: {len(new_jobs)}")
        print("[•] Writing new jobs to CSV...")
        append_to_csv(new_jobs)
        print(f"DEBUG: {len(new_jobs)} jobs collected")
        if backup_path:
            print(f"::set-output name=backup_path::{backup_path}")
    else:
        print("[i] No new jobs were added today. CSV remains unchanged.")

    print("[✓] Job scraping run completed.")

if __name__ == '__main__':
    main()
