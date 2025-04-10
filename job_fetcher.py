import requests
import json
import base64
import csv
import time
from datetime import datetime
import os

API_KEY = 'jobboerse-jobsuche'
BASE_URL = 'https://rest.arbeitsagentur.de/jobboerse/jobsuche-service/pc/v4'
HEADERS = {'X-API-Key': API_KEY}
CSV_FILE = 'job_details.csv'

ALL_FIELDS = [
    'aktuelleVeroeffentlichungsdatum', 'angebotsart', 'arbeitgeber', 'branchengruppe', 'branche', 'arbeitgeberHashId',
    'arbeitsorte', 'arbeitszeitmodelle', 'befristung', 'uebernahme', 'betriebsgroesse', 'eintrittsdatum', 
    'ersteVeroeffentlichungsdatum', 'allianzpartner', 'allianzpartnerUrl', 'titel', 'hashId', 'beruf', 
    'modifikationsTimestamp', 'stellenbeschreibung', 'refnr', 'fuerFluechtlingeGeeignet', 'nurFuerSchwerbehinderte', 
    'anzahlOffeneStellen', 'arbeitgeberAdresse', 'fertigkeiten', 'mobilitaet', 'fuehrungskompetenzen', 'verguetung', 
    'arbeitgeberdarstellungUrl', 'arbeitgeberdarstellung', 'hauptDkz', 'istBetreut', 'istGoogleJobsRelevant', 
    'anzeigeAnonym', 'scraping_date'
]

def encode_refnr(refnr):
    return base64.b64encode(refnr.encode('utf-8')).decode('utf-8')

def fetch_job_ids():
    page = 1
    job_ids = []
    total_jobs = 0

    while True:
        params = {
            'was': 'data',
            'angebotsart': '1',
            'page': page,
            'size': '50'
        }
        resp = requests.get(f"{BASE_URL}/jobs", headers=HEADERS, params=params)
        if resp.status_code != 200:
            print(f"Error page {page}: {resp.status_code}")
            break

        jobs = resp.json().get('stellenangebote', [])
        if not jobs:
            break

        job_ids.extend([j['refnr'] for j in jobs if 'refnr' in j])
        total_jobs += len(jobs)

        if total_jobs >= int(resp.json().get('maxErgebnisse', 0)):
            break

        page += 1
        time.sleep(1)

    return job_ids

def fetch_job_details(refnr):
    encoded_refnr = encode_refnr(refnr)
    url = f"{BASE_URL}/jobdetails/{encoded_refnr}"
    resp = requests.get(url, headers=HEADERS)
    if resp.status_code == 200:
        return resp.json()
    return None

def load_existing_refnrs():
    if not os.path.exists(CSV_FILE):
        return set()
    with open(CSV_FILE, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        return set(row['refnr'] for row in reader)

def append_to_csv(new_jobs):
    if not new_jobs:
        print("No new jobs found.")
        return

    with open(CSV_FILE, mode='a', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=ALL_FIELDS)
        if f.tell() == 0:
            writer.writeheader()
        for job in new_jobs:
            writer.writerow(job)
    print(f"[✓] Added {len(new_jobs)} new jobs to {CSV_FILE}")

def main():
    existing_refnrs = load_existing_refnrs()
    all_refnrs = fetch_job_ids()
    new_jobs = []

    for i, refnr in enumerate(all_refnrs):
        if refnr in existing_refnrs:
            continue
        print(f"Fetching job {i+1}: {refnr}")
        job = fetch_job_details(refnr)
        if job:
            job['scraping_date'] = datetime.utcnow().isoformat()
            for field in ALL_FIELDS:
                job.setdefault(field, '')
            new_jobs.append(job)
        time.sleep(1)

    append_to_csv(new_jobs)

if __name__ == '__main__':
    main()
