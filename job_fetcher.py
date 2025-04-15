# Import necessary libraries
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
CSV_FILE = 'job_details.csv'
BACKUP_FOLDER = 'job_details_backups'

# Mapping from API response fields to CSV fields
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
    'refnr': 'referenznummer',
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

# Output fields for the CSV (based on the mapping)
ALL_FIELDS = list(FIELD_MAPPING.values())

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
            'size': '50',
            'sort': 'veroeffdatum',
            'veroeffentlichtseit': '1'
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
        time.sleep(0.2)

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

def backup_csv():
    if not os.path.exists(BACKUP_FOLDER):
        os.makedirs(BACKUP_FOLDER)

    if os.path.exists(CSV_FILE):
        current_date = datetime.now().strftime('%m-%d-%y')
        backup_file = os.path.join(BACKUP_FOLDER, f"job_details_{current_date}.csv")
        shutil.copy(CSV_FILE, backup_file)
        print(f"[✓] Backup saved as {backup_file}")

def append_to_csv(new_jobs):
    if not new_jobs:
        print("No new jobs found.")
        return

    with open(CSV_FILE, mode='a', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=ALL_FIELDS)
        if f.tell() == 0:
            writer.writeheader()
        for job in new_jobs:
            # Filter out any keys that are not in ALL_FIELDS
            filtered_job = {k: v for k, v in job.items() if k in ALL_FIELDS}
            
            # Add the 'scraping_date' to the filtered job
            filtered_job['scraping_date'] = datetime.utcnow().isoformat()
            
            # Write the filtered job to the CSV
            writer.writerow(filtered_job)

    print(f"[✓] Added {len(new_jobs)} new jobs to {CSV_FILE}")


def main():
    existing_refnrs = load_existing_refnrs()
    all_refnrs = fetch_job_ids()
    new_jobs = []

    for i, refnr in enumerate(all_refnrs):
        if refnr in existing_refnrs:
            print(f"[i] Found existing job {refnr}. Stopping further fetches.")
            break
        print(f"Fetching job {i+1}: {refnr}")
        job = fetch_job_details(refnr)

        if job:
            job['scraping_date'] = datetime.utcnow().isoformat()
            for field in ALL_FIELDS:
                job.setdefault(field, '')
            new_jobs.append(job)
        time.sleep(0.2)

    if new_jobs:
        print(f"Total new jobs found: {len(new_jobs)}")
        backup_csv()
        append_to_csv(new_jobs)

if __name__ == '__main__':
    main()
