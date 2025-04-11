# Import necessary libraries
import requests  # For making HTTP requests to the API
import json      # For parsing JSON responses
import base64    # For encoding job reference numbers for API requests
import csv       # For reading and writing CSV files
import time      # For adding delay between requests
from datetime import datetime  # For handling timestamps
import os        # For file and directory operations
import shutil    # For backing up existing files

# API configuration
API_KEY = 'jobboerse-jobsuche'
BASE_URL = 'https://rest.arbeitsagentur.de/jobboerse/jobsuche-service/pc/v4'
HEADERS = {'X-API-Key': API_KEY}

# Output CSV and backup directory
CSV_FILE = 'job_details.csv'
BACKUP_FOLDER = 'job_details_backups'

# Define all job fields to extract and write into the CSV
ALL_FIELDS = [
    'aktuelleVeroeffentlichungsdatum', 'angebotsart', 'arbeitgeber', 'branchengruppe', 'branche', 'arbeitgeberHashId',
    'arbeitsorte', 'arbeitszeitmodelle', 'befristung', 'uebernahme', 'betriebsgroesse', 'eintrittsdatum', 
    'ersteVeroeffentlichungsdatum', 'allianzpartner', 'allianzpartnerUrl', 'titel', 'hashId', 'beruf', 
    'modifikationsTimestamp', 'stellenbeschreibung', 'refnr', 'fuerFluechtlingeGeeignet', 'nurFuerSchwerbehinderte', 
    'anzahlOffeneStellen', 'arbeitgeberAdresse', 'fertigkeiten', 'mobilitaet', 'fuehrungskompetenzen', 'verguetung', 
    'arbeitgeberdarstellungUrl', 'arbeitgeberdarstellung', 'hauptDkz', 'istBetreut', 'istGoogleJobsRelevant', 
    'anzeigeAnonym', 'scraping_date'
]

# Encode the job reference number for use in the job details endpoint
def encode_refnr(refnr):
    return base64.b64encode(refnr.encode('utf-8')).decode('utf-8')

# Fetch all job reference numbers via paginated API requests
def fetch_job_ids():
    page = 1
    job_ids = []
    total_jobs = 0

    while True:
        params = {
            'was': 'data',           # Job search keyword
            'angebotsart': '1',      # Job offer type
            'page': page,
            'size': '50',             # Jobs per page
            'sort': 'veroeffdatum'
        }
        resp = requests.get(f"{BASE_URL}/jobs", headers=HEADERS, params=params)
        if resp.status_code != 200:
            print(f"Error page {page}: {resp.status_code}")
            break

        jobs = resp.json().get('stellenangebote', [])
        if not jobs:
            break

        # Collect job reference numbers
        job_ids.extend([j['refnr'] for j in jobs if 'refnr' in j])
        total_jobs += len(jobs)

        # Stop if we've reached the max result count from the API
        if total_jobs >= int(resp.json().get('maxErgebnisse', 0)):
            break

        page += 1
        time.sleep(1)  # Be polite to the API

    return job_ids

# Fetch detailed job info using encoded reference number
def fetch_job_details(refnr):
    encoded_refnr = encode_refnr(refnr)
    url = f"{BASE_URL}/jobdetails/{encoded_refnr}"
    resp = requests.get(url, headers=HEADERS)
    if resp.status_code == 200:
        return resp.json()
    return None

# Load job reference numbers already present in the existing CSV
def load_existing_refnrs():
    if not os.path.exists(CSV_FILE):
        return set()
    with open(CSV_FILE, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        return set(row['refnr'] for row in reader)

# Create a timestamped backup of the existing CSV
def backup_csv():
    if not os.path.exists(BACKUP_FOLDER):
        os.makedirs(BACKUP_FOLDER)

    if os.path.exists(CSV_FILE):
        current_date = datetime.now().strftime('%m-%d-%y')
        backup_file = os.path.join(BACKUP_FOLDER, f"job_details_{current_date}.csv")
        shutil.copy(CSV_FILE, backup_file)
        print(f"[✓] Backup saved as {backup_file}")

# Append newly fetched job details to the CSV
def append_to_csv(new_jobs):
    if not new_jobs:
        print("No new jobs found.")
        return

    with open(CSV_FILE, mode='a', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=ALL_FIELDS)
        if f.tell() == 0:  # File is empty, write headers
            writer.writeheader()
        for job in new_jobs:
            filtered_job = {k: job.get(k, '') for k in ALL_FIELDS}
            writer.writerow(filtered_job)
    print(f"[✓] Added {len(new_jobs)} new jobs to {CSV_FILE}")

# Main execution flow
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
            job['scraping_date'] = datetime.utcnow().isoformat()  # Add timestamp
            for field in ALL_FIELDS:
                job.setdefault(field, '')  # Fill missing fields with empty strings
            new_jobs.append(job)
        time.sleep(0.3) # be nice to the API :)

    if new_jobs:
        backup_csv()
        append_to_csv(new_jobs)

# Entry point
if __name__ == '__main__':
    main()
