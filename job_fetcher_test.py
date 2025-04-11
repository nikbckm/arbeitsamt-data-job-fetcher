import requests, base64, csv
from datetime import datetime

API_KEY = 'jobboerse-jobsuche'
BASE_URL = 'https://rest.arbeitsagentur.de/jobboerse/jobsuche-service/pc/v4'
HEADERS = {'X-API-Key': API_KEY}
CSV_FILE = 'test_job_details.csv'

def encode_refnr(refnr):
    return base64.b64encode(refnr.encode('utf-8')).decode('utf-8')

def fetch_one_job_id():
    resp = requests.get(f"{BASE_URL}/jobs", headers=HEADERS, params={'was': 'data', 'size': 1})
    jobs = resp.json().get('stellenangebote', [])
    return jobs[0]['refnr'] if jobs else None

def fetch_job_details(refnr):
    encoded = encode_refnr(refnr)
    resp = requests.get(f"{BASE_URL}/jobdetails/{encoded}", headers=HEADERS)
    return resp.json() if resp.status_code == 200 else None

def write_to_csv(job):
    job['scraping_date'] = datetime.utcnow().isoformat()
    with open(CSV_FILE, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=list(job.keys()))
        writer.writeheader()
        writer.writerow(job)

def main():
    refnr = fetch_one_job_id()
    if refnr:
        job = fetch_job_details(refnr)
        if job:
            write_to_csv(job)
            print(f"✅ Saved one job to {CSV_FILE}")
        else:
            print("❌ Failed to fetch job details.")
    else:
        print("❌ No job ID found.")

if __name__ == '__main__':
    main()
