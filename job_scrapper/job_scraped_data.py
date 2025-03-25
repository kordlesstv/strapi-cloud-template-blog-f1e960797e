import os
import csv
import psycopg2
from apify_client import ApifyClient

client = ApifyClient("######################")

DB_CONFIG = {
    "dbname": "##########",
    "user": "##############",
    "password": "##############",
    "host": "##########",
    "port": "############",
}

CSV_FILE_PATH = "job_scraped_data.csv"
CSV_HEADERS = [
    "salary", "company", "position_name", "posted_at", "job_type", "location",
    "rating", "review_count", "job_url", "scraped_at", "posting_date",
    "description", "search_input", "search_location", "country",
    "indeed_url", "company_url", "company_logo"
]

if not os.path.exists(CSV_FILE_PATH):
    with open(CSV_FILE_PATH, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(CSV_HEADERS)

conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()

TABLE_NAME = "job_scraped_data"

create_table_query = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    id SERIAL PRIMARY KEY,
    salary TEXT,
    company TEXT,
    position_name TEXT,
    posted_at TEXT,
    job_type TEXT,
    location TEXT,
    rating TEXT,
    review_count INTEGER,
    job_url TEXT UNIQUE,  -- Ensuring job_url is unique
    scraped_at TEXT,
    posting_date TEXT,
    description TEXT,
    search_input TEXT,
    search_location TEXT,
    country TEXT,
    indeed_url TEXT,
    company_url TEXT,
    company_logo TEXT
);
"""
cursor.execute(create_table_query)
conn.commit()

run_input = {
    "position": "Marketing",
    "country": "US",
    "location": "",
    "maxItems": 1,
    "parseCompanyDetails": True,
    "saveOnlyUniqueItems": True,
    "followApplyRedirects": False,
}

run = client.actor("hMvNSpz3JnHgl5jkh").call(run_input=run_input)

skipped_count = 0
inserted_count = 0
with open(CSV_FILE_PATH, mode="a", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)

    for item in client.dataset(run["defaultDatasetId"]).iterate_items():
        
        job_url = item.get("url", "N/A")  

        # Check if the job already exists
        cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE job_url = %s;", (job_url,))
        if cursor.fetchone()[0] > 0:
            skipped_count += 1
             
            continue  

        job_type_list = item.get("jobType", [])
        job_type = job_type_list[0] if isinstance(job_type_list, list) and job_type_list else "Unknown"

        job_data = {
            "salary": item.get("salary") or "Not provided",
            "company": item.get("company") or "Unknown",
            "position_name": item.get("positionName") or "Unknown",
            "posted_at": item.get("postedAt") or "Unknown",
            "job_type": job_type,  
            "location": item.get("location") or "Unknown",
            "rating": item.get("rating") or "N/A",
            "review_count": item.get("reviewsCount") or 0,
            "job_url": job_url,
            "scraped_at": item.get("scrapedAt") or "N/A",
            "posting_date": item.get("postingDateParsed") or "N/A",
            "description": item.get("description") or "No description available",
            "search_input": item.get("searchInput", {}).get("position") or "Unknown",
            "search_location": item.get("searchInput", {}).get("location") or "Unknown",
            "country": item.get("searchInput", {}).get("country") or "Unknown",
            "indeed_url": item.get("companyInfo", {}).get("indeedUrl") or "N/A",
            "company_url": item.get("companyInfo", {}).get("url") or "N/A",
            "company_logo": item.get("companyInfo", {}).get("companyLogo") or "N/A",
        }

        insert_query = f"""
        INSERT INTO {TABLE_NAME} (
            salary, company, position_name, posted_at, job_type, location, rating, 
            review_count, job_url, scraped_at, posting_date, description, search_input, 
            search_location, country, indeed_url, company_url, company_logo
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        
        cursor.execute(insert_query, tuple(job_data.values()))
        conn.commit()

        writer.writerow(job_data.values())
        inserted_count += 1
        

cursor.close()
conn.close()

print(f"\n📊 Job Scraping Summary:")
print(f"✅ New jobs inserted: {inserted_count}")
print(f"⚠️ Jobs skipped (already in DB): {skipped_count}")
print(f"📁 Data saved to CSV: {CSV_FILE_PATH}")
