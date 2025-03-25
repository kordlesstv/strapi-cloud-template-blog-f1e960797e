import requests
import json
import re
import psycopg2
from psycopg2.extras import Json  
import time 

# Database connection details
DB_PARAMS = {
    "dbname": "###########",
    "user": "##########",
    "password": "##############",
    "host": "##############",
    "port": "################",
}


def fetch_company_urls():
    """Fetch company URLs dynamically from the database."""
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()

        
        cursor.execute("SELECT company_url, indeed_url FROM job_scraped_data;")
        rows = cursor.fetchall()  

        # Process and filter the URLs
        url_list = []
        for company_url, indeed_url in rows:
            if company_url and company_url not in ["N/A", "null", None]:
                url_list.append(company_url)
            elif indeed_url and indeed_url not in ["N/A", "null", None]:
                url_list.append(indeed_url)

        return url_list

    except Exception as e:
        print(f"❌ Error fetching URLs: {e}")
        return []

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

 

print("🔹 Dynamically fetched URLs......................")
 



def create_table_if_not_exists():
    """Ensures the job_postings table exists in PostgreSQL."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS job_postings (
        id SERIAL PRIMARY KEY,
        company_url TEXT NOT NULL,
        data JSONB NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    try:
        with psycopg2.connect(**DB_PARAMS) as conn:
            with conn.cursor() as cur:
                cur.execute(create_table_query)
                conn.commit()
    except Exception as e:
        print("❌ Error creating table:", e)

# Insert data into PostgreSQL
def insert_into_db(company_url, extracted_data):
    """Inserts extracted job data into PostgreSQL."""
    insert_query = """
    INSERT INTO job_postings (company_url, data) 
    VALUES (%s, %s);
    """
    try:
        with psycopg2.connect(**DB_PARAMS) as conn:
            with conn.cursor() as cur:
                cur.execute(insert_query, (company_url, Json(extracted_data)))
                conn.commit()
    except Exception as e:
        print(f"❌ Error inserting data for {company_url}: {e}")

# Ensure table exists
create_table_if_not_exists()

PERPLEXITY_API_KEY = "###########################"
api_endpoint = "https://api.perplexity.ai/chat/completions"

headers = {
    'accept': 'application/json',
    'content-type': 'application/json',
    'Authorization': f'Bearer {PERPLEXITY_API_KEY}'
}

company_urls = fetch_company_urls()
    
for company_url in company_urls:

    prompt = {
        "model": "sonar",
        "stream": False,
        "max_tokens": 1024,
        "frequency_penalty": 1,
        "temperature": 0.0,
        "messages": [
            {"role": "system", "content": "Be precise and concise in your responses. Do not assume or infer missing details."},
            {
                "role": "user",
                "content": f"""Extract **only marketing-related job postings** from U.S.-based companies listed on {company_url}.  

                **Important Rules:**  
                - ✅ **Only include marketing-related job postings.**  
                - ❌ **Do NOT include jobs unrelated to marketing** (e.g., engineering, finance, HR, sales).  
                - ✅ The job title **must** contain keywords like **Marketing, Digital Marketing, SEO, Social Media, Content, PPC, Branding, Advertising, Growth Marketing, Marketing Analyst, Marketing Coordinator.**  
                - ❌ **Do NOT generate or assume job postings** if none exist.  
                - ✅ If no valid marketing jobs exist, return:  
                ```
                "No marketing job postings available for this company."
                ```

                **Ensure salary extraction is consistent:**  
                - ✅ If salary is available, provide it as an **object** with clearly defined zones (e.g., `"Zone A": "$85,100 - $113,400"`).  
                - ✅ If only one salary range is given, wrap it in an object under `"General"` (e.g., `"General": "$70,600 - $113,400"`).    
                - ✅ **Check for salary under "Salary", "Salary Description", "Compensation", "Base Pay Range" or similar labels.**
                + ✅ If multiple salary fields exist, choose the most specific **base salary range**.
                - ✅ If salary is missing, return `N/A`.  
                

                **Extract ratings and review counts in the following order of priority:**  
                1️⃣ **Job Listing (`job_url` - BuiltInSF):**  
                    - If available, extract **job-specific rating and review count** from BuiltInSF's job posting page.  
                    - Look for `"Job rating"` or `"Company rating"` inside the job listing page.  
                    - If `rating` or `review_count` is found, use it.  

                

                3️⃣ **If no rating or review count is found anywhere, return `N/A`.**  

               **Determine job expiration details:**  
               - ✅ Extract the **posting date** (e.g., `"Posted 5 days ago"`, `"Published: March 1, 2024"`).  
               - ✅ Look for **job expiration date** (e.g., `"Closes on March 20, 2024"`).  
               - ✅ If no explicit expiration date is available, estimate the expiry using a **default duration of 10 days from the posting date**.  
               - ✅ If the job is expired, set `"expired": true`, otherwise `"expired": false`. 

                **Provide the extracted jobs in JSON array format only**, with these exact fields:  
                - `company`  
                - `position`  
                - `salary` (should always be an **object** or `N/A`)  
                - `job_type`  
                - `location`  
                - `rating` (✅ Extract from `job_url` first, then Indeed)  
                - `review_count` (✅ Extract from `job_url` first, then Indeed)  
                - `job_url`  
                - `scraped_at`  
                - `posting_date`  
                - `expiry_date` (✅ Extract if available, otherwise estimate as `posting_date + 10 days`)  
                - `active_days_remaining` (✅ Calculate days left before expiry)
                - `description`  
                - `search_input`  
                - `search_location`  
                - `country` (✅ Must be **USA** only)  
                - `indeed_url` (✅ Provide Indeed's profile link if available)  
                - `company_url`  
                - `expired` (✅ `true` if the job is no longer available, otherwise `false`)  
                """
            }
        ]
    }






    try:
        response = requests.post(api_endpoint, headers=headers, json=prompt)
        response.raise_for_status()  
        response_json = response.json()

        
        choices = response_json.get("choices", [])
        if not choices:
            print(f"❌ No valid job data extracted for {company_url}.")
            continue

        raw_text = choices[0]["message"].get("content", "").strip()

        if not raw_text or "No marketing job postings available" in raw_text:
            print(f"🚫 No job postings found for {company_url}.")
            no_jobs_message = {"message": "No job postings found for this company."}
            insert_into_db(company_url, no_jobs_message)
            continue

        # Try extracting JSON from ```json ... ``` format

        json_match = re.search(r"```json\n(.*?)\n```", raw_text, re.DOTALL)
        clean_text = json_match.group(1).strip() if json_match else raw_text.strip()

        
        print("==== Extracted JSON (Before Parsing) ====")
        # print(clean_text)
        print("=========================================")

        # If clean_text is not properly formatted JSON (e.g., missing brackets, incorrect syntax), it raises a json.JSONDecodeError.
        try:
            extracted_data = json.loads(clean_text)
            # print("extract_data", extracted_data)
        except json.JSONDecodeError as e:
            print("❌ JSON Parsing Error for {company_url}:", e)
            # print("Raw Response:", raw_text)
            continue

        # Ensure extracted data is always a list
        if isinstance(extracted_data, dict):  
            extracted_data = [extracted_data]  
        elif not isinstance(extracted_data, list):  
            print("❌ Unexpected JSON structure for {company_url}. Expected a list of job postings.")  
            continue

    
        
        #  rename keys 
        for job in extracted_data:
            if "position_name" in job:
                job['position'] = job.pop("position_name")
            

         # Save to PostgreSQL
        insert_into_db(company_url, extracted_data)
         
        file_name = company_url.replace("http://", "").replace("https://", "").replace("/", "_") + ".json"
       

        print(f"✅ Data successfully saved for {company_url} in {file_name}!")

    except requests.exceptions.RequestException as err:
        print(f"❌ API Request failed for {company_url}: {err}")


 # Add a 5-second delay before the next iteration
    print("⏳ Waiting for 5 seconds before the next request...")
    time.sleep(5)

 
 
