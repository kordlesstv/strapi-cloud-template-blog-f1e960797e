from fastapi import FastAPI, Query
import psycopg2
import json
from typing import List

app = FastAPI()

 
DB_CONFIG = {
    "dbname": "##############",
    "user": "###############",
    "password": "##############",
    "host": "################",
    "port": "#################",
}

 
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

@app.get("/get_job_data")
def get_jobs(page: int = Query(1, alias="page", ge=1), page_size: int = Query(10, alias="page_size", ge=1)):
    """Fetch paginated job data in JSON format."""
    offset = (page - 1) * page_size  # Calculate offset for pagination

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get total count of records
        cursor.execute("SELECT COUNT(*) FROM job_scraped_data")
        total_count = cursor.fetchone()[0]

        # Ensure we don't fetch more records than available
        if offset >= total_count:
            return {
                "page": page,
                "page_size": page_size,
                "total_records": total_count,
                "total_pages": (total_count // page_size) + (1 if total_count % page_size > 0 else 0),
                "data": [],
            }

        # Fetch data with pagination
        cursor.execute("SELECT * FROM job_scraped_data ORDER BY id ASC LIMIT %s OFFSET %s", (page_size, offset))
        rows = cursor.fetchall()

        # Fetch column names
        col_names = [desc[0] for desc in cursor.description]

        # Convert to JSON format
        jobs = [dict(zip(col_names, row)) for row in rows]

        conn.close()

        return {
            "page": page,
            "page_size": len(jobs),  # This ensures correct count is returned
            "total_records": total_count,
            "total_pages": (total_count // page_size) + (1 if total_count % page_size > 0 else 0),
            "data": jobs,
        }

    except Exception as e:
        return {"error": str(e)}

