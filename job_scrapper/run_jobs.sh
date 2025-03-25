#!/bin/bash
# Navigate to your FastAPI project directory
cd /home/ubuntu/jobscraper/web-crawler || exit

# Activate the virtual environment
source /home/ubuntu/jobscraper/web-crawler/env/bin/activate

# Run the Python scripts
python job_scraped_data.py
python job_postings.py

# Deactivate virtual environment
deactivate
