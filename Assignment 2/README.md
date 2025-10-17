# SSL Labs ETL Connector (Assignment 2 - Extended to 3 endpoints)

Done by Abhishek 3122225001002 CSE A

## Overview
This connector extracts data from Qualys SSL Labs API endpoints (`/info`, `/analyze`, `/getEndpointData`) and loads raw JSON responses into MongoDB for auditing and processing.

## Files
- `etl_connector.py` — main ETL script
- `.env` — local config (not committed)
- `requirements.txt`

## Setup
1. Clone the central repo and create your branch (see Git section).
2. Create and activate a virtualenv:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt


Outputs:
![alt text](<Screenshot 2025-10-17 at 1.54.07 PM.png>)


In mongo db shell:
ssllabs_info_raw:
![alt text](<Screenshot 2025-10-17 at 1.55.36 PM.png>)
ssllabs_analyze_raw:
![alt text](<Screenshot 2025-10-17 at 1.55.52 PM.png>)
ssllabs_endpoint_raw:
![alt text](<Screenshot 2025-10-17 at 1.56.13 PM.png>)
