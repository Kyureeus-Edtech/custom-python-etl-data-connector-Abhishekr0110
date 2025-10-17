import os
from datetime import datetime, timezone
import requests
from dotenv import load_dotenv
from pymongo import MongoClient, errors as mongo_errors


load_dotenv()

SPAMHAUS_URL = "https://www.spamhaus.org/drop/drop.txt"

load_dotenv()

MONGO_URI       = os.getenv("MONGO_URI")
MONGO_DB_NAME   = os.getenv("MONGO_DB")
MONGO_COLL_NAME = os.getenv("MONGO_COLLECTION")


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def extract_drop_list(url: str):
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.text


def transform_drop_list(raw_text: str):
    records = []
    for line in raw_text.splitlines():
        line = line.strip()

        if not line or line.startswith(";") or line.startswith("#"):
            continue
        if ";" in line:
            cidr, desc = line.split(";", 1)
            cidr = cidr.strip()
            desc = desc.strip()
        else:
            cidr, desc = line, ""
        records.append({
            "_id": cidr,
            "description": desc,
            "_ingested_at": utc_now_iso()
        })
    return records


def load_into_mongo(records):
    client = MongoClient(MONGO_URI)
    coll = client[MONGO_DB_NAME][MONGO_COLL_NAME]
    inserted = 0
    skipped = 0
    for rec in records:
        try:
            coll.insert_one(rec)
            inserted += 1
        except mongo_errors.DuplicateKeyError:
            skipped += 1
    return inserted, skipped


def run():
    print(f"[INFO] Starting Spamhaus DROP ETL at {utc_now_iso()}")
    raw_text = extract_drop_list(SPAMHAUS_URL)
    records = transform_drop_list(raw_text)
    inserted, skipped = load_into_mongo(records)
    print(f"[INFO] Done. Inserted={inserted}, Skipped={skipped}")

if __name__ == "__main__":
    run()
