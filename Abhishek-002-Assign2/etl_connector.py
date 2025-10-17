#!/usr/bin/env python3


import os
import sys
import argparse
import time
import json
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from pymongo import MongoClient, errors
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# load .env
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "etl_db")
BASE_URL = os.getenv("SSL_LABS_BASE", "https://api.ssllabs.com/api/v3")
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))

# Mongo collections
COL_INFO = "ssllabs_info_raw"
COL_ANALYZE = "ssllabs_analyze_raw"
COL_ENDPOINT = "ssllabs_endpoint_raw"

client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
db = client[MONGO_DB]

# Helper: insertion with timestamp
def insert_raw(collection_name: str, payload: dict):
    payload_copy = dict(payload)  # avoid mutating original
    payload_copy["_ingested_at"] = datetime.now(timezone.utc).isoformat()
    payload_copy["_source"] = "ssllabs"
    try:
        coll = db[collection_name]
        result = coll.insert_one(payload_copy)
        print(f"[MONGO] inserted id={result.inserted_id} into {collection_name}")
    except errors.PyMongoError as e:
        print("[MONGO ERROR]", e)
        raise

# Generic GET with retries/backoff and handling 429
class HttpError(Exception):
    pass

@retry(retry=retry_if_exception_type(HttpError),
       wait=wait_exponential(multiplier=1, min=1, max=60),
       stop=stop_after_attempt(MAX_RETRIES))
def safe_get(url, params=None, timeout=REQUEST_TIMEOUT):
    headers = {"User-Agent": "etl-ssllabs/1.0 (+assignment)"}
    try:
        resp = requests.get(url, params=params, timeout=timeout, headers=headers)
    except requests.exceptions.RequestException as e:
        print("[HTTP] network error:", e)
        raise HttpError(e)

    # 429 or other throttling
    if resp.status_code == 429:
        # Prefer Retry-After header if present
        ra = resp.headers.get("Retry-After")
        if ra:
            try:
                sleep_seconds = int(ra)
            except ValueError:
                sleep_seconds = 10
        else:
            sleep_seconds = 10
        print(f"[HTTP] 429 rate limit. Sleeping {sleep_seconds}s and retrying.")
        time.sleep(sleep_seconds)
        raise HttpError("429 rate-limited")

    if resp.status_code >= 400:
        # Some endpoints may return 503 while scan is in progress - propagate for retry
        print(f"[HTTP] status {resp.status_code} for {resp.url} -> body: {resp.text[:200]}")
        raise HttpError(f"HTTP {resp.status_code}")

    try:
        return resp.json()
    except ValueError:
        print("[HTTP] invalid JSON response")
        raise HttpError("invalid json")

# 1) Info endpoint
def run_info():
    url = f"{BASE_URL}/info"
    print("[INFO] calling", url)
    data = safe_get(url)
    if not data:
        print("[INFO] empty response")
    insert_raw(COL_INFO, {"endpoint": "info", "response": data})

# 2) Analyze endpoint: start scan or get cached result
def run_analyze(host: str, start_new: bool = False, from_cache: bool = True):
    url = f"{BASE_URL}/analyze"
    params = {"host": host}
    if start_new:
        params["startNew"] = "on"
    params["fromCache"] = "on" if from_cache else "off"
    print(f"[ANALYZE] calling {url} params={params}")
    data = safe_get(url, params=params)
    if not data:
        print("[ANALYZE] empty response")
    insert_raw(COL_ANALYZE, {"endpoint": "analyze", "host": host, "params": params, "response": data})

    # If analyze returns endpoints list, optionally store endpoint-level entries as well
    endpoints = data.get("endpoints")
    if endpoints:
        # store each endpoint's summary document
        for ep in endpoints:
            insert_raw(COL_ENDPOINT, {"endpoint": "analyze-endpoint-summary", "host": host, "endpoint_summary": ep})

# 3) getEndpointData host+ip
def run_get_endpoint_data(host: str, ip: str):
    url = f"{BASE_URL}/getEndpointData"
    params = {"host": host, "ip": ip}
    print(f"[ENDPOINT] calling {url} params={params}")
    data = safe_get(url, params=params)
    if not data:
        print("[ENDPOINT] empty response")
    insert_raw(COL_ENDPOINT, {"endpoint": "getEndpointData", "host": host, "ip": ip, "response": data})

# small utility: wait politely between calls (rate-limiting courtesy)
def polite_wait(seconds=1.0):
    time.sleep(seconds)

def parse_args():
    ap = argparse.ArgumentParser(description="SSL Labs ETL connector")
    ap.add_argument("--info", action="store_true", help="Call /info")
    ap.add_argument("--analyze", type=str, help="Call /analyze?host=<host>")
    ap.add_argument("--start-new", action="store_true", help="When analyzing, force a new scan")
    ap.add_argument("--from-cache", action="store_true", default=True, help="When analyzing, use cache if available (default: on)")
    ap.add_argument("--endpoint", type=str, help="Call /getEndpointData with --ip")
    ap.add_argument("--ip", type=str, help="IP address for getEndpointData")
    ap.add_argument("--batch-hosts", type=str, help="Path to file with hostnames (one per line) to analyze")
    ap.add_argument("--wait-between", type=float, default=1.0, help="Seconds to wait between API calls (default 1.0s polite)")
    return ap.parse_args()

def main():
    args = parse_args()

    # quick connectivity test to Mongo
    try:
        client.admin.command('ping')
        print("[MONGO] connected OK")
    except Exception as e:
        print("[MONGO] cannot connect:", e)
        sys.exit(1)

    if args.info:
        run_info()

    if args.analyze:
        run_analyze(args.analyze, start_new=args.start_new, from_cache=args.from_cache)
        polite_wait(args.wait_between)

    if args.endpoint:
        if not args.ip:
            print("--endpoint requires --ip")
            sys.exit(1)
        run_get_endpoint_data(args.endpoint, args.ip)
        polite_wait(args.wait_between)

    if args.batch_hosts:
        with open(args.batch_hosts, "r") as fh:
            hosts = [h.strip() for h in fh if h.strip()]
        for h in hosts:
            try:
                run_analyze(h, start_new=False, from_cache=True)
            except Exception as e:
                print(f"[BATCH] error for {h}: {e}")
            polite_wait(args.wait_between)

if __name__ == "__main__":
    main()
