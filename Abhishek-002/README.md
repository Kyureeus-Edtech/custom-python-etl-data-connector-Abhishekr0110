# Spamhaus DROP List ETL Connector

**Kyureeus EdTech | SSN CSE — Software Architecture Assignment**

## Overview
This connector:
- Downloads the [Spamhaus DROP List](https://www.spamhaus.org/drop/drop.txt) (CIDR blocks of hijacked/spam networks).
- Transforms it into structured JSON objects.
- Loads it into a MongoDB collection for analysis or blocking rules.

## Workflow
1. **Extract** — Fetch plain text list from Spamhaus.
2. **Transform** — Remove comments, split into `cidr` + `description`.
3. **Load** — Insert into MongoDB (`spamhaus_drop_raw`) with `_id` as the CIDR.

## Installation

```bash
python3 -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
