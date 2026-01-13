# CMS Hospitals Provider Data Ingestion

This repository contains a Python job that incrementally discovers, downloads, and processes **CMS Provider Data** datasets related to the **Hospitals** theme. The job queries the official CMS Provider Data metastore API, downloads CSV distributions in parallel, normalizes all column headers to `snake_case`, and persists execution state locally to support reliable daily runs.

The solution is designed to run on a standard Windows or Linux machine, avoids cloud-specific tooling, and uses minimal external dependencies.
---

## Requirements

This project implements a portable, production-ready Python job that programmatically discovers, downloads, and processes CMS Provider Data related to the **Hospitals** theme. The job queries the official CMS Provider Data metastore API to identify all relevant datasets, downloads only CSV distributions, and normalizes all column headers to `snake_case`. Files are downloaded and processed in parallel, and persistent state tracking is used to ensure that only datasets modified since the previous successful run are processed. The solution is designed to run daily on a standard Windows or Linux machine and manages all non-standard dependencies via a minimal `requirements.txt` file.
---

## Data Source

**CMS Provider Data Metastore (Dataset Catalog)**  
https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items



