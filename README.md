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

This API provides metadata for all CMS Provider Data datasets, including dataset identifiers, dataset themes, last-modified dates, and downloadable CSV distributions. The job uses this endpoint at runtime to dynamically discover all datasets associated with the **Hospitals** theme.

---

## Python Job Workflow

The Python script (`cms_hospitals_job.py`) executes the following steps:

### 1. Metastore Discovery
The job queries the CMS Provider Data metastore API to retrieve metadata for all available datasets, including dataset identifiers, titles, themes, modification dates, and downloadable distributions.

### 2. Dataset Filtering
From the full metastore response, the job filters datasets to include only those associated with the **Hospitals** theme and selects CSV distributions for processing.

### 3. Incremental Execution Logic
Before downloading any files, the job reads the timestamp of the last successful run from a local SQLite database. Each dataset’s `modified` date is compared against this timestamp so that only datasets updated since the previous successful run are downloaded.

### 4. Parallel Download and Processing
Eligible datasets are downloaded and processed in parallel using a thread pool. CSV files are streamed directly to disk to avoid loading large datasets into memory.

### 5. Column Header Normalization
For each CSV file, the header row is rewritten by converting mixed-case column names, spaces, and special characters into standardized `snake_case` column names while preserving all row data.

**Example:**



