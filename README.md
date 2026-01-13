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
Patients’ rating of the facility linear mean score → patients_rating_of_the_facility_linear_mean_score

### 6. Output Artifact Generation
For each processed dataset, the job produces:
- A processed CSV file with normalized headers  
- A JSON header-mapping file showing original → normalized column names  

### 7. State Persistence
After each dataset completes processing, the main thread records dataset metadata, timestamps, and processing status in a local SQLite database to support future incremental runs.

### 8. Run Reporting
At the end of each execution, the job writes a per-run JSON report summarizing execution metadata, dataset counts, success and error status, and output artifact locations.

### 9. Daily Scheduling Compatibility
The job is designed to run daily using standard system schedulers such as **cron** (Linux) or **Task Scheduler** (Windows). Incremental logic ensures that only newly modified datasets are processed on subsequent runs.

---

## Design Decisions

### Metastore-Driven Discovery
Dataset URLs are not hardcoded. Instead, the job dynamically queries the CMS Provider Data metastore API at runtime. This allows the pipeline to automatically adapt to newly added datasets, removed datasets, or changes in download URLs without requiring code changes.

### Incremental Processing with Persistent State
A local SQLite database is used to persist run and dataset metadata across executions. This enables safe daily scheduling, clean restarts after failures, and efficient incremental processing. SQLite was chosen for its portability, reliability, and zero external service dependencies.

### Parallel I/O with Streaming Processing
Parallelism is applied to network and file I/O to improve throughput. CSV files are streamed row-by-row rather than loaded entirely into memory, making the solution safe for large datasets and constrained environments.

### Explicit Schema Normalization
Column headers are normalized deterministically to `snake

---

## Concurrency and SQLite Issue Encountered

### The Issue
During development, an issue was encountered when combining parallel file processing with SQLite state tracking. SQLite connections are, by default, restricted to the thread in which they are created. When worker threads attempted to write dataset state directly to SQLite, the following error occurred:

   sqlite3.ProgrammingError: SQLite objects created in a thread can only be used in that same thread

This behavior is expected and commonly encountered when using SQLite in multi-threaded applications.

### Resolution
To resolve this cleanly and safely, the design was adjusted so that:
- Worker threads perform only network I/O and file processing
- All SQLite reads and writes occur exclusively in the main thread

Each worker thread returns a structured result object describing its outcome. The main thread consumes these results and performs all database updates sequentially.

### Outcome
This approach preserves parallelism where it provides the most benefit (network and file I/O) while ensuring deterministic, reliable state management. It avoids the need for complex locking, thread-safe connection configurations, or external database services and is well-suited for SQLite-backed batch jobs.

   
---

## How to Run

The job can be run locally on any standard Windows or Linux machine with Python 3.9+ installed.

### 1. Clone the repository
```bash
git clone https://github.com/jdvisual/cms-hospitals-ingestion.git
cd cms-hospitals-ingestion

create enviornment
python -m venv .venv
.\.venv\Scripts\Activate.ps1


