# Azure Fintech Data Migration Pipeline

An end-to-end data engineering pipeline that migrates fintech data (customers, accounts, loans, transactions, payments) from Azure SQL Database into a governed Delta Lake using the Medallion Architecture (Bronze → Silver → Gold), orchestrated with Azure Data Factory and processed with Azure Databricks.

## Overview

This project simulates a real-world fintech data migration scenario: moving core banking data from a relational source system into a cloud-native lakehouse, with data quality validation, business-rule transformations, and a dimensional (star schema) model ready for analytics and reporting.

## Architecture

```
Azure SQL Database (source)
        │
        │  ADF: Lookup (get table list) → ForEach → Copy Activity
        ▼
ADLS Gen2 — Bronze Layer (raw, as-is)
        │
        │  Databricks Notebook: validate → cleanse → enrich → upsert (MERGE)
        ▼
ADLS Gen2 — Silver Layer (cleaned, standardized, business-enriched)
        │
        │  Databricks Notebook: build dimensions, facts, aggregates
        ▼
ADLS Gen2 — Gold Layer (star schema, analytics-ready)
        │
        │  ADF: Web Activity → Logic App
        ▼
Email Notification (pipeline success / failure)
```

Credentials (SQL Database access) are managed centrally via **Azure Key Vault**, accessed through a Databricks Key Vault-backed secret scope — no credentials are stored in code.

## Tech Stack

| Layer | Technology |
|---|---|
| Source system | Azure SQL Database |
| Orchestration | Azure Data Factory |
| Compute / Transformation | Azure Databricks, PySpark |
| Storage | Azure Data Lake Storage Gen2 (Delta Lake format) |
| Secrets management | Azure Key Vault + Databricks Secret Scope |
| Alerting | Azure Logic Apps |
| Data governance | Unity Catalog (External Location for secure storage access) |

## Pipeline Stages

### 1. Bronze — Raw Ingestion
An ADF pipeline dynamically discovers all tables in the source database (via `INFORMATION_SCHEMA.TABLES`) and copies each one into ADLS Gen2 as-is, using a parameterized `ForEach` loop — no hardcoded table list, so new source tables are picked up automatically.

### 2. Silver — Validation, Cleansing & Enrichment
A Databricks notebook:
- **Validates row counts** between the source SQL tables and the Bronze layer, failing the pipeline on mismatch to prevent incomplete data from propagating downstream
- **Scores data quality** per table (null-rate based) and flags tables falling below a 95% threshold
- **Cleanses and standardizes** each table — e.g., normalizing account types, capping invalid interest rates, trimming/casing text fields, correcting negative balances
- **Enriches** each table with derived business fields — customer segment/tier, account status/tier, loan risk category, transaction category/size, payment method grouping
- **Writes using Delta `MERGE` (upsert)** rather than overwrite, keyed on each table's primary key — making writes idempotent and safe to re-run without duplicating data

### 3. Gold — Star Schema
A second Databricks notebook builds an analytics-ready dimensional model:
- **Dimensions:** `dim_customers`, `dim_accounts`, `dim_loans`, `dim_time`
- **Facts:** `fact_transactions`, `fact_payments`, `fact_customer_accounts`
- **Aggregates:** `agg_customer_summary`, `agg_account_summary`, `agg_loan_summary` — pre-computed rollups for reporting (transaction volume, deposit/withdrawal totals, late payment rates, etc.)

### 4. Alerting
On pipeline completion, an ADF Web Activity calls an Azure Logic App, which sends a success or failure email — giving visibility into pipeline runs without manual monitoring.

## Key Engineering Decisions

**Synapse → Databricks pivot.** The pipeline was originally designed around Azure Synapse Analytics for both orchestration-adjacent compute and SQL-based validation. During implementation, the subscription's SQL Database server provisioning was blocked across every available region (`SqlServerRegionDoesNotAllowProvisioning`). Rather than wait on a support ticket, the architecture was redesigned around **ADF + Databricks**, moving both transformation and row-count validation logic into PySpark — arguably a better real-world pattern, since data quality checks generally belong in the compute layer where richer validation (not just counts) is possible.

**Key Vault-backed secrets.** Initial secret scope setup hit a tenant-level Conditional Access policy requiring an On-Behalf-Of (OBO) authentication flow between Databricks and Key Vault — a common restriction on education-tier Azure subscriptions. This was resolved by correcting the Key Vault's network access configuration (enabling trusted Microsoft services) and properly scoping access policies for both the user and the Databricks service principal.

**Idempotent writes.** Silver-layer writes use Delta Lake `MERGE` (upsert) keyed on primary key, rather than a full overwrite each run — a closer match to how real incremental/CDC-style pipelines behave, and safer to re-run without data duplication.

## Repository Structure

```
azure-fintech-data-pipeline/
├── adf-pipelines/
│   └── PL_FintechMigration.json      # ADF pipeline definition
├── databricks-notebooks/
│   ├── 01_bronze_to_silver.ipynb     # Validation, cleansing, enrichment, upsert
│   └── 02_silver_to_gold.ipynb       # Star schema build
├── sql_database_tables/
│   ├── Customers.sql
│   ├── Accounts.sql
│   ├── Loans.sql
│   ├── Transactions.sql
│   └── Payments.sql
└── README.md
```

## Possible Extensions

- Source-side incremental extraction using a watermark column (`LastModifiedDate`) and a pipeline control table, rather than full source reads each run
- Quarantine path for records failing data quality checks (currently logged as warnings)
- Pipeline run audit log (start/end time, row counts, pass/fail) as a Gold-layer table
- AI-generated business summary of Gold-layer aggregates, delivered via automated email

## Author

Soujanya Mandula — [LinkedIn](https://www.linkedin.com/in/soujanyamandula/)
