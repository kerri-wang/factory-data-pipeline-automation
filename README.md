# Factory Data Pipeline Automation

A simple prototype ETL project that simulates factory order and delivery reporting using synthetic data.

## Project Overview

This project demonstrates a lightweight factory data pipeline built for portfolio and interview purposes. It uses synthetic order and delivery data to simulate a realistic reporting workflow, including raw data ingestion, data cleaning, KPI generation, data quality checks, and simple anomaly monitoring.

## Business Problem

Factory operations data often comes from multiple sources and may contain duplicates, missing fields, inconsistent statuses, and reporting delays. Manual reporting can be slow and error-prone.

This prototype shows how a small ETL pipeline can automate daily reporting and improve data reliability.

## Project Goals

- Generate synthetic factory raw data
- Clean and standardize order and delivery records
- Build daily KPI reporting outputs
- Run basic data quality checks
- Detect unusual drops in order volume

## Tech Stack

- Python
- Pandas
- NumPy
- Google Colab
- GitHub

## Project Structure

```text
factory-data-pipeline-automation/
├── README.md
├── requirements.txt
├── src/
│   ├── generate_fake_data.py
│   ├── pipeline.py
│   └── checks.py
└── data/
    ├── raw/
    │   ├── factory_orders_raw.csv
    │   └── factory_deliveries_raw.csv
    ├── clean/
    │   └── factory_orders_clean.csv
    └── output/
        ├── daily_factory_kpi.csv
        ├── dq_report.csv
        └── alert_log.csv
