# Factory Data Pipeline Automation

A lightweight data pipeline prototype that simulates factory order and delivery reporting using synthetic data.

## Overview

This project is a public, interview-friendly prototype inspired by factory data automation work. It demonstrates how raw operational data can be transformed through layered pipeline steps into business-facing KPI outputs, while also supporting basic data quality checks and anomaly monitoring.

The project is intentionally simplified for portfolio use, but it preserves the core ideas behind a real analytics pipeline:
- raw data ingestion
- layered transformation logic
- business KPI generation
- data quality validation
- monitoring and alerting

## Business Problem

Factory operations data often comes from multiple systems and may contain duplicate records, missing fields, inconsistent status values, and incomplete downstream events. When reporting relies on manual cleanup, the process becomes slow, error-prone, and difficult to monitor.

This project simulates how a lightweight pipeline can automate that workflow and improve reporting reliability.

## Project Goals

- Generate synthetic factory raw data
- Transform data through source, prepare, and semantic layers
- Produce daily KPI outputs for reporting
- Run basic data quality checks
- Detect unusual day-over-day drops in order volume

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
├── data/
│   ├── raw/
│   │   ├── factory_orders_raw.csv
│   │   └── factory_deliveries_raw.csv
│   ├── clean/
│   │   └── factory_orders_clean.csv
│   └── output/
│       ├── daily_factory_kpi.csv
│       ├── dq_report.csv
│       └── alert_log.csv
└── images/
    └── daily_total_orders.png
```

## Pipeline Architecture

### 1. Raw Layer

Synthetic raw data is generated for:
- factory orders
- factory deliveries

The raw layer intentionally includes realistic data issues such as:
- duplicate records
- missing supplier IDs
- invalid quantities
- inconsistent status values
- orphan deliveries
- one low-volume day for anomaly detection

### 2. Source Layer

The pipeline reads raw CSV files and performs basic schema handling:
- date parsing
- field type normalization
- raw-to-source ingestion preparation

### 3. Prepare Layer

The pipeline then applies transformation logic to create an analysis-ready table:
- duplicate removal
- status standardization
- invalid quantity filtering
- joining deliveries back to orders
- calculating delivery delay
- deriving on-time flags
- deriving delivery coverage fields

### 4. Semantic Layer

The semantic layer aggregates the prepared data into daily reporting outputs:
- total orders
- total deliveries
- on-time delivery rate
- average delivery delay days

### 5. Monitoring Layer

The project also generates:
- a data quality report
- an alert log for large day-over-day order volume drops

## Output Files

### Prepare Layer Table
`data/clean/factory_orders_clean.csv`

### Semantic Layer KPI Table
`data/output/daily_factory_kpi.csv`

### Data Quality Report
`data/output/dq_report.csv`

### Alert Log
`data/output/alert_log.csv`

## KPI Visualization
![Daily Total Orders](images/daily_total_orders.png)

## Example Data Quality Checks

The project currently includes:
- duplicate order ID check
- missing supplier ID check
- invalid order quantity check
- orphan delivery check

## Example Alert Rule

An alert is triggered when daily total orders drop by more than 40% compared with the previous day.

## How to Run

### Step 1: Generate raw data
Run `generate_fake_data.py`

### Step 2: Build source, prepare, and semantic outputs
Run `pipeline.py`

### Step 3: Run data quality checks and alert logic
Run `checks.py`

## Why I Built This Project

I created this project as a public prototype to translate more complex factory data automation work into a simpler, portfolio-ready example. The goal was to keep the project easy to explain while still demonstrating layered ETL design, KPI reporting, data quality checks, and anomaly monitoring.

## Future Improvements

- Add incremental processing with a watermark file
- Rename output folders to match source / prepare / semantic more explicitly
- Add a dashboard layer
- Add unit tests
- Add more factory domains such as inventory and returns
- Deploy the pipeline in a cloud environment
