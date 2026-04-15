# 🏥 End-to-End Healthcare Data Cleaning & Analytics Pipeline (MySQL)

## Overview

This repository contains an end-to-end SQL data pipeline built on MySQL for a healthcare dataset.

The project demonstrates how raw patient-level data is ingested from a CSV file, validated, standardized, and transformed into an analytical-ready dataset for healthcare insights and operational analysis.

The pipeline is designed using a layered architecture:

Raw layer → Clean layer → Analytical layer

The main focus of this project is:

* cleaning and standardizing inconsistent healthcare records,
* validating critical medical and financial fields,
* handling invalid date formats and missing values,
* and building a reliable analytical foundation for healthcare insights.

All transformations and analyses are implemented purely in SQL using MySQL.

---

## Architecture Overview

```text
CSV File
   │
   ▼
healthcare_dataset_raw        (Raw ingestion layer)
   │
   ▼
healthcare_dataset_clean      (Clean / analytical base table)
   │
   ▼
Ad-hoc analytical queries
```

This architecture ensures:
* preservation of original data,
* controlled data transformation,
* and reliable analytical outputs.

---

## Data Source

The dataset is loaded from a CSV file:

```
healthcare_dataset.csv
```

The ingestion process simulates a batch load from an external healthcare system using `LOAD DATA INFILE`.

---

## Raw Layer – Data Ingestion

Table:

```
portofolio.healthcare_dataset_raw
```

The raw table stores data exactly as received from the source system.

Columns include:
* patient demographics,
* hospital information,
* medical conditions,
* billing data,
* admission and discharge dates.

No transformation is applied at this stage.

---

## Ingestion Mechanism

Data is loaded using:
* comma-separated values,
* optional quoted fields,
* header row exclusion.

Special handling:
* first column is mapped using a variable (`@name`) to align with auto-increment `patient_id`.

---

## Initial Data Profiling

Basic profiling queries are executed to identify:

* total row count,
* NULL values,
* duplicate records,
* invalid age values,
* missing billing data.

This step ensures early detection of structural issues.

---

## Clean Layer – Standardization & Transformation

Table:

```
portofolio.healthcare_dataset_clean
```

This table represents the curated analytical dataset.

### Transformations Applied:

#### Text Standardization

* all text fields are trimmed and converted to uppercase

#### Gender Normalization

• multiple representations mapped into:

* MALE
* FEMALE
* UNKNOWN

#### Date Validation & Conversion

* only valid `YYYY-MM-DD` format is accepted
* invalid formats converted to NULL

#### Financial Data

* billing_amount preserved as numeric
* invalid or NULL values identified during validation

---

## Data Validation Rules

The following validations are implemented:

* age must be between 0–120
* billing_amount must be ≥ 0
* discharge_date must be ≥ admission_date

Invalid records are not removed but flagged through validation queries.

---

## Analytical Use Cases

All analysis is performed on the clean table.

### 1. Revenue Analysis

* total and average revenue per hospital

### 2. Length of Stay (LOS)

* average hospital stay duration using:

```sql
DATEDIFF(discharge_date, admission_date)
```

### 3. Medical Condition Frequency

* most common diseases across patients

### 4. Medication Usage

* most frequently prescribed medications

### 5. Abnormal Test Rate

* percentage of abnormal test results per hospital

### 6. Revenue Classification

* grouping medical conditions into:

* LOW
* MEDIUM
* HIGH
  based on average billing

---

## Key Engineering Characteristics

This project demonstrates:

* layered data architecture (raw → clean → analytics),
* defensive SQL techniques (REGEXP, NULLIF),
* robust date validation handling,
* separation of ingestion and transformation logic,
* reusable analytical queries.

---

## Indexing Strategy

Indexes are created on:

* hospital
* medical_condition
* admission_date

This improves performance for aggregation and filtering queries.

---

## Audit Logging

Table:

```
portofolio.audit_log
```

Used to track data quality issues such as:

* invalid date relationships

This enables basic data governance tracking.

---

## Assumptions and Scope

* each row represents one patient record
* dataset may contain inconsistencies and missing values
* project focuses on analytical readiness, not transactional design
* pipeline is batch-based

---

## Technology Stack

* MySQL 8.x
* SQL (DDL, DML, Data Cleaning, Analytics)

---

## Repository Structure

```
.
├── data/
│   └── healthcare_dataset.csv
├── sql/
│   └── healthcare_pipeline.sql
└── README.md
```

---

## How to Run This Project

1. Create database and raw table
2. Load CSV into:

```
healthcare_dataset_raw
```

3. Create clean table
4. Run validation queries
5. Execute analytical queries

Ensure:
* file path is allowed by `secure_file_priv`
* MySQL server has file access

---

## Intended Usage

This repository serves as a reference for:

* SQL-based data cleaning pipelines
* healthcare data analysis preparation
* real-world data validation scenarios
* analytical dataset construction

---

## 👤 Author

Y Baskara : https://www.linkedin.com/in/yugobaskara/

Auditors| Data Analyst | SQL | Data Engineering Enthusiast

---

## 📄 Data Source & Attribution

Dataset sourced from a public healthcare dataset (Prasad Patil).

This project is created for educational and portfolio purposes.
All transformation logic and analysis are developed by the author.

---
