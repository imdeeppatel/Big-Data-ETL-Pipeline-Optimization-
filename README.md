# Big Data ETL Pipeline Optimization

**Cross-Domain Analytics Initiative**

A cloud-native ETL pipeline architecture built on PySpark, SQL, and AWS that processes large-scale healthcare and financial datasets — reducing analysis time by 30% and enabling self-service analytics that freed stakeholders from centralized data team dependencies.

---

## Overview

This initiative modernized fragmented, slow ETL workflows across healthcare and financial data domains into a unified, optimized pipeline architecture. By combining distributed PySpark processing with a fully managed AWS cloud data stack (S3, Glue, QuickSight), the platform dramatically improved data freshness, reduced pipeline runtimes, and put self-service analytics directly in the hands of business stakeholders.

---

## Key Features

- **Optimized PySpark ETL Pipelines** — Redesigned transformation logic for distributed execution, eliminating bottlenecks in large-scale data processing
- **AWS Cloud Data Infrastructure** — End-to-end cloud stack using S3 (storage), Glue (cataloging + ETL), and QuickSight (self-service BI)
- **Cross-Domain Data Processing** — Unified pipeline handling both healthcare and financial datasets with domain-specific transformation logic
- **Self-Service Analytics** — QuickSight dashboards enabling stakeholders to run ad hoc reports without engaging the central data team
- **Improved Data Freshness** — Pipeline optimization reduced latency between source data updates and downstream BI availability

---

## Tech Stack

| Layer | Technology |
|---|---|
| Distributed Processing | PySpark (Apache Spark) |
| Transformation Logic | SQL, PySpark DataFrames |
| Cloud Storage | AWS S3 |
| Data Catalog & ETL | AWS Glue |
| Self-Service BI | AWS QuickSight |
| Orchestration | AWS Glue Workflows / Apache Airflow |
| Data Domains | Healthcare (EHR), Financial |

---

## Architecture

```
Source Systems
(EHR / Healthcare Data, Financial Data)
        │
        ▼
  AWS S3 — Raw Zone
  (landing layer for all ingested data)
        │
        ▼
  AWS Glue + PySpark ETL Jobs
  ┌─────────────────────────────────┐
  │  - Schema validation            │
  │  - Cleaning & normalization     │
  │  - Domain-specific transforms   │
  │  - Partitioning & optimization  │
  └─────────────────────────────────┘
        │
        ▼
  AWS S3 — Curated Zone
  (analytics-ready, partitioned datasets)
        │
        ▼
  AWS Glue Data Catalog
  (schema registry, table discovery)
        │
        ├──────────────────────────────┐
        ▼                              ▼
  AWS QuickSight                 Downstream BI Tools
  (self-service dashboards)      (Tableau, reporting layers)
        │
        ▼
  Business Stakeholders
  (ad hoc reporting, no data team dependency)
```

---

## Pipeline Layers

### 1. Raw Zone (Landing)
- All source data lands in S3 raw zone as-is (CSV, JSON, Parquet)
- No transformations applied — preserves source fidelity
- AWS Glue crawlers auto-catalog incoming datasets

### 2. Staging Zone (Cleansed)
- PySpark jobs handle null handling, deduplication, type casting, and schema enforcement
- Domain-specific validation rules applied (healthcare vs. financial data)
- Outputs written as partitioned Parquet for query efficiency

### 3. Curated Zone (Analytics-Ready)
- Aggregated, joined, and enriched datasets optimized for BI consumption
- SQL-based transformations for business KPIs and metrics
- Partition pruning and columnar formats minimize QuickSight query costs and latency

---

## Optimization Techniques

| Technique | Impact |
|---|---|
| Partition pruning on S3 datasets | Reduced scan volume for downstream queries |
| Columnar format (Parquet) conversion | Faster reads, lower storage cost |
| PySpark broadcast joins | Eliminated shuffle bottlenecks on large joins |
| Predicate pushdown in Glue jobs | Reduced data movement across pipeline stages |
| Pipeline parallelization | Concurrent domain pipelines reduced total runtime |

**Overall Result: ~30% reduction in end-to-end analysis time**

---

## AWS Infrastructure

### S3 Bucket Structure
```
s3://project-data-lake/
├── raw/
│   ├── healthcare/
│   └── financial/
├── staging/
│   ├── healthcare/
│   └── financial/
└── curated/
    ├── healthcare_kpis/
    └── financial_kpis/
```

### AWS Glue
- Crawlers auto-discover and catalog new data in raw zone
- PySpark ETL jobs managed as Glue jobs for serverless execution
- Glue Workflows orchestrate multi-step pipeline runs with dependency management

### AWS QuickSight
- Connected directly to Glue Data Catalog for schema-aware dataset creation
- SPICE in-memory engine used for high-performance dashboard queries
- Row-level security configured for domain-appropriate data access

---

## Project Structure

```
etl-pipeline-optimization/
├── glue_jobs/
│   ├── healthcare/
│   │   ├── raw_to_staging.py         # Healthcare cleaning & normalization
│   │   └── staging_to_curated.py     # Healthcare KPI aggregations
│   └── financial/
│       ├── raw_to_staging.py         # Financial cleaning & normalization
│       └── staging_to_curated.py     # Financial KPI aggregations
├── pyspark/
│   ├── utils/
│   │   ├── schema_validator.py       # Schema enforcement utilities
│   │   ├── deduplication.py          # Dedup logic across domains
│   │   └── partitioning.py           # Partition strategy helpers
│   └── transformations/
│       ├── healthcare_transforms.py  # Domain-specific logic
│       └── financial_transforms.py   # Domain-specific logic
├── sql/
│   ├── healthcare_kpis.sql           # KPI definitions — healthcare
│   └── financial_kpis.sql            # KPI definitions — financial
├── infrastructure/
│   ├── glue_workflows.json           # Glue Workflow definitions
│   └── s3_bucket_policy.json         # IAM and bucket policies
├── notebooks/
│   ├── pipeline_profiling.ipynb      # Runtime analysis & bottleneck identification
│   └── optimization_results.ipynb    # Before/after performance comparison
├── docs/
│   ├── architecture.md               # Full pipeline architecture documentation
│   └── data_dictionary.md            # Field-level documentation per domain
├── requirements.txt
└── README.md
```

---

## Setup & Usage

### Prerequisites

- Python 3.9+
- AWS account with access to S3, Glue, and QuickSight
- PySpark environment (local or via AWS Glue)

### Installation

```bash
git clone https://github.com/your-org/etl-pipeline-optimization.git
cd etl-pipeline-optimization
pip install -r requirements.txt
```

### Running Locally (PySpark)

```bash
# Healthcare pipeline
spark-submit pyspark/transformations/healthcare_transforms.py \
  --input s3://project-data-lake/raw/healthcare/ \
  --output s3://project-data-lake/staging/healthcare/

# Financial pipeline
spark-submit pyspark/transformations/financial_transforms.py \
  --input s3://project-data-lake/raw/financial/ \
  --output s3://project-data-lake/staging/financial/
```

### Deploying Glue Jobs

```bash
# Upload job scripts to S3
aws s3 cp glue_jobs/ s3://your-glue-scripts-bucket/jobs/ --recursive

# Trigger Glue workflow
aws glue start-workflow-run --name etl-pipeline-workflow
```

---

## Impact

- **~30% reduction in analysis time** — Pipeline optimizations (partitioning, Parquet, broadcast joins) cut end-to-end processing and query latency
- **Improved data freshness** — Faster pipelines reduced lag between source updates and BI-ready data availability
- **Self-service analytics at scale** — QuickSight + Glue Catalog enabled stakeholders to run ad hoc reports independently, reducing data team ticket volume
- **Cross-domain scalability** — Unified architecture handles both healthcare and financial datasets with domain-isolated transformation logic

---

## Compliance & Governance

- Healthcare data processed in compliance with HIPAA data handling requirements
- Financial data subject to client data governance and access control policies
- Row-level security enforced in QuickSight to restrict domain-level data access by role
- No raw PII committed to version control or exposed in BI layers

---

*Cross-Domain Analytics Initiative — Internal*
