# Retail Sales Data Pipeline — End-to-End Azure & Snowflake Project

![Pipeline Architecture](docs/architecture.png)

A complete data engineering pipeline that ingests raw retail transaction data, applies data quality transformations using **Medallion Architecture**, and structures it into a **Star Schema** in Snowflake for business analytics.

Built as part of my MSc in Computer Science (Saint Leo University, 2024–2025) to demonstrate end-to-end data engineering skills on Azure and Snowflake.

---

## Project Overview

A mid-size retail company receives daily transaction data from multiple store systems. The data arrives messy — mixed date formats, duplicate records, null values, and inconsistent categories. This pipeline solves that by:

1. **Ingesting** raw data into a Bronze landing layer
2. **Cleaning** it through a Silver transformation layer
3. **Structuring** it into a Gold star schema for reporting
4. **Answering** real business questions through analytical queries

---

## Architecture

```
Raw CSV (Source Data)
        ↓
Azure Data Factory (Orchestration)
        ↓
ADLS Gen2 (Raw Storage)
        ↓
┌─────────────────────────────────┐
│         SNOWFLAKE               │
│                                 │
│  BRONZE  →  SILVER  →  GOLD    │
│  (Raw)     (Clean)   (Star)    │
└─────────────────────────────────┘
        ↓
  Analytical Queries
  (Business Insights)
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Azure Data Factory |
| Storage | Azure Data Lake Storage Gen2 |
| Transformation | PySpark on Azure Databricks |
| Data Warehouse | Snowflake |
| Data Format | Delta Lake (Bronze→Silver) |
| Modelling | Star Schema (Gold Layer) |
| Version Control | GitHub |

---

## Data Quality Issues Handled

The source data contained 8 intentional real-world problems:

| Problem | Solution |
|---|---|
| Mixed date formats (`yyyy-MM-dd` vs `dd-MM-yyyy`) | CASE + RLIKE + TRY_TO_DATE in Silver |
| Duplicate orders (API retry logic) | ROW_NUMBER() deduplication |
| Null `store_region` | COALESCE → 'Unknown' |
| Null `discount_pct` | COALESCE → 0 |
| Negative quantity (returns) | Filtered out, separated for returns tracking |
| Zero/null `unit_price` | Removed — cannot calculate order value |
| Null `customer_id` | Removed — orphan records |
| Inconsistent category case (`SPORTS`, `sports`) | INITCAP + TRIM |

**Result: 124 raw rows → 110 clean rows (14 bad records removed)**

---

## Medallion Architecture

### Bronze Layer — Raw Landing
- All columns stored as VARCHAR
- No transformations applied
- Preserves original source data for reprocessing
- Handles schema drift gracefully

### Silver Layer — Cleaned & Validated
- Proper data types enforced (DATE, NUMBER, VARCHAR)
- All 8 data quality issues resolved
- `total_order_value` derived: `quantity × unit_price × (1 - discount_pct/100)`
- Date parts extracted for partitioning

### Gold Layer — Star Schema
```
           DIM_CUSTOMER
           (16 customers)
                |
DIM_DATE ── FACT_SALES ── DIM_PRODUCT
(83 dates)  (110 orders)  (22 products)
                |
           DIM_STORE
```

---

## Business Insights

### Revenue by Category
| Category | Total Revenue |
|---|---|
| Electronics | Highest 🏆 |
| Home | High avg order value ($160.68) |
| Sports | Most orders |

### Monthly Revenue Trend
| Month | Revenue |
|---|---|
| January 2024 | $3,744.15 |
| March 2024 | $3,442.87 |
| February 2024 | $3,387.20 |

### Top Customer
**CUST-820** with $1,987.24 total spend

### Average Order Value by Category
- Home: $160.68 (highest — Standing Desk, Air Purifier)
- Electronics: $148.70
- Sports: $56.74 (lowest — affordable items, high volume)

---

## Project Structure

```
retail-sales-pipeline-azure-snowflake/
│
├── README.md
├── data/
│   └── sample_transactions.csv       ← 124 rows, intentionally messy
├── pyspark/
│   └── transform_retail.py           ← PySpark cleaning + Delta Lake
├── snowflake/
│   ├── 01_bronze_tables.sql          ← Raw landing table
│   ├── 02_silver_transforms.sql      ← Data cleaning CTEs
│   ├── 03_gold_star_schema.sql       ← Star schema + dimension loads
│   └── 04_analytical_queries.sql     ← Business insight queries
└── docs/
    └── architecture.md               ← Architecture details
```

---

## Key Technical Decisions

**Why all VARCHAR in Bronze?**
Snowflake rejects rows if data types don't match. By accepting everything as text first, we guarantee 100% ingestion and handle type enforcement in Silver — a standard production pattern.

**Why ROW_NUMBER() instead of DISTINCT for deduplication?**
DISTINCT removes all duplicates but doesn't tell you which record to keep. ROW_NUMBER() with ORDER BY ingestion timestamp lets us keep the first arrival and discard retries — preserving auditability.

**Why INNER JOIN in analytical queries?**
We only want revenue for orders where the product category is known. A LEFT JOIN would include NULL categories which would distort grouping.

**Real challenge faced: Schema Drift**
During development, the source CSV had more columns than the Bronze table — Snowflake rejected the load. Fixed by redesigning Bronze to accept all source columns, mirroring the real-world pattern of schema-on-read at the Bronze layer.

---

## How to Run

1. Clone this repository
2. Upload `data/sample_transactions.csv` to your Snowflake stage
3. Run SQL files in order: `01` → `02` → `03` → `04`
4. For PySpark: deploy `transform_retail.py` on Azure Databricks

---

## Author

**Sowmya D** —Data Engineer
MSc Computer Science, Saint Leo University (2024–2025)

[GitHub](https://github.com/sowmyad-de) | [LinkedIn](https://linkedin.com/in/sowmyad)

---

*This project was built to demonstrate end-to-end data engineering skills including pipeline design, data quality handling, cloud data warehousing, and analytical SQL.*
