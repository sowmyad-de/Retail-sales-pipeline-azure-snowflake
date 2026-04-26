# Retail Sales Data Pipeline — Snowflake SQL Project

An end-to-end retail sales data pipeline built entirely in Snowflake, following the **Medallion Architecture** (Bronze → Silver → Gold) with a **Star Schema** for business analytics.

Built as part of my MSc in Computer Science (Saint Leo University, 2024–2025).

---

## What This Project Does

A retail company receives daily transaction data from store systems. The data arrives messy — duplicate records, mixed date formats, null values, and inconsistent categories. This pipeline:

1. **Ingests** raw data into a Bronze landing layer (as-is, no transformation)
2. **Cleans** it in a Silver layer using SQL transformations
3. **Structures** it into a Gold star schema for reporting
4. **Answers** real business questions through analytical SQL queries

---

## Tech Stack

| Component | Technology |
|---|---|
| Data Warehouse | Snowflake |
| Transformation | SQL (CTEs, Window Functions) |
| Architecture | Medallion (Bronze/Silver/Gold) |
| Modelling | Star Schema |
| Version Control | GitHub |

---

## Data Quality Issues Handled

Source data had 8 real-world problems:

| Problem | How Fixed |
|---|---|
| Mixed date formats (yyyy-MM-dd vs dd-MM-yyyy) | CASE + RLIKE + TRY_TO_DATE |
| Duplicate orders (API retry logic) | ROW_NUMBER() deduplication |
| Null store_region | COALESCE → 'Unknown' |
| Null discount_pct | COALESCE → 0 |
| Negative quantity (returns) | Filtered and separated |
| Zero/null unit_price | Removed — cannot calculate order value |
| Null customer_id | Removed — orphan records |
| Inconsistent category case (SPORTS, sports, Sports) | INITCAP + TRIM |

**Result: 124 raw rows → 110 clean rows (14 bad records removed)**

---

## Medallion Architecture

### Bronze Layer
- All columns VARCHAR — accepts everything as-is
- No transformations — raw data preserved for reprocessing
- Handles schema drift gracefully

### Silver Layer
- Proper data types enforced (DATE, NUMBER, VARCHAR)
- All 8 data quality issues resolved using CTE chain
- total_order_value derived: quantity x unit_price x (1 - discount_pct/100)

### Gold Layer — Star Schema

```
           DIM_CUSTOMER
           (16 customers)
                |
DIM_DATE — FACT_SALES — DIM_PRODUCT
(83 dates)  (110 orders)  (22 products)
                |
           DIM_STORE
```

---

## Business Insights

### Revenue by Category
Electronics had the highest total revenue across all 3 months.

### Monthly Revenue Trend
| Month | Revenue |
|---|---|
| January 2024 | $3,744.15 |
| March 2024 | $3,442.87 |
| February 2024 | $3,387.20 |

### Top Customer
CUST-820 with $1,987.24 total spend

### Average Order Value by Category
| Category | Avg Order Value |
|---|---|
| Home | $160.68 |
| Electronics | $148.70 |
| Footwear | $102.00 |
| Health | $75.67 |
| Accessories | $65.61 |
| Sports | $56.74 |

---

## Project Structure

```
retail-sales-pipeline-azure-snowflake/
│
├── README.md
├── data/
│   └── sample_transactions.csv       <- 124 rows, intentionally messy
├── snowflake/
│   ├── 01_bronze_tables.sql          <- Raw landing table
│   ├── 02_silver_transforms.sql      <- Data cleaning CTEs
│   ├── 03_gold_star_schema.sql       <- Star schema + dimension loads
│   └── 04_analytical_queries.sql     <- Business insight queries
```

---

## Key Technical Decisions

**Why all VARCHAR in Bronze?**
Snowflake rejects rows if data types do not match. Accepting everything as text first guarantees 100% ingestion and handles type enforcement in Silver — a standard production pattern.

**Why ROW_NUMBER() for deduplication?**
DISTINCT removes duplicates but does not control which record to keep. ROW_NUMBER() with ORDER BY ingestion timestamp keeps the first arrival and discards retries — preserving auditability.

**Why INNER JOIN in analytical queries?**
Only want revenue for orders where the product category is known. A LEFT JOIN would include NULL categories and distort grouping.

**Real challenge faced: Schema Drift**
During development, the source CSV had more columns than the Bronze table — Snowflake rejected the load. Fixed by redesigning Bronze to accept all source columns, mirroring the real-world pattern of schema-on-read at the Bronze layer.

---

## How to Run

1. Clone this repository
2. Sign up for Snowflake free trial at snowflake.com
3. Run SQL files in order: 01 → 02 → 03 → 04
4. Load data/sample_transactions.csv into Bronze using Snowflake Load Data UI

---

## Author

**Sowmya D** — Data Engineer
MSc Computer Science, Saint Leo University (2024–2025)

GitHub: https://github.com/sowmyad-de
