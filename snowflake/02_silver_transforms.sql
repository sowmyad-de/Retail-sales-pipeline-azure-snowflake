CREATE DATABASE IF NOT EXISTS RETAIL_DW;
USE DATABASE RETAIL_DW;
CREATE SCHEMA IF NOT EXISTS SILVER;
USE SCHEMA SILVER;

CREATE OR REPLACE TABLE SILVER.CLEAN_TRANSACTIONS (
    order_id            VARCHAR,
    customer_id         VARCHAR,
    product_id          VARCHAR,
    order_date_clean    DATE,
    quantity            NUMBER,
    unit_price          NUMBER(10,2),
    discount_pct_clean  NUMBER(5,2),
    store_region_clean  VARCHAR,
    category_clean      VARCHAR,
    status              VARCHAR,
    total_order_value   NUMBER(12,2),
    ingestion_ts        TIMESTAMP
);
WITH deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY ingestion_ts) AS rn
    FROM RAW_TRANSACTIONS
),
fixed_dates AS (
    SELECT *,
        CASE
            WHEN order_date RLIKE '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
                THEN TRY_TO_DATE(order_date, 'DD-MM-YYYY')
            ELSE
                TRY_TO_DATE(order_date, 'YYYY-MM-DD')
        END AS order_date_clean
    FROM deduped
    WHERE rn = 1
),
nulls_handled AS (
    SELECT *,
        COALESCE(store_region, 'Unknown')  AS store_region_clean,
        COALESCE(discount_pct, 0)          AS discount_pct_clean,
        INITCAP(TRIM(category))            AS category_clean
    FROM fixed_dates
),
validated AS (
    SELECT *,
        ROUND(quantity * unit_price * (1 - discount_pct_clean / 100), 2) AS total_order_value
    FROM nulls_handled
    WHERE quantity > 0
    AND unit_price > 0
    AND unit_price IS NOT NULL
    AND customer_id IS NOT NULL
)
INSERT INTO SILVER.CLEAN_TRANSACTIONS
SELECT
    order_id,
    customer_id,
    product_id,
    order_date_clean,
    quantity,
    unit_price,
    discount_pct_clean,
    store_region_clean,
    category_clean,
    status,
    total_order_value,
    ingestion_ts
FROM validated;











