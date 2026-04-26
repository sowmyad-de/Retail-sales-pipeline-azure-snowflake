-- ============================================================
-- Retail Sales Pipeline - Gold Layer (Star Schema)
-- Author : Sowmya D
-- ============================================================

USE DATABASE RETAIL_DW;

CREATE SCHEMA IF NOT EXISTS GOLD;
USE SCHEMA GOLD;

-- Dimension Table 1 — Customer
CREATE OR REPLACE TABLE DIM_CUSTOMER (
    customer_id       VARCHAR PRIMARY KEY,
    store_region      VARCHAR
);

-- Dimension Table 2 — Product
CREATE OR REPLACE TABLE DIM_PRODUCT (
    product_id        VARCHAR PRIMARY KEY,
    product_name      VARCHAR,
    category          VARCHAR,
    unit_price        NUMBER(10,2)
);

-- Dimension Table 3 — Date
CREATE OR REPLACE TABLE DIM_DATE (
    date_id           NUMBER PRIMARY KEY,
    full_date         DATE,
    day               NUMBER(2,0),
    month             NUMBER(2,0),
    year              NUMBER(4,0),
    month_name        VARCHAR,
    quarter           NUMBER(1,0)
);

-- Dimension Table 4 — Store
CREATE OR REPLACE TABLE DIM_STORE (
    store_region      VARCHAR PRIMARY KEY
);

-- Fact Table — Sales
CREATE OR REPLACE TABLE FACT_SALES (
    order_id          VARCHAR PRIMARY KEY,
    customer_id       VARCHAR,
    product_id        VARCHAR,
    date_id           NUMBER,
    quantity          NUMBER,
    unit_price        NUMBER(10,2),
    discount_pct      NUMBER(5,2),
    total_order_value NUMBER(12,2),
    payment_method    VARCHAR,
    status            VARCHAR,
    -- Foreign keys pointing to dimension tables
    FOREIGN KEY (customer_id)  REFERENCES DIM_CUSTOMER(customer_id),
    FOREIGN KEY (product_id)   REFERENCES DIM_PRODUCT(product_id),
    FOREIGN KEY (date_id)      REFERENCES DIM_DATE(date_id),
    FOREIGN KEY (store_region) REFERENCES DIM_STORE(store_region)
);

-- ============================================================
-- Load Dimension Tables from Silver
-- ============================================================

-- Load DIM_CUSTOMER
INSERT INTO GOLD.DIM_CUSTOMER
SELECT DISTINCT
    customer_id,
    store_region_clean
FROM SILVER.CLEAN_TRANSACTIONS;

-- Load DIM_PRODUCT
INSERT INTO GOLD.DIM_PRODUCT
SELECT DISTINCT
    product_id,
    product_name,
    category_clean,
    unit_price
FROM SILVER.CLEAN_TRANSACTIONS;

-- Load DIM_DATE
INSERT INTO GOLD.DIM_DATE
SELECT DISTINCT
    TO_NUMBER(TO_CHAR(order_date_clean, 'YYYYMMDD'))  AS date_id,
    order_date_clean                                   AS full_date,
    DAY(order_date_clean)                              AS day,
    MONTH(order_date_clean)                            AS month,
    YEAR(order_date_clean)                             AS year,
    MONTHNAME(order_date_clean)                        AS month_name,
    QUARTER(order_date_clean)                          AS quarter
FROM SILVER.CLEAN_TRANSACTIONS;

-- Load DIM_STORE
INSERT INTO GOLD.DIM_STORE
SELECT DISTINCT
    store_region_clean
FROM SILVER.CLEAN_TRANSACTIONS;

-- Load FACT_SALES
INSERT INTO GOLD.FACT_SALES
SELECT
    order_id,
    customer_id,
    product_id,
    TO_NUMBER(TO_CHAR(order_date_clean, 'YYYYMMDD'))  AS date_id,
    quantity,
    unit_price,
    discount_pct_clean,
    total_order_value,
    payment_method,
    status
FROM SILVER.CLEAN_TRANSACTIONS;
























