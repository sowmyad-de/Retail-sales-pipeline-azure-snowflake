-- ============================================================
-- Retail Sales Pipeline - Bronze Layer
-- Raw landing tables — no transformations applied
-- All columns VARCHAR to accept messy data without rejection
-- Author : Sowmya D
-- ============================================================

CREATE DATABASE IF NOT EXISTS RETAIL_DW;
USE DATABASE RETAIL_DW;

CREATE SCHEMA IF NOT EXISTS BRONZE;
USE SCHEMA BRONZE;

-- Raw transactions — one row per order
CREATE OR REPLACE TABLE RAW_TRANSACTIONS (
    ORDER_ID         VARCHAR,
    CUSTOMER_ID      VARCHAR,
    PRODUCT_ID       VARCHAR,
    ORDER_DATE       VARCHAR,
    QUANTITY         VARCHAR,
    DISCOUNT_PCT     VARCHAR,
    STATUS           VARCHAR,
    INGESTION_TS     TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Raw customer data
CREATE OR REPLACE TABLE RAW_CUSTOMERS (
    CUSTOMER_ID      VARCHAR,
    STORE_REGION     VARCHAR,
    INGESTION_TS     TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Raw product catalogue
CREATE OR REPLACE TABLE RAW_PRODUCTS (
    PRODUCT_ID       VARCHAR,
    PRODUCT_NAME     VARCHAR,
    CATEGORY         VARCHAR,
    UNIT_PRICE       VARCHAR,
    INGESTION_TS     TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Raw payment details
CREATE OR REPLACE TABLE RAW_PAYMENTS (
    ORDER_ID         VARCHAR,
    PAYMENT_METHOD   VARCHAR,
    DISCOUNT_PCT     VARCHAR,
    STATUS           VARCHAR,
    INGESTION_TS     TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Raw store data
CREATE OR REPLACE TABLE RAW_STORES (
    STORE_REGION     VARCHAR,
    INGESTION_TS     TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
