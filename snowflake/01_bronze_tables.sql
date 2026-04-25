-- =============================================================================
-- Retail Sales Pipeline - Bronze Layer
-- =============================================================================
-- Purpose  : Raw landing table for retail transaction data
--            Data is loaded exactly as received — no transformations applied
--            This preserves the original source data for reprocessing if needed
-- Layer    : Bronze (Raw)
-- Author   : Sowmya D
-- =============================================================================

-- Create a dedicated database and schema for the pipeline
CREATE DATABASE IF NOT EXISTS RETAIL_DW;
USE DATABASE RETAIL_DW;

CREATE SCHEMA IF NOT EXISTS BRONZE;
USE SCHEMA BRONZE;

-- =============================================================================
-- RAW TRANSACTIONS TABLE
-- Stores data exactly as it arrives from source systems
-- All columns are VARCHAR to handle dirty data without rejection
-- Data type enforcement happens in the Silver layer
-- =============================================================================
CREATE TABLE IF NOT EXISTS RAW_TRANSACTIONS (
    ORDER_ID        VARCHAR(20),
    CUSTOMER_ID     VARCHAR(20),
    PRODUCT_ID      VARCHAR(20),
    PRODUCT_NAME    VARCHAR(100),
    CATEGORY        VARCHAR(50),
    QUANTITY        VARCHAR(10),    -- VARCHAR not INT — source sends negatives and nulls
    UNIT_PRICE      VARCHAR(20),    -- VARCHAR not FLOAT — source sends nulls and zeros
    DISCOUNT_PCT    VARCHAR(10),    -- VARCHAR not FLOAT — source sends nulls
    ORDER_DATE      VARCHAR(20),    -- VARCHAR not DATE — source sends mixed formats
    STORE_REGION    VARCHAR(50),
    PAYMENT_METHOD  VARCHAR(50),
    STATUS          VARCHAR(20),
    -- Audit columns added at load time
    INGESTION_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    SOURCE_FILE         VARCHAR(500)
);

-- =============================================================================
-- LOAD DATA FROM ADLS INTO BRONZE TABLE
-- In production this is triggered by ADF pipeline after file lands in ADLS
-- =============================================================================
COPY INTO BRONZE.RAW_TRANSACTIONS (
    ORDER_ID, CUSTOMER_ID, PRODUCT_ID, PRODUCT_NAME,
    CATEGORY, QUANTITY, UNIT_PRICE, DISCOUNT_PCT,
    ORDER_DATE, STORE_REGION, PAYMENT_METHOD, STATUS,
    SOURCE_FILE
)
FROM (
    SELECT
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
        METADATA$FILENAME
    FROM @RETAIL_DW.BRONZE.RETAIL_STAGE
)
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('', 'NULL', 'null')
)
ON_ERROR = 'CONTINUE';  -- Log errors but don't fail the entire load

-- Quick row count check after load
SELECT COUNT(*) AS total_rows_loaded FROM BRONZE.RAW_TRANSACTIONS;
