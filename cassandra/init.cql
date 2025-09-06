-- =====================================================
-- E-COMMERCE REAL-TIME ANALYTICS - CASSANDRA SCHEMA
-- =====================================================
-- This script sets up the complete database schema for 
-- the real-time e-commerce analytics pipeline
-- =====================================================

-- Create keyspace
CREATE KEYSPACE IF NOT EXISTS ecommerce 
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

USE ecommerce;

-- =====================================================
-- 1. RAW TRANSACTIONS TABLE
-- =====================================================
-- Stores all incoming transaction data from Kafka
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id text PRIMARY KEY,
    user_id text,
    user_name text,
    email text,
    age int,
    gender text,
    country text,
    city text,
    product_id text,
    product_name text,
    category text,
    quantity int,
    price double,
    payment_method text,
    transaction_status text,
    timestamp text,
    event_time timestamp,
    transaction_value double,
    processed_at timestamp
);

-- =====================================================
-- 2. TRANSACTION METRICS TABLE
-- =====================================================
-- Stores real-time aggregated transaction metrics per batch
CREATE TABLE IF NOT EXISTS transaction_metrics (
    batch_id bigint PRIMARY KEY,
    batch_time timestamp,
    total_transactions int,
    successful_transactions int,
    failed_transactions int,
    pending_transactions int,
    success_rate_percentage double,
    total_revenue double,
    avg_transaction_value double,
    max_transaction_value double,
    min_transaction_value double
);

-- =====================================================
-- 3. USER METRICS TABLE
-- =====================================================
-- Stores user analytics and demographics per batch
CREATE TABLE IF NOT EXISTS user_metrics (
    batch_id bigint PRIMARY KEY,
    batch_time timestamp,
    unique_users int,
    avg_customer_age double,
    male_customers int,
    female_customers int,
    other_gender_customers int,
    avg_spend_per_user double
);

-- =====================================================
-- 4. CATEGORY METRICS TABLE
-- =====================================================
-- Stores product category performance metrics
CREATE TABLE IF NOT EXISTS category_metrics (
    batch_id bigint,
    category text,
    batch_time timestamp,
    order_count int,
    category_revenue double,
    avg_order_value double,
    unique_products int,
    unique_customers int,
    PRIMARY KEY (batch_id, category)
);

-- =====================================================
-- 5. PAYMENT METHOD METRICS TABLE
-- =====================================================
-- Stores payment method performance and success rates
CREATE TABLE IF NOT EXISTS payment_metrics (
    batch_id bigint,
    payment_method text,
    batch_time timestamp,
    transaction_count int,
    successful_revenue double,
    failed_count int,
    success_rate double,
    avg_transaction_value double,
    PRIMARY KEY (batch_id, payment_method)
);

-- =====================================================
-- 6. GEOGRAPHIC METRICS TABLE
-- =====================================================
-- Stores location-based analytics
CREATE TABLE IF NOT EXISTS geo_metrics (
    batch_id bigint,
    country text,
    city text,
    batch_time timestamp,
    transaction_count int,
    revenue double,
    unique_customers int,
    avg_transaction_value double,
    PRIMARY KEY (batch_id, country, city)
);

-- =====================================================
-- 7. FAILED TRANSACTIONS LOG TABLE
-- =====================================================
-- Stores failed transactions for monitoring and analysis
CREATE TABLE IF NOT EXISTS failed_transactions (
    transaction_id text,
    batch_id bigint,
    user_id text,
    user_name text,
    email text,
    product_name text,
    payment_method text,
    transaction_value double,
    country text,
    city text,
    event_time timestamp,
    logged_at timestamp,
    PRIMARY KEY (batch_id, transaction_id)
);

-- =====================================================
-- 8. HIGH VALUE ALERTS TABLE
-- =====================================================
-- Stores alerts for high-value transactions (>$1000)
CREATE TABLE IF NOT EXISTS high_value_alerts (
    transaction_id text,
    alert_time timestamp,
    user_id text,
    user_name text,
    product_name text,
    transaction_value double,
    payment_method text,
    country text,
    PRIMARY KEY (alert_time, transaction_id)
);

-- =====================================================
-- 9. DAILY AGGREGATES TABLE (Optional - for historical data)
-- =====================================================
-- Stores daily rolled-up metrics for historical reporting
CREATE TABLE IF NOT EXISTS daily_aggregates (
    date text PRIMARY KEY,
    total_transactions int,
    total_revenue double,
    avg_transaction_value double,
    unique_customers int,
    successful_transactions int,
    failed_transactions int,
    success_rate double,
    top_category text,
    top_country text,
    created_at timestamp
);

-- =====================================================
-- 10. REAL-TIME DASHBOARD TABLE (Optional)
-- =====================================================
-- Stores latest metrics for real-time dashboard display
CREATE TABLE IF NOT EXISTS dashboard_metrics (
    metric_type text PRIMARY KEY,
    metric_value double,
    metric_text text,
    last_updated timestamp
);


