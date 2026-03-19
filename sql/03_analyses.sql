-- =============================================================================
-- Part 3: Analytical Queries
-- All queries run against the datamart schema (star schema)
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Query 1: Revenue by Category and Quarter
-- Shows sales trend across product categories over time
-- ---------------------------------------------------------------------------
SELECT
    dp.category_name,
    dt.year,
    dt.quarter,
    SUM(f.total_amount)                            AS total_revenue,
    ROUND(SUM(f.total_amount) / SUM(SUM(f.total_amount)) OVER (PARTITION BY dt.year, dt.quarter) * 100, 1) AS pct_of_quarter
FROM datamart.fact_orders f
JOIN datamart.dim_product dp ON f.product_key    = dp.product_key
JOIN datamart.dim_time    dt ON f.order_time_key = dt.time_key
GROUP BY dp.category_name, dt.year, dt.quarter
ORDER BY dt.year, dt.quarter, total_revenue DESC;

-- ---------------------------------------------------------------------------
-- Query 2: Top 10 Customers by Revenue
-- Most valuable customers with country and average order value
-- ---------------------------------------------------------------------------
SELECT
    dc.company_name,
    dc.country,
    COUNT(DISTINCT f.order_id)             AS order_count,
    SUM(f.total_amount)                    AS total_revenue,
    ROUND(SUM(f.total_amount) / COUNT(DISTINCT f.order_id), 2) AS avg_order_value
FROM datamart.fact_orders f
JOIN datamart.dim_customer dc ON f.customer_key = dc.customer_key
GROUP BY dc.customer_key, dc.company_name, dc.country
ORDER BY total_revenue DESC
LIMIT 10;

-- ---------------------------------------------------------------------------
-- Query 3: Employee Sales Performance with Manager Hierarchy
-- Revenue ranking including who each employee reports to
-- ---------------------------------------------------------------------------
SELECT
    de.full_name                           AS employee_name,
    de.title,
    COALESCE(de.reports_to_name, '(Top Manager)') AS manager_name,
    COUNT(DISTINCT f.order_id)             AS order_count,
    SUM(f.total_amount)                    AS total_revenue,
    RANK() OVER (ORDER BY SUM(f.total_amount) DESC) AS revenue_rank
FROM datamart.fact_orders f
JOIN datamart.dim_employee de ON f.employee_key = de.employee_key
GROUP BY de.employee_key, de.full_name, de.title, de.reports_to_name
ORDER BY total_revenue DESC;

-- ---------------------------------------------------------------------------
-- Query 4: Shipping Method Analysis
-- Freight cost as percentage of revenue per shipper
-- ---------------------------------------------------------------------------
SELECT
    ds.shipper_name,
    COUNT(DISTINCT f.order_id)             AS order_count,
    SUM(f.total_amount)                    AS total_revenue,
    SUM(f.freight_allocation)              AS total_freight,
    ROUND(SUM(f.freight_allocation) / NULLIF(SUM(f.total_amount), 0) * 100, 2) AS freight_pct_of_revenue,
    ROUND(SUM(f.freight_allocation) / NULLIF(COUNT(DISTINCT f.order_id), 0), 2) AS avg_freight_per_order
FROM datamart.fact_orders f
JOIN datamart.dim_shipping ds ON f.shipping_key = ds.shipping_key
GROUP BY ds.shipping_key, ds.shipper_name
ORDER BY total_revenue DESC;

-- ---------------------------------------------------------------------------
-- Query 5: Weekend vs Weekday Ordering Patterns by Country
-- Compares order behaviour on weekends vs weekdays per customer country
-- ---------------------------------------------------------------------------
SELECT
    dc.country,
    SUM(CASE WHEN dt.is_weekend THEN f.total_amount ELSE 0 END) AS weekend_revenue,
    SUM(CASE WHEN NOT dt.is_weekend THEN f.total_amount ELSE 0 END) AS weekday_revenue,
    COUNT(DISTINCT CASE WHEN dt.is_weekend THEN f.order_id END)     AS weekend_orders,
    COUNT(DISTINCT CASE WHEN NOT dt.is_weekend THEN f.order_id END) AS weekday_orders,
    ROUND(
        COUNT(DISTINCT CASE WHEN dt.is_weekend THEN f.order_id END)::NUMERIC
        / NULLIF(COUNT(DISTINCT f.order_id), 0) * 100, 1
    ) AS weekend_order_pct
FROM datamart.fact_orders f
JOIN datamart.dim_customer dc ON f.customer_key    = dc.customer_key
JOIN datamart.dim_time     dt ON f.order_time_key  = dt.time_key
GROUP BY dc.country
ORDER BY weekend_order_pct DESC, weekday_revenue DESC;
