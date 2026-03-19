-- =============================================================================
-- Part 2: Data Mart (Star Schema)
-- Grain: one order line item (OrderDetail level)
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS datamart;

-- =============================================================================
-- DIMENSIONS
-- =============================================================================

-- ---------------------------------------------------------------------------
-- dim_product: products enriched with category name
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS datamart.dim_product CASCADE;
CREATE TABLE datamart.dim_product (
    product_key   SERIAL       PRIMARY KEY,
    product_id    INTEGER      NOT NULL,
    product_name  VARCHAR(100) NOT NULL,
    category_name VARCHAR(50),
    unit_price    NUMERIC(10, 2),
    discontinued  BOOLEAN
);

INSERT INTO datamart.dim_product (product_id, product_name, category_name, unit_price, discontinued)
SELECT
    p.product_id,
    p.product_name,
    c.category_name,
    p.unit_price,
    p.discontinued
FROM staging.products p
LEFT JOIN staging.categories c ON p.category_id = c.category_id;

-- ---------------------------------------------------------------------------
-- dim_customer
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS datamart.dim_customer CASCADE;
CREATE TABLE datamart.dim_customer (
    customer_key  SERIAL       PRIMARY KEY,
    customer_id   CHAR(5)      NOT NULL,
    company_name  VARCHAR(100) NOT NULL,
    city          VARCHAR(100),
    region        VARCHAR(100),
    country       VARCHAR(100)
);

INSERT INTO datamart.dim_customer (customer_id, company_name, city, region, country)
SELECT
    customer_id,
    company_name,
    city,
    region,
    country
FROM staging.customers;

-- ---------------------------------------------------------------------------
-- dim_employee: includes manager name via self-join
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS datamart.dim_employee CASCADE;
CREATE TABLE datamart.dim_employee (
    employee_key    SERIAL       PRIMARY KEY,
    employee_id     INTEGER      NOT NULL,
    full_name       VARCHAR(100) NOT NULL,
    title           VARCHAR(100),
    reports_to_name VARCHAR(100)  -- NULL for top-level managers
);

INSERT INTO datamart.dim_employee (employee_id, full_name, title, reports_to_name)
SELECT
    e.employee_id,
    e.first_name || ' ' || e.last_name                         AS full_name,
    e.title,
    m.first_name || ' ' || m.last_name                         AS reports_to_name
FROM staging.employees e
LEFT JOIN staging.employees m ON e.reports_to = m.employee_id;

-- ---------------------------------------------------------------------------
-- dim_time: one row per distinct order date
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS datamart.dim_time CASCADE;
CREATE TABLE datamart.dim_time (
    time_key   SERIAL  PRIMARY KEY,
    date       DATE    NOT NULL UNIQUE,
    day_name   VARCHAR(10),   -- e.g. 'Monday'
    month      INTEGER,       -- 1-12
    quarter    INTEGER,       -- 1-4
    year       INTEGER,
    is_weekend BOOLEAN        -- TRUE for Saturday / Sunday
);

INSERT INTO datamart.dim_time (date, day_name, month, quarter, year, is_weekend)
SELECT DISTINCT
    order_date,
    TO_CHAR(order_date, 'Day'),
    EXTRACT(MONTH  FROM order_date)::INTEGER,
    EXTRACT(QUARTER FROM order_date)::INTEGER,
    EXTRACT(YEAR   FROM order_date)::INTEGER,
    EXTRACT(DOW    FROM order_date) IN (0, 6)  -- 0=Sunday, 6=Saturday
FROM staging.orders
WHERE order_date IS NOT NULL
ORDER BY order_date;

-- ---------------------------------------------------------------------------
-- dim_shipping: the three Northwind shippers (hardcoded)
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS datamart.dim_shipping CASCADE;
CREATE TABLE datamart.dim_shipping (
    shipping_key SERIAL      PRIMARY KEY,
    ship_via_id  INTEGER     NOT NULL UNIQUE,
    shipper_name VARCHAR(100) NOT NULL
);

INSERT INTO datamart.dim_shipping (ship_via_id, shipper_name)
VALUES
    (1, 'Speedy Express'),
    (2, 'United Package'),
    (3, 'Federal Shipping');

-- =============================================================================
-- FACT TABLE
-- Grain: one order line item
-- =============================================================================

DROP TABLE IF EXISTS datamart.fact_orders CASCADE;
CREATE TABLE datamart.fact_orders (
    fact_id           SERIAL         PRIMARY KEY,
    -- surrogate keys
    product_key       INTEGER        NOT NULL REFERENCES datamart.dim_product(product_key),
    customer_key      INTEGER        NOT NULL REFERENCES datamart.dim_customer(customer_key),
    employee_key      INTEGER        NOT NULL REFERENCES datamart.dim_employee(employee_key),
    order_time_key    INTEGER        NOT NULL REFERENCES datamart.dim_time(time_key),
    shipping_key      INTEGER        NOT NULL REFERENCES datamart.dim_shipping(shipping_key),
    -- degenerate dimension
    order_id          INTEGER        NOT NULL,
    -- measures
    unit_price        NUMERIC(10, 2) NOT NULL,
    quantity          INTEGER        NOT NULL,
    discount          NUMERIC(5, 4)  NOT NULL DEFAULT 0,
    total_amount      NUMERIC(12, 2) NOT NULL,  -- unit_price * quantity * (1 - discount)
    freight_allocation NUMERIC(10, 2)            -- order freight / number of line items
);

-- Populate fact table
-- freight_allocation = order freight divided equally among line items
WITH line_counts AS (
    SELECT order_id, COUNT(*) AS line_count
    FROM staging.order_details
    GROUP BY order_id
)
INSERT INTO datamart.fact_orders (
    product_key, customer_key, employee_key, order_time_key, shipping_key,
    order_id,
    unit_price, quantity, discount, total_amount, freight_allocation
)
SELECT
    dp.product_key,
    dc.customer_key,
    de.employee_key,
    dt.time_key,
    ds.shipping_key,
    o.order_id,
    od.unit_price,
    od.quantity,
    od.discount,
    ROUND(od.unit_price * od.quantity * (1 - od.discount), 2) AS total_amount,
    ROUND(o.freight / lc.line_count, 2)                        AS freight_allocation
FROM staging.order_details od
JOIN staging.orders   o  ON od.order_id   = o.order_id
JOIN staging.products p  ON od.product_id = p.product_id
JOIN staging.customers cu ON o.customer_id = cu.customer_id
JOIN staging.employees e  ON o.employee_id = e.employee_id
JOIN line_counts       lc ON o.order_id    = lc.order_id
JOIN datamart.dim_product  dp ON od.product_id = dp.product_id
JOIN datamart.dim_customer dc ON cu.customer_id = dc.customer_id
JOIN datamart.dim_employee de ON e.employee_id  = de.employee_id
JOIN datamart.dim_time     dt ON o.order_date   = dt.date
JOIN datamart.dim_shipping ds ON o.ship_via     = ds.ship_via_id;
