-- =============================================================================
-- Part 1: Staging Area
-- Northwind Data Warehouse - ZHAW University Project
-- =============================================================================
-- Data quality issues handled in KNIME before loading:
--   - Negative discounts in OrderDetails (20 rows): ABS() applied
--   - Empty discounts (30 rows): defaulted to 0
--   - Product duplicates (ProductIDs 78-81): only first 77 rows kept
--   - Date formats: Orders MM-DD-YYYY, Employees DD-MM-YYYY → ISO DATE
--   - NULL strings in Region, Fax, ShipRegion, ShippedDate → actual NULL
--   - CSVs encoded as Latin-1
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS staging;

-- ---------------------------------------------------------------------------
-- Categories
-- Source: Categories.csv (delimiter: ;, encoding: Latin-1)
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS staging.categories CASCADE;
CREATE TABLE staging.categories (
    category_id   INTEGER      NOT NULL PRIMARY KEY,
    category_name VARCHAR(50)  NOT NULL,
    description   TEXT
);

-- ---------------------------------------------------------------------------
-- Customers
-- Source: Customers.csv
-- "NULL" strings in Region and Fax converted to actual NULL in KNIME
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS staging.customers CASCADE;
CREATE TABLE staging.customers (
    customer_id    CHAR(5)      NOT NULL PRIMARY KEY,
    company_name   VARCHAR(100) NOT NULL,
    contact_name   VARCHAR(100),
    contact_title  VARCHAR(100),
    address        VARCHAR(200),
    city           VARCHAR(100),
    region         VARCHAR(100),   -- NULLable; "NULL" strings cleaned in KNIME
    postal_code    VARCHAR(20),
    country        VARCHAR(100),
    phone          VARCHAR(30),
    fax            VARCHAR(30)     -- NULLable; "NULL" strings cleaned in KNIME
);

-- ---------------------------------------------------------------------------
-- Products
-- Source: Products.csv
-- Duplicate rows for ProductIDs 78-81 removed in KNIME (keep rows 1-77)
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS staging.products CASCADE;
CREATE TABLE staging.products (
    product_id        INTEGER        NOT NULL PRIMARY KEY,
    product_name      VARCHAR(100)   NOT NULL,
    supplier_id       INTEGER,
    category_id       INTEGER,
    quantity_per_unit VARCHAR(50),
    unit_price        NUMERIC(10, 2),
    units_in_stock    INTEGER,
    units_on_order    INTEGER,
    reorder_level     INTEGER,
    discontinued      BOOLEAN        NOT NULL DEFAULT FALSE
);

-- ---------------------------------------------------------------------------
-- Orders
-- Source: Orders.csv
-- OrderDate / RequiredDate / ShippedDate: MM-DD-YYYY → DATE in KNIME
-- "NULL" strings in ShipRegion and ShippedDate → actual NULL in KNIME
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS staging.orders CASCADE;
CREATE TABLE staging.orders (
    order_id         INTEGER        NOT NULL PRIMARY KEY,
    customer_id      CHAR(5),
    employee_id      INTEGER,
    order_date       DATE,
    required_date    DATE,
    shipped_date     DATE,          -- NULLable
    ship_via         INTEGER,
    freight          NUMERIC(10, 2),
    ship_name        VARCHAR(100),
    ship_address     VARCHAR(200),
    ship_city        VARCHAR(100),
    ship_region      VARCHAR(100),  -- NULLable
    ship_postal_code VARCHAR(20),
    ship_country     VARCHAR(100)
);

-- ---------------------------------------------------------------------------
-- Order Details
-- Source: OrderDetails.csv
-- Discount: ABS() applied for negative values, empty → 0 in KNIME
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS staging.order_details CASCADE;
CREATE TABLE staging.order_details (
    order_id    INTEGER        NOT NULL,
    product_id  INTEGER        NOT NULL,
    unit_price  NUMERIC(10, 2) NOT NULL,
    quantity    INTEGER        NOT NULL,
    discount    NUMERIC(5, 4)  NOT NULL DEFAULT 0,
    PRIMARY KEY (order_id, product_id)
);

-- ---------------------------------------------------------------------------
-- Employees
-- Source: Employees.csv
-- BirthDate / HireDate: DD-MM-YYYY → DATE in KNIME
-- Region, ReportsTo may be NULL
-- Photo column (binary) skipped / stored as text placeholder
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS staging.employees CASCADE;
CREATE TABLE staging.employees (
    employee_id      INTEGER      NOT NULL PRIMARY KEY,
    last_name        VARCHAR(50)  NOT NULL,
    first_name       VARCHAR(50)  NOT NULL,
    title            VARCHAR(100),
    title_of_courtesy VARCHAR(10),
    birth_date       DATE,
    hire_date        DATE,
    address          VARCHAR(200),
    city             VARCHAR(100),
    region           VARCHAR(100), -- NULLable
    postal_code      VARCHAR(20),
    country          VARCHAR(100),
    home_phone       VARCHAR(30),
    extension        VARCHAR(10),
    notes            TEXT,
    reports_to       INTEGER,      -- NULLable (top-level managers)
    photo_path       VARCHAR(255)
);
