# Northwind Mini Data Warehouse

ZHAW Intelligent Information Systems 2 - DWH project built on the Northwind dataset.

## Architecture

```
CSV files -> KNIME (staging ETL) -> staging schema -> SQL transforms -> datamart schema (star schema)
```

**Staging layer:** 6 raw tables mirroring source CSVs, with data quality fixes applied in KNIME.
**Data mart:** Star schema - 4 dimension tables + `fact_order` (grain: one order line item, ~2155 rows).

## Prerequisites

- Docker
- KNIME Analytics Platform
- A PostgreSQL client (psql, DBeaver, etc.)

## Setup

### 1. Start PostgreSQL

```bash
docker compose up -d
```

Default connection:

| Parameter | Value |
|-----------|-------|
| Host | `localhost` |
| Port | `5432` |
| Database | `iis2_dwh_project_1` |
| User | `admin` |
| Password | `123456` |

### 2. Create the staging schema

```bash
psql -h localhost -U admin -d iis2_dwh_project_1 -f sql/01_staging_area.sql
```

### 3. Run the KNIME ETL workflow

Import `staging_etl/` into KNIME (File -> Import KNIME Workflow -> select the directory).

Before executing:
1. Open the **PostgreSQL Connector (#1)** node -> update host/database/user/password if needed.
2. Open each **CSV Reader** node -> update the file path to the absolute path of the corresponding CSV.
3. Execute the workflow -> all 6 staging tables are populated.

### 4. Load the data mart

```bash
psql -h localhost -U admin -d iis2_dwh_project_1 -f sql/02_data_mart.sql
```

This creates the `datamart` schema with 4 dimension tables and `fact_order`, then inserts all data from staging.

### 5. Run the analytical queries

```bash
psql -h localhost -U admin -d iis2_dwh_project_1 -f sql/03_analyses.sql
```

## Project Structure

```
.
├── compose.yml                  # PostgreSQL 17 via Docker
├── sql/
│   ├── 01_staging_area.sql      # staging schema + 6 CREATE TABLE statements
│   ├── 02_data_mart.sql         # star schema (4 dims + fact_order) with INSERTs
│   └── 03_analyses.sql          # 6 analytical queries
├── staging_etl/                 # KNIME workflow directory (importable)
└── docs/
    └── star_schema.mmd          # Mermaid ER diagram of the star schema
```

## Source CSV Files

All files are semicolon-delimited, Latin-1 encoded.

| File | Key columns |
|------|-------------|
| `Categories.csv` | CategoryID; CategoryName; Description |
| `Customers.csv` | CustomerID; CompanyName; ...; Region; Fax |
| `Products.csv` | ProductID; ...; Discontinued |
| `Orders.csv` | OrderID; ...; OrderDate (MM-DD-YYYY); ShipRegion |
| `OrderDetails.csv` | OrderID; ProductID; UnitPrice; Quantity; Discount |
| `Employees.csv` | EmployeeID; ...; BirthDate (DD-MM-YYYY); HireDate (DD-MM-YYYY) |

## Data Quality Issues (handled in KNIME)

| Issue | Source | Fix |
|-------|--------|-----|
| Negative discounts (20 rows) | OrderDetails | `ABS()` via Math Formula node |
| Empty discounts (30 rows) | OrderDetails | Missing Value node -> default `0` |
| Duplicate products (IDs 78-81) | Products | Row Filter: keep ProductID <= 77 |
| Date format MM-DD-YYYY | Orders | String to Date&Time node |
| Date format DD-MM-YYYY | Employees | String to Date&Time node |
| `"NULL"` text strings | Customers, Orders | String Manipulation -> empty string (DB treats as NULL) |

## Star Schema

See [`docs/star_schema.mmd`](docs/star_schema.mmd) for the Mermaid ER diagram (renders on GitHub).

**Dimensions:** `dim_product`, `dim_customer`, `dim_employee`, `dim_time`
**Fact table:** `fact_order` - measures: `unit_price`, `quantity`, `discount`, `total_amount`, `freight`, `ship_via`

## Analytical Queries

| # | Question |
|---|----------|
| 1 | Revenue by category and quarter (with % share of quarter) |
| 2 | Top 10 customers by total revenue |
| 3 | Employee sales performance with manager hierarchy and revenue rank |
| 4 | Shipping method analysis - freight cost as % of revenue |
| 5 | Weekend vs weekday ordering patterns by customer country |
| 6 | In which months do customers order the most by country? |

Query 5 revealed that the Northwind dataset contains no orders placed on weekends, which is consistent with its B2B nature (orders come exclusively from business customers during business days). Since this result is trivial, query 6 was added as an extension to extract more meaningful insights from the time dimension.
