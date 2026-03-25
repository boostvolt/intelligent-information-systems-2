/*
CREATE TABLE data_mart.dim_time (
    "Date"             DATE PRIMARY KEY,
    "DayOfWeekNumber"  INTEGER,
    "DayOfWeekName"    VARCHAR(255),
    "Month"            INTEGER,
    "Quarter"          INTEGER,
    "Year"             INTEGER,
    "IsWeekend"        BOOLEAN
);

CREATE TABLE data_mart.dim_customer (
    "CustomerID"   VARCHAR(10) PRIMARY KEY,
    "CompanyName"  VARCHAR(2000),
    "City"         VARCHAR(255),
    "Region"       VARCHAR(255),
    "Country"      VARCHAR(255)
);

CREATE TABLE data_mart.dim_employee (
    "EmployeeID"        INTEGER PRIMARY KEY,
    "Title"             VARCHAR(255),
    "ReportsToFullName" VARCHAR(255),
    "FullName"          VARCHAR(255)
);

CREATE TABLE data_mart.dim_product (
    "ProductID"    INTEGER PRIMARY KEY,
    "ProductName"  VARCHAR(255),
    "UnitPrice"    FLOAT,
    "Discontinued" INTEGER,
    "CategoryName" VARCHAR(255)
);
*/