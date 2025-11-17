# NovaRetail-Kenya-LTD
This repository contains a complete SQL-based analysis of a retail business scenario, focusing on inventory movement, sales trends, product performance, and operational efficiency. The project emphasizes real-world data analytics skills, from query design to metric extraction. 

## Project Overview

This project mimics a real retail data structure with:

## Sales data: Transaction-level detail including sale_date, product_id, unit_price, and quantity_sold

## Inventory data: product_id, stock_on_hand, reorder_level, cost_price

## Purchase orders: Insights into procurement through order_date, expected_delivery, delivery_delay, and quantity_order


## The goal is to answer business-critical questions like:

Which products drive revenue and profit?

How is stock moving vs. demand?

What’s the relationship between delivery delays and stock-outs?

How can the business forecast inventory more effectively?


## Tech Stack

Microsoft SQL Server (T-SQL)

SQL Server Management Studio (SSMS)

Data model designed manually (no auto-generators)

## Key Queries & Insights

Top 10 Best-selling Products

Stock Aging Analysis

Delivery Performance Metrics

Monthly Revenue Trend

Inventory Turnover Ratio

You’ll find all queries organized by topic in this repository.

 
 NovaRetail Kenya Ltd. — Synthetic Business + Supply Chain Dataset
 Period: 2020-01-01 to 2024-12-31
 Currency: USD
 ~17,000 rows total
********************************************************************/

USE NovaRetailDB;

-- 1) Drop existing tables if rerunning (safe for development)
IF OBJECT_ID('dbo.Budget_vs_Actual','U') IS NOT NULL DROP TABLE dbo.Budget_vs_Actual;
IF OBJECT_ID('dbo.Inventory','U') IS NOT NULL DROP TABLE dbo.Inventory;
IF OBJECT_ID('dbo.Expenses','U') IS NOT NULL DROP TABLE dbo.Expenses;
IF OBJECT_ID('dbo.Sales','U') IS NOT NULL DROP TABLE dbo.Sales;
IF OBJECT_ID('dbo.Purchase_Orders','U') IS NOT NULL DROP TABLE dbo.Purchase_Orders;
IF OBJECT_ID('dbo.Suppliers','U') IS NOT NULL DROP TABLE dbo.Suppliers;
IF OBJECT_ID('dbo.Products','U') IS NOT NULL DROP TABLE dbo.Products;

-- 2) Create tables
CREATE TABLE dbo.Products (
    product_id INT IDENTITY(1,1) PRIMARY KEY,
    product_code VARCHAR(20) NOT NULL UNIQUE,
    product_name VARCHAR(150) NOT NULL,
    category VARCHAR(50) NOT NULL,
    unit_cost DECIMAL(10,2) NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    created_at DATE NOT NULL DEFAULT(CONVERT(date,GETDATE()))
);

CREATE TABLE dbo.Suppliers (
    supplier_id INT IDENTITY(1,1) PRIMARY KEY,
    supplier_name VARCHAR(150) NOT NULL,
    country VARCHAR(50) NOT NULL,
    region VARCHAR(50) NULL,
    rating TINYINT NOT NULL, -- 1..5
    lead_time_days TINYINT NOT NULL, -- typical lead time
    is_local BIT NOT NULL
);

CREATE TABLE dbo.Purchase_Orders (
    order_id INT IDENTITY(1,1) PRIMARY KEY,
    supplier_id INT NOT NULL,
    product_id INT NOT NULL,
    order_date DATE NOT NULL,
    quantity_ordered INT NOT NULL,
    unit_cost DECIMAL(10,2) NOT NULL,
    total_cost AS (quantity_ordered * unit_cost) PERSISTED,
    expected_delivery_date DATE NOT NULL,
    actual_delivery_date DATE NULL,
    delivery_delay_days AS (CASE WHEN actual_delivery_date IS NULL THEN NULL ELSE DATEDIFF(day, expected_delivery_date, actual_delivery_date) END) PERSISTED,
    FOREIGN KEY (supplier_id) REFERENCES dbo.Suppliers(supplier_id),
    FOREIGN KEY (product_id) REFERENCES dbo.Products(product_id)
);

CREATE TABLE dbo.Sales (
    sale_id INT IDENTITY(1,1) PRIMARY KEY,
    product_id INT NOT NULL,
    region VARCHAR(50) NOT NULL,
    sale_date DATE NOT NULL,
    quantity_sold INT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    revenue AS (quantity_sold * unit_price) PERSISTED,
    FOREIGN KEY (product_id) REFERENCES dbo.Products(product_id)
);

CREATE TABLE dbo.Expenses (
    expense_id INT IDENTITY(1,1) PRIMARY KEY,
    expense_date DATE NOT NULL,
    department VARCHAR(100) NOT NULL,
    category VARCHAR(100) NOT NULL,
    amount DECIMAL(12,2) NOT NULL,
    notes VARCHAR(250) NULL
);

CREATE TABLE dbo.Inventory (
    inventory_id INT IDENTITY(1,1) PRIMARY KEY,
    product_id INT NOT NULL,
    snapshot_date DATE NOT NULL,
    stock_on_hand INT NOT NULL,
    reorder_level INT NOT NULL,
    unit_cost DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (product_id) REFERENCES dbo.Products(product_id)
);

CREATE TABLE dbo.Budget_vs_Actual (
    id INT IDENTITY(1,1) PRIMARY KEY,
    department VARCHAR(100) NOT NULL,
    month_start DATE NOT NULL, -- first day of month
    budget_amount DECIMAL(12,2) NOT NULL,
    actual_amount DECIMAL(12,2) NOT NULL
);

-- 3) Helper: Create a tally source large enough for generating rows
-- We'll use system views to produce many rows
-- No persistent table required

-- 4) Seed products (120 items across 5 categories)
DECLARE @Categories TABLE (cat VARCHAR(50));
INSERT INTO @Categories(cat) VALUES ('Electronics'),('Home & Kitchen'),('Clothing & Accessories'),('Groceries'),('Health & Beauty');

;WITH nums AS (
    SELECT TOP (120) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
    FROM sys.all_objects a CROSS JOIN sys.all_objects b
)
INSERT INTO dbo.Products(product_code, product_name, category, unit_cost, unit_price)
SELECT
    'NR-' + RIGHT('0000' + CAST(n AS VARCHAR(10)),4) AS product_code,
    CONCAT(
        CASE WHEN (n % 5) = 1 THEN 'Smart ' WHEN (n % 5) = 2 THEN 'Multi ' WHEN (n % 5) = 3 THEN 'Classic ' 
		WHEN (n % 5) = 4 THEN 'Premium ' ELSE 'Eco ' END,
        SUBSTRING(md5hash,1,6)
    ) AS product_name,
    cat = (SELECT cat FROM (SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) rn, cat FROM @Categories) c WHERE ((n-1) % 5)+1 = c.rn),
    unit_cost = CAST(ROUND( (10 + ABS(CHECKSUM(NEWID())) % 490) + (ABS(CHECKSUM(NEWID())) % 100)/100.0, 2) AS DECIMAL(10,2)),
    unit_price = CAST(ROUND( ( (10 + ABS(CHECKSUM(NEWID())) % 490) * (1 + (0.15 + (ABS(CHECKSUM(NEWID())) % 50)/100.0)) ), 2) AS DECIMAL(10,2))
FROM (
    SELECT n, CONVERT(VARCHAR(32), HASHBYTES('MD5', CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS VARCHAR(20))), 2) AS md5hash
    FROM nums
) t;
-- note: HASHBYTES returns varbinary; converted to hex string; ok for uniqueness


-- 5) Seed suppliers (40)
DECLARE @SupplierCountries TABLE (cty VARCHAR(50), region VARCHAR(50));
INSERT INTO @SupplierCountries(cty, region) VALUES
('Kenya','East Africa'),
('Uganda','East Africa'),
('Tanzania','East Africa'),
('South Africa','Southern Africa'),
('China','Asia'),
('India','Asia'),
('United Arab Emirates','Middle East');

;WITH s(n) AS (
    SELECT TOP (40) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) FROM sys.all_objects a CROSS JOIN sys.all_objects b
)
INSERT INTO dbo.Suppliers (supplier_name, country, region, rating, lead_time_days, is_local)
SELECT
    'Supplier ' + RIGHT('00' + CAST(n AS VARCHAR(3)),2),
    country = (SELECT cty FROM (SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) rn, cty, 
	region FROM @SupplierCountries) c WHERE ((n-1) % (SELECT COUNT(*) FROM @SupplierCountries))+1 = c.rn),
    region = (SELECT region FROM (SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) rn, cty, 
	region FROM @SupplierCountries) c WHERE ((n-1) % (SELECT COUNT(*) FROM @SupplierCountries))+1 = c.rn),
    rating = CAST(1 + ABS(CHECKSUM(NEWID())) % 5 AS TINYINT),
    lead_time_days = CAST(3 + ABS(CHECKSUM(NEWID())) % 20 AS TINYINT),
    is_local = CASE WHEN ((n-1) % 7) = 0 THEN 1 WHEN ((n-1) % 7) = 1 THEN 1 WHEN ((n-1) % 7) = 2 THEN 1 ELSE 0 END
FROM s;


-- Quick check: product and supplier counts
SELECT COUNT(*) FROM dbo.Products; SELECT COUNT(*) FROM dbo.Suppliers;

-- 6) Generate Purchase_Orders (~4,000 rows)
-- We'll pick random suppliers and products; create expected and actual delivery dates with occasional delays
;WITH tally AS (
    SELECT TOP (4000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
    FROM sys.all_objects a CROSS JOIN sys.all_objects b
)
INSERT INTO dbo.Purchase_Orders (supplier_id, product_id, order_date, quantity_ordered, unit_cost, expected_delivery_date, actual_delivery_date)
SELECT
    supplier_id = 1 + ABS(CHECKSUM(NEWID())) % (SELECT COUNT(*) FROM dbo.Suppliers),
    product_id = 1 + ABS(CHECKSUM(NEWID())) % (SELECT COUNT(*) FROM dbo.Products),
    order_date = DATEADD(DAY, ABS(CHECKSUM(NEWID())) % DATEDIFF(DAY, '2020-01-01', '2024-12-31'), '2020-01-01'),
    quantity_ordered = 5 + ABS(CHECKSUM(NEWID())) % 500, -- quantities between 5 and ~504
    unit_cost = CAST(ROUND(
        (SELECT unit_cost FROM dbo.Products p WHERE p.product_id = (1 + ABS(CHECKSUM(NEWID())) % (SELECT COUNT(*) FROM dbo.Products))) 
        * (0.95 + (ABS(CHECKSUM(NEWID())) % 30)/100.0)
    ,2) AS DECIMAL(10,2)),
    expected_delivery_date = DATEADD(DAY, (2 + ABS(CHECKSUM(NEWID())) % 45), DATEADD(DAY, ABS(CHECKSUM(NEWID())) % DATEDIFF(DAY, '2020-01-01', 
	'2024-12-31'), '2020-01-01')),
    actual_delivery_date = CASE WHEN (ABS(CHECKSUM(NEWID())) % 100) < 85 
        THEN DATEADD(DAY, (2 + ABS(CHECKSUM(NEWID())) % 45), DATEADD(DAY, ABS(CHECKSUM(NEWID())) % DATEDIFF(DAY, '2020-01-01', '2024-12-31'),
		'2020-01-01')) -- on time ~85%
        ELSE DATEADD(DAY, (2 + ABS(CHECKSUM(NEWID())) % 45 + (1 + ABS(CHECKSUM(NEWID())) % 12)), DATEADD(DAY, ABS(CHECKSUM(NEWID())) 
		% DATEDIFF(DAY, '2020-01-01', '2024-12-31'), '2020-01-01')) -- delayed
    END
FROM tally;

-- 7) Generate Sales (~10,000 rows)
-- Regions: Nairobi, Mombasa, Kisumu, Eldoret, Nakuru
;WITH s(num) AS (
    SELECT TOP (10000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
    FROM sys.all_objects a CROSS JOIN sys.all_objects b CROSS JOIN sys.all_objects c
)
INSERT INTO dbo.Sales (product_id, region, sale_date, quantity_sold, unit_price)
SELECT
    product_id = 1 + ABS(CHECKSUM(NEWID())) % (SELECT COUNT(*) FROM dbo.Products),
    region = CASE (ABS(CHECKSUM(NEWID())) % 5)
                WHEN 0 THEN 'Nairobi'
                WHEN 1 THEN 'Mombasa'
                WHEN 2 THEN 'Kisumu'
                WHEN 3 THEN 'Eldoret'
                ELSE 'Nakuru'
             END,
    sale_date = DATEADD(DAY, ABS(CHECKSUM(NEWID())) % DATEDIFF(DAY, '2020-01-01', '2024-12-31'), '2020-01-01'),
    quantity_sold = CASE WHEN ABS(CHECKSUM(NEWID())) % 100 < 70 THEN 1 + ABS(CHECKSUM(NEWID())) % 5 ELSE 6 + ABS(CHECKSUM(NEWID())) % 40 END,
    unit_price = (SELECT unit_price FROM dbo.Products p WHERE p.product_id = (1 + ABS(CHECKSUM(NEWID())) % (SELECT COUNT(*) FROM dbo.Products)))
FROM s;

-- 8) Generate Expenses (~800 rows) - fixed for category column
ALTER TABLE dbo.Expenses
ADD expense_type NVARCHAR(50);

;WITH e(num) AS (
    SELECT TOP (800) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
    FROM sys.all_objects a CROSS JOIN sys.all_objects b
)
INSERT INTO dbo.Expenses (expense_date, department, category, expense_type, amount)
SELECT
    expense_date = DATEADD(DAY, ABS(CHECKSUM(NEWID())) % DATEDIFF(DAY, '2020-01-01', '2024-12-31'), '2020-01-01'),
    department = CASE (ABS(CHECKSUM(NEWID())) % 5)
                    WHEN 0 THEN 'Marketing'
                    WHEN 1 THEN 'Logistics'
                    WHEN 2 THEN 'Human Resources'
                    WHEN 3 THEN 'IT'
                    ELSE 'Administration'
                 END,
    category = CASE (ABS(CHECKSUM(NEWID())) % 3)
                    WHEN 0 THEN 'Operational'
                    WHEN 1 THEN 'Personnel'
                    ELSE 'Overhead'
               END,
    expense_type = CASE (ABS(CHECKSUM(NEWID())) % 4)
                      WHEN 0 THEN 'Supplies'
                      WHEN 1 THEN 'Salaries'
                      WHEN 2 THEN 'Maintenance'
                      ELSE 'Utilities'
                   END,
    amount = CAST(100 + (ABS(CHECKSUM(NEWID())) % 5000) AS DECIMAL(10,2))
FROM e;


-- 9) Inventory snapshots (~500)
;WITH i AS (
    SELECT TOP (500) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n FROM sys.all_objects a CROSS JOIN sys.all_objects b
)
INSERT INTO dbo.Inventory (product_id, snapshot_date, stock_on_hand, reorder_level, unit_cost)
SELECT
    product_id = 1 + ABS(CHECKSUM(NEWID())) % (SELECT COUNT(*) FROM dbo.Products),
    snapshot_date = DATEADD(MONTH, - (ABS(CHECKSUM(NEWID())) % 60), CAST('2024-12-01' AS DATE)),
    stock_on_hand = 0 + ABS(CHECKSUM(NEWID())) % 2000,
    reorder_level = 5 + ABS(CHECKSUM(NEWID())) % 200,
    unit_cost = (SELECT unit_cost FROM dbo.Products p WHERE p.product_id = (1 + ABS(CHECKSUM(NEWID())) % (SELECT COUNT(*) FROM dbo.Products)))
FROM i;
 
EXEC sp_help 'dbo.Budget_vs_Actual';


-- 10) Budget_vs_Actual (~360 rows: 12 months * ~6 depts * 5 years ~ 360)
DECLARE @Departments TABLE (department NVARCHAR(50));
INSERT INTO @Departments (department)
VALUES ('Marketing'), ('Logistics'), ('Human Resources'), ('IT'), ('Administration');

;WITH months AS (
    SELECT DATEFROMPARTS(y, m, 1) AS month_start
    FROM (VALUES (2020),(2021),(2022),(2023),(2024)) AS years(y)
    CROSS APPLY (VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12)) AS months(m)
)
INSERT INTO dbo.Budget_vs_Actual (department, month_start, budget_amount, actual_amount)
SELECT
    d.department,
    m.month_start,
    budget_amount = CAST(2000 + (ABS(CHECKSUM(NEWID())) % 8000) AS DECIMAL(10,2)),
    actual_amount = CAST(2000 + (ABS(CHECKSUM(NEWID())) % 8000) AS DECIMAL(10,2))
FROM months m
CROSS JOIN @Departments d;


-- 11) Create some helpful indexes for analysis queries
CREATE INDEX ix_PurchaseOrders_supplier_date ON dbo.Purchase_Orders(supplier_id, order_date);
CREATE INDEX ix_Sales_date_product ON dbo.Sales(sale_date, product_id);
CREATE INDEX ix_Inventory_product_date ON dbo.Inventory(product_id, snapshot_date);
CREATE INDEX ix_Expenses_date_dept ON dbo.Expenses(expense_date, department);


-- 12) Add example computed KPIs table (optional) - quick sample aggregated view for executives
IF OBJECT_ID('dbo.vw_ExecSummary','V') IS NOT NULL DROP VIEW dbo.vw_ExecSummary;
CREATE VIEW dbo.vw_ExecSummary AS
    SELECT
    YEAR(s.sale_date) AS yr,
    s.region,
    p.category,
    COUNT(DISTINCT s.product_id) AS sku_count,
    SUM(s.quantity_sold) AS total_units_sold,
    SUM(s.revenue) AS total_revenue,
    SUM(po.total_cost) AS procurement_spend,
    COALESCE(SUM(e.amount),0) AS total_expenses
FROM dbo.Sales s
JOIN dbo.Products p ON s.product_id = p.product_id
LEFT JOIN dbo.Purchase_Orders po ON po.product_id = s.product_id AND YEAR(po.order_date) = YEAR(s.sale_date)
LEFT JOIN dbo.Expenses e ON YEAR(e.expense_date) = YEAR(s.sale_date)
GROUP BY YEAR(s.sale_date), s.region, p.category;


-- 13) Final counts & quick sanity checks
PRINT 'Row counts (Products, Suppliers, Purchase_Orders, Sales, Expenses, Inventory, Budget_vs_Actual):';
SELECT 
    (SELECT COUNT(*) FROM dbo.Products) AS Products,
    (SELECT COUNT(*) FROM dbo.Suppliers) AS Suppliers,
    (SELECT COUNT(*) FROM dbo.Purchase_Orders) AS Purchase_Orders,
    (SELECT COUNT(*) FROM dbo.Sales) AS Sales,
    (SELECT COUNT(*) FROM dbo.Expenses) AS Expenses,
    (SELECT COUNT(*) FROM dbo.Inventory) AS Inventory,
    (SELECT COUNT(*) FROM dbo.Budget_vs_Actual) AS Budget_vs_Actual;

-- Forecasting hint: export Sales aggregated by month into Power BI and use built-in forecasting to predict 2025 revenue.

PRINT 'Dataset generation complete.';

SELECT TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE';

SELECT TOP 10 * FROM dbo.Sales;


-- Top 10 SKUs by revenue:
SELECT TOP (10) p.product_id, p.product_name, SUM(s.revenue) AS revenue 
FROM dbo.Sales s 
 JOIN dbo.Products p ON s.product_id = p.product_id 
GROUP BY p.product_id, p.product_name 
ORDER BY revenue DESC;

-- Suppliers with highest average delivery delay:
SELECT TOP (20) sup.supplier_id, sup.supplier_name, AVG(p.delivery_delay_days) AS avg_delay 
FROM dbo.Purchase_Orders p 
 JOIN dbo.Suppliers sup ON p.supplier_id = sup.supplier_id 
WHERE p.delivery_delay_days IS NOT NULL 
GROUP BY sup.supplier_id, sup.supplier_name 
ORDER BY avg_delay DESC;

-- Monthly revenue vs expenses:
SELECT YEAR(s.sale_date) yr, MONTH(s.sale_date) m, SUM(s.revenue) revenue, 
       (SELECT SUM(amount) FROM dbo.Expenses e 
WHERE YEAR(e.expense_date)=YEAR(s.sale_date) AND MONTH(e.expense_date)=MONTH(s.sale_date)) expenses 
FROM dbo.Sales s 
GROUP BY YEAR(s.sale_date), MONTH(s.sale_date) 
ORDER BY yr, m;

-- Total Revenue by Region 
SELECT region, SUM(quantity_sold * unit_price) AS total_revenue
FROM dbo.Sales
GROUP BY region
ORDER BY total_revenue DESC;

-- Top 5 performing suppliers by order count  
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'Purchase_Orders';

SELECT s.supplier_name, COUNT(po.order_id) AS total_orders
FROM dbo.Suppliers s
 JOIN dbo.Purchase_Orders po ON s.supplier_id = po.supplier_id
GROUP BY s.supplier_name
ORDER BY total_orders DESC; 

-- Revenue and Sales Insights 

-- Total revenue by year 
SELECT 
    YEAR(sale_date) AS year,
    SUM(quantity_sold * unit_price) AS total_revenue
FROM dbo.Sales
GROUP BY YEAR(sale_date)
ORDER BY year;

-- Revenue by Region and Year
SELECT 
    region,
    YEAR(sale_date) AS year,
    SUM(quantity_sold * unit_price) AS total_revenue
FROM dbo.Sales
GROUP BY region, YEAR(sale_date)
ORDER BY region, year;
 
-- Top 10 products by Revenue 
SELECT 
    p.product_name,
    SUM(s.quantity_sold * s.unit_price) AS total_revenue
FROM dbo.Sales s
JOIN dbo.Products p ON s.product_id = p.product_id
GROUP BY p.product_name
ORDER BY total_revenue DESC
OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY;

-- Monthly Sales Trend (For Forecasting)
SELECT 
    FORMAT(sale_date, 'yyyy-MM') AS month,
    SUM(quantity_sold * unit_price) AS total_revenue
FROM dbo.Sales
GROUP BY FORMAT(sale_date, 'yyyy-MM')
ORDER BY month;

-- Supplier and Procurement Analysis
 
-- Total spend by Supplier
SELECT 
    s.supplier_name,
    SUM(po.quantity_ordered * po.unit_cost) AS total_spend
FROM dbo.Purchase_Orders po
 JOIN dbo.Suppliers s ON po.supplier_id = s.supplier_id
GROUP BY s.supplier_name
ORDER BY total_spend DESC;

-- Average Delivery Size per Supplier
SELECT 
    s.supplier_name,
    AVG(po.quantity_ordered) AS avg_order_quantity
FROM dbo.Purchase_Orders po
 JOIN dbo.Suppliers s ON po.supplier_id = s.supplier_id
GROUP BY s.supplier_name
ORDER BY avg_order_quantity DESC;

-- Expense Insights

--Total Expenses by Department
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'Expenses';

SELECT 
    department,
    SUM(amount) AS total_expense
FROM dbo.Expenses
GROUP BY department
ORDER BY total_expense DESC;

-- Expense Category Breakdown (Top 5) Departments consuming the largest share of company's spending
SELECT 
    category,
    SUM(amount) AS total_spent
FROM dbo.Expenses
GROUP BY category
ORDER BY total_spent DESC
OFFSET 0 ROWS FETCH NEXT 5 ROWS ONLY;

-- Monthly Expense Trend 
SELECT 
    FORMAT(expense_date, 'yyyy-MM') AS month,
    SUM(amount) AS monthly_expense
FROM dbo.Expenses
GROUP BY FORMAT(expense_date, 'yyyy-MM')
ORDER BY month;

-- Department vs Category Matrix (Cross-Analysis)
SELECT 
    department,
    category,
    SUM(amount) AS total_expense
FROM dbo.Expenses
GROUP BY department, category
ORDER BY department, total_expense DESC;

-- Phase 2.4 — Profitability & Performance

-- Combine Revenue and Expenses
SELECT 
    YEAR(s.sale_date) AS year,
    SUM(s.quantity_sold * s.unit_price) AS total_revenue,
    (SELECT SUM(e.amount) FROM dbo.Expenses e WHERE YEAR(e.expense_date) = YEAR(s.sale_date)) AS total_expenses,
    SUM(s.quantity_sold * s.unit_price) 
      - (SELECT SUM(e.amount) FROM dbo.Expenses e WHERE YEAR(e.expense_date) = YEAR(s.sale_date)) AS profit
FROM dbo.Sales s
GROUP BY YEAR(s.sale_date)
ORDER BY year;

-- Budget vs Actual
-- Which depts overspend vs underspend 
SELECT 
    department,
    YEAR(month_start) AS year,
    SUM(budget_amount) AS budget_total,
    SUM(actual_amount) AS actual_total,
    SUM(actual_amount - budget_amount) AS variance,
    ROUND(SUM(actual_amount - budget_amount) / SUM(budget_amount) * 100, 2) AS variance_percent
FROM dbo.Budget_vs_Actual
GROUP BY department, YEAR(month_start)
ORDER BY department, year;

-- Create Executive Summary View 
CREATE VIEW dbo.vw_FinancialPerformance AS
SELECT 
    YEAR(s.sale_date) AS year,
    SUM(s.quantity_sold * s.unit_price) AS total_revenue,
    (SELECT SUM(e.amount) FROM dbo.Expenses e WHERE YEAR(e.expense_date) = YEAR(s.sale_date)) AS total_expenses,
    SUM(s.quantity_sold * s.unit_price) 
      - (SELECT SUM(e.amount) FROM dbo.Expenses e WHERE YEAR(e.expense_date) = YEAR(s.sale_date)) AS profit
FROM dbo.Sales s
GROUP BY YEAR(s.sale_date);

## Licensed under MIT — free to use for training, learning and portfolio projects.
