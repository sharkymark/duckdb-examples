# DuckDB Example Dataset Harvesting

Tools for loading and analyzing example datasets into DuckDB.

Dataset steps included for:
- Northwind
- Online Retail

## Code

- `create_northwind_tables.py`: Python script to download and create Northwind tables in DuckDB
- `clean.ipynb`: Jupyter notebook for data exploration and cleaning of CSV files

## Data Source

### Northwind Dataset

The Northwind dataset is sourced from:
https://github.com/neo4j-contrib/northwind-neo4j/tree/master/data/

This is a standard demo database containing business data like customers, orders, products, shippers and suppliers. Originally created by Microsoft to demonstrate MS Access and SQL Server features.

This repository uses camelCase for column names.

In theory, any GitHub repository can be used as a data source. The script `create_northwind_tables.py` downloads the CSV files from the GitHub repository and creates DuckDB tables.

### Online Retail Dataset
The Online Retail dataset is sourced from:
https://raw.githubusercontent.com/databricks/Spark-The-Definitive-Guide/refs/heads/master/data/retail-data/all/online-retail-dataset.csv

This dataset contains transactional data from a UK-based online retailer. It includes information about customer orders, products, and order details.

## Usage

### Create Northwind Tables

```bash
python3 create_northwind_tables.py
```
This will download the Northwind datasets from GitHub and create DuckDB tables in an in-memory database. 

To use a DuckDB database file instead of in-memory, change the `db_path` variable in the script to a file path. For example:
```bash
python3 create_northwind_tables.py --db mynorthwindmydatabase.duckdb
```

### Create Online Retail Table

Launch DuckDB and run the following SQL command to create the Online Retail table:

```sql
CREATE OR REPLACE TABLE online_retail AS
FROM read_csv(
    'https://raw.githubusercontent.com/databricks/Spark-The-Definitive-Guide/refs/heads/master/data/retail-data/all/online-retail-dataset.csv',
    columns = {
        'InvoiceNo': 'VARCHAR',
        'StockCode': 'VARCHAR',
        'Description': 'VARCHAR',
        'Quantity': 'INTEGER',
        'InvoiceDate': 'TIMESTAMP',
        'UnitPrice': 'DOUBLE',
        'CustomerID': 'VARCHAR',
        'Country': 'VARCHAR'
    },
    timestampformat = '%m/%d/%Y %H:%M'
);
```

> DuckDB's `read_csv_auto` function is not used here because it incorrectly InvoiceDate a VARCHAR and not a TIMESTAMP. The `read_csv` function allows for more control over the column types and formats.

## Data cleaning

### Northwind Dataset

The customers, orders and suppliers tables had commas in some cells:
- nw_customers: `fax` column
- nw_orders: `shipCountry` column
- nw_suppliers: `homePage` column

To be safe, all CSV files were cleaned by removing commas from all cells, before creating new tables in DuckDB. This is done in the `create_northwind_tables.py` script.

All table names are prefixed with `nw_` to avoid conflicts with existing tables in the DuckDB database.

## Example Queries

### Northwind Dataset

https://github.com/neo4j-contrib/northwind-neo4j/tree/master/data

Use [create_northwind_tables.py](create_northwind_tables.py) to import the data into duckdb tables


```sql

-- Get Customer Information for Orders
SELECT
    o.orderID,
    o.orderDate,
    c.customerID,
    c.companyName,
    c.contactName
FROM nw_orders o
JOIN nw_customers c ON o.customerID = c.customerID
LIMIT 10;

-- List Products in a Specific Order (e.g., OrderID 10248)
SELECT
    od.orderId,
    p.productName,
    od.quantity,
    od.unitPrice,
    (od.quantity * od.unitPrice) AS lineItemTotal 
FROM nw_order_details od
JOIN nw_products p ON od.productId = p.productId 
WHERE od.orderId = 10248;

-- Total Sales Value per Customer (Joining across Orders and Order Details)
SELECT
    c.customerId, 
    c.companyName, 
    SUM(od.quantity * od.unitPrice * (1 - od.discount)) AS totalSpent 
FROM nw_customers c
JOIN nw_orders o ON c.customerId = o.customerId 
JOIN nw_order_details od ON o.orderId = od.orderId
GROUP BY c.customerId, c.companyName 
ORDER BY totalSpent DESC 
LIMIT 10;

-- Monthly Sales Trend (Using OrderDate from Orders table)
SELECT
    strftime('%Y-%m', o.orderDate) AS saleMonth, 
    SUM(od.quantity * od.unitPrice * (1 - od.discount)) AS monthlyRevenue 
FROM nw_orders o 
JOIN nw_order_details od ON o.orderId = od.orderId 
WHERE o.orderDate IS NOT NULL 
GROUP BY saleMonth 
ORDER BY saleMonth; 

-- --- Northwind Analytics Queries ---

-- Sales Performance by Employee (Using nw_employees table)
SELECT
    e.firstName || ' ' || e.lastName AS employeeName,
    COUNT(DISTINCT o.orderId) AS totalOrdersHandled,
    SUM(od.quantity * od.unitPrice * (1 - od.discount)) AS totalSalesValue
FROM nw_orders o
JOIN nw_order_details od ON o.orderId = od.orderId
JOIN nw_employees e ON o.employeeId = e.employeeId -- Join to nw_employees table
GROUP BY employeeName
ORDER BY totalSalesValue DESC;



-- Top Selling Products within Each Category (Using nw_categories table)
WITH ProductRevenue AS (
    SELECT
        p.categoryId,
        c.categoryName, -- Get categoryName from nw_categories
        p.productId,
        p.productName,
        SUM(od.quantity * od.unitPrice * (1 - od.discount)) AS totalProductRevenue
    FROM nw_order_details od
    JOIN nw_products p ON od.productId = p.productId
    JOIN nw_categories c ON p.categoryId = c.categoryId -- Join to nw_categories table
    GROUP BY p.categoryId, c.categoryName, p.productId, p.productName
),
RankedProductRevenue AS (
    SELECT
        categoryId,
        categoryName,
        productId,
        productName,
        totalProductRevenue,
        -- Rank products within their category by revenue
        RANK() OVER (PARTITION BY categoryId ORDER BY totalProductRevenue DESC) as rankWithinCategory
    FROM ProductRevenue
)
SELECT
    categoryName,
    rankWithinCategory,
    productName,
    totalProductRevenue
FROM RankedProductRevenue
WHERE rankWithinCategory <= 3 -- Filter for the top 3 products per category
ORDER BY categoryName, rankWithinCategory;



-- Geographic Sales Analysis by Country and City (Already using nw_orders and nw_order_details)
SELECT
    o.shipCountry,
    o.shipCity,
    SUM(od.quantity * od.unitPrice * (1 - od.discount)) AS totalSalesValue,
    COUNT(DISTINCT o.orderId) AS totalOrders
FROM nw_orders o
JOIN nw_order_details od ON o.orderId = od.orderId
WHERE o.shipCountry IS NOT NULL AND o.shipCity IS NOT NULL
GROUP BY o.shipCountry, o.shipCity
ORDER BY o.shipCountry, totalSalesValue DESC;


-- Analyze Shipping Performance by Shipper (Using nw_shippers table)
SELECT
    s.companyName AS shipperName,
    COUNT(DISTINCT o.orderId) AS totalOrdersHandled,
    SUM(od.quantity * od.unitPrice * (1 - od.discount)) AS totalSalesValue,
    SUM(o.freight) AS totalFreightCost,
    -- Cast timestamps to DATE and subtract *after* filtering out invalid values
    AVG(o.shippedDate::DATE - o.orderDate::DATE) AS averageDaysToShip
FROM nw_orders o
JOIN nw_order_details od ON o.orderId = od.orderId
JOIN nw_shippers s ON o.shipVia = s.shipperId
WHERE
    -- Use TRY_CAST to check if the values can be parsed as DATE.
    -- This filters out SQL NULLs and the literal string "NULL" (and any other unparseable strings).
    TRY_CAST(o.shippedDate AS DATE) IS NOT NULL
    AND TRY_CAST(o.orderDate AS DATE) IS NOT NULL
GROUP BY shipperName
ORDER BY averageDaysToShip, totalSalesValue DESC;


-- Northwind Dataset: Customer Lifetime Value (Simple Sum of Sales) and Order Count
-- More sophisticated CLV models would require time windows, churn prediction etc.
SELECT
    c.customerId,
    c.companyName,
    COUNT(DISTINCT o.orderId) AS totalOrders,
    SUM(od.quantity * od.unitPrice * (1 - od.discount)) AS customerLifetimeValue
FROM nw_customers c
JOIN nw_orders o ON c.customerId = o.customerId
JOIN nw_order_details od ON o.orderId = od.orderId
GROUP BY c.customerId, c.companyName
ORDER BY customerLifetimeValue DESC
LIMIT 10;


```


### Online Retail Dataset

```sql

-- basic exploration of the online_retail dataset

DESCRIBE online_retail;

SELECT DISTINCT InvoiceDate FROM online_retail LIMIT 10;

-- View the first few rows
SELECT * FROM online_retail LIMIT 10;

-- Count total transactions (invoices)
SELECT COUNT(DISTINCT InvoiceNo) FROM online_retail;

-- Count total unique customers
SELECT COUNT(DISTINCT CustomerID) FROM online_retail WHERE CustomerID IS NOT NULL; -- Exclude rows without customer ID

-- Count total unique products sold
SELECT COUNT(DISTINCT StockCode) FROM online_retail;

-- Find transactions with negative quantity (returns?)
SELECT * FROM online_retail WHERE Quantity < 0 LIMIT 10;


-- Aggregations & Grouping (Orders/Transaction

-- Calculate Line Item Total Price
SELECT *, Quantity * UnitPrice AS LineTotal FROM online_retail LIMIT 10;

-- Calculate total revenue (sum of LineTotal, excluding returns/adjustments)
SELECT SUM(Quantity * UnitPrice) AS TotalRevenue
FROM online_retail
WHERE Quantity > 0 AND UnitPrice > 0;

-- Total revenue per country
SELECT Country, SUM(Quantity * UnitPrice) AS TotalRevenue
FROM online_retail
WHERE Quantity > 0 AND UnitPrice > 0
GROUP BY Country
ORDER BY TotalRevenue DESC;

-- Top 10 most sold products by quantity
SELECT Description, StockCode, SUM(Quantity) AS TotalQuantitySold
FROM online_retail
WHERE Quantity > 0 AND Description IS NOT NULL -- Filter out items without descriptions (often misc items)
GROUP BY Description, StockCode
ORDER BY TotalQuantitySold DESC
LIMIT 10;

-- Top 10 customers by total spending
SELECT CustomerID, SUM(Quantity * UnitPrice) AS TotalSpent
FROM online_retail
WHERE Quantity > 0 AND UnitPrice > 0 AND CustomerID IS NOT NULL
GROUP BY CustomerID
ORDER BY TotalSpent DESC
LIMIT 10;

-- Trend Analysis (Time-Based)

--dr as timestamps

-- Monthly Sales Trend
SELECT
    strftime('%Y-%m', InvoiceDate) AS SaleMonth, -- Format date to Year-Month
    SUM(Quantity * UnitPrice) AS MonthlyRevenue
FROM online_retail
WHERE Quantity > 0 AND UnitPrice > 0
GROUP BY SaleMonth
ORDER BY SaleMonth;

-- Daily Sales Trend (example for a specific month)
SELECT
    strftime('%Y-%m-%d', InvoiceDate) AS SaleDay, -- Format date to Year-Month-Day
    SUM(Quantity * UnitPrice) AS DailyRevenue
FROM online_retail
WHERE Quantity > 0 AND UnitPrice > 0
  AND InvoiceDate BETWEEN '2011-11-01' AND '2011-11-30 23:59:59' -- Example date range
GROUP BY SaleDay
ORDER BY SaleDay;

-- Number of new customers per month (Requires finding the first order date for each customer)
WITH CustomerFirstOrder AS (
    SELECT
        CustomerID,
        MIN(InvoiceDate) AS FirstOrderDate
    FROM online_retail
    WHERE CustomerID IS NOT NULL
    GROUP BY CustomerID
)
SELECT
    strftime('%Y-%m', FirstOrderDate) AS AcquisitionMonth,
    COUNT(CustomerID) AS NewCustomers
FROM CustomerFirstOrder
GROUP BY AcquisitionMonth
ORDER BY AcquisitionMonth;



-- Customer Segmentation (RFM Analysis)

-- Get Customer Information for Orders
SELECT
    o.OrderID,
    o.OrderDate,
    c.CustomerID,
    c.CompanyName,
    c.ContactName
FROM nw_orders o
JOIN nw_customers c ON o.CustomerID = c.CustomerID
LIMIT 10;

-- List Products in a Specific Order (e.g., OrderID 10248)
SELECT
    od.OrderID,
    p.ProductName,
    od.Quantity,
    od.UnitPrice,
    (od.Quantity * od.UnitPrice) AS LineItemTotal
FROM nw_order_details od
JOIN nw_products p ON od.ProductID = p.ProductID
WHERE od.OrderID = 10248;

-- Total Sales Value per Customer (Joining across Orders and Order Details)
SELECT
    c.CustomerID,
    c.CompanyName,
    SUM(od.Quantity * od.UnitPrice) AS TotalSpent
FROM nw_customers c
JOIN nw_orders o ON c.CustomerID = o.CustomerID
JOIN nw_order_details od ON o.OrderID = od.OrderID
GROUP BY c.CustomerID, c.CompanyName
ORDER BY TotalSpent DESC
LIMIT 10;

-- Monthly Sales Trend (Using OrderDate from Orders table)
SELECT
    strftime('%Y-%m', OrderDate) AS SaleMonth,
    SUM(od.Quantity * od.UnitPrice) AS MonthlyRevenue
FROM nw_orders o
JOIN nw_order_details od ON o.OrderID = od.OrderID
WHERE o.OrderDate IS NOT NULL -- Exclude orders without a date if any
GROUP BY SaleMonth
ORDER BY SaleMonth;

-- Analytics queries 

-- Online Retail Dataset: Customer Order Frequency and Average Basket Value
SELECT
    customerId,
    COUNT(DISTINCT invoiceNo) AS totalOrders,
    SUM(quantity * unitPrice) AS totalSpending,
    AVG(quantity * unitPrice) AS averageLineItemValue, -- Average value per item/line
    SUM(quantity * unitPrice) / COUNT(DISTINCT invoiceNo) AS averageOrderValue -- Average value per distinct invoice
FROM online_retail
WHERE
    customerId IS NOT NULL
    AND customerId != ''
    AND quantity > 0
    AND unitPrice > 0
GROUP BY customerId
HAVING COUNT(DISTINCT invoiceNo) > 0 -- Ensure they actually placed orders
ORDER BY totalSpending DESC
LIMIT 10;


-- Online Retail Dataset: Simple Product Affinity (Top 10 Pairs of Products Bought in the Same Invoice)
SELECT
    od1.description AS product1,
    od2.description AS product2,
    COUNT(*) AS sharedInvoices
FROM online_retail od1
JOIN online_retail od2
    ON od1.invoiceNo = od2.invoiceNo
    AND od1.stockCode < od2.stockCode -- Avoid self-joins and duplicate pairs (A, B vs B, A)
WHERE
    od1.quantity > 0 AND od1.unitPrice > 0
    AND od2.quantity > 0 AND od2.unitPrice > 0
    AND od1.description IS NOT NULL AND od2.description IS NOT NULL
GROUP BY product1, product2
ORDER BY sharedInvoices DESC
LIMIT 10;

-- Online Retail Dataset: Monthly Cohort Analysis (Total Spending by Acquisition Month and Order Month)
WITH CustomerFirstOrder AS (
    SELECT
        customerId,
        MIN(invoiceDate) AS firstOrderDate
    FROM online_retail
    WHERE
        customerId IS NOT NULL
        AND customerId != ''
    GROUP BY customerId
)
SELECT
    strftime('%Y-%m', cfo.firstOrderDate) AS acquisitionMonth,
    strftime('%Y-%m', od.invoiceDate) AS orderMonth,
    COUNT(DISTINCT od.customerId) AS monthlyActiveCustomers, -- How many from this cohort were active this month
    SUM(od.quantity * od.unitPrice) AS monthlySpending -- Total spending by this cohort this month
FROM online_retail od
JOIN CustomerFirstOrder cfo
    ON od.customerId = cfo.customerId
WHERE
    od.quantity > 0 AND od.unitPrice > 0
    AND od.customerId IS NOT NULL
    AND od.customerId != ''
GROUP BY acquisitionMonth, orderMonth
ORDER BY acquisitionMonth, orderMonth;

```



## License

MIT License

Copyright (c) 2025

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.