# DuckDB Northwind Data Tools

Tools for loading and analyzing the Northwind dataset in DuckDB and Jupyter notebooks.

## Contents

- `create_northwind_tables.py`: Python script to download and create Northwind tables in DuckDB
- `clean.ipynb`: Jupyter notebook for data exploration and cleaning of CSV files

## Data Source

The Northwind dataset is sourced from:
https://github.com/neo4j-contrib/northwind-neo4j/tree/master/data/

This is a standard demo database containing business data like customers, orders, products, shippers and suppliers. Originally created by Microsoft to demonstrate MS Access and SQL Server features.

This repository uses camelCase for column names.

In theory, any GitHub repository can be used as a data source. The script `create_northwind_tables.py` downloads the CSV files from the GitHub repository and creates DuckDB tables.

## Usage

```bash
python3 create_northwind_tables.py
```
This will download the Northwind datasets from GitHub and create DuckDB tables in an in-memory database. 

To use a DuckDB database file instead of in-memory, change the `db_path` variable in the script to a file path. For example:
```bash
python3 create_northwind_tables.py --db mynorthwindmydatabase.duckdb
```

## Data cleaning

The customers, orders and suppliers tables had commas in some cells:
- nw_customers: `fax` column
- nw_orders: `shipCountry` column
- nw_suppliers: `homePage` column

To be safe, all CSV files were cleaned by removing commas from all cells, before creating new tables in DuckDB. This is done in the `create_northwind_tables.py` script.

All table names are prefixed with `nw_` to avoid conflicts with existing tables in the DuckDB database.

## Northwind Example Queries

# northwind data

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