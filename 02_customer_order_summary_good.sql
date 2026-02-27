-- Clean analytics query expected to migrate well and pass validation.

SELECT
    c.CustomerID,
    c.FirstName,
    c.LastName,
    c.EmailAddress,
    COUNT(DISTINCT soh.SalesOrderID) AS total_orders,
    SUM(CAST(soh.TotalDue AS DECIMAL(18, 2))) AS total_revenue,
    MIN(soh.OrderDate) AS first_order_date,
    MAX(soh.OrderDate) AS last_order_date
FROM SalesLT.Customer c
LEFT JOIN SalesLT.SalesOrderHeader soh
    ON c.CustomerID = soh.CustomerID
GROUP BY
    c.CustomerID,
    c.FirstName,
    c.LastName,
    c.EmailAddress
ORDER BY
    total_revenue DESC,
    total_orders DESC;

