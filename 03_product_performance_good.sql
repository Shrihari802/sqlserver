-- Clean product performance query expected to migrate well and pass validation.

SELECT
    p.ProductID,
    p.Name AS product_name,
    pc.Name AS category_name,
    COUNT(*) AS order_line_count,
    SUM(CAST(sod.OrderQty AS INT)) AS units_sold,
    SUM(CAST(sod.LineTotal AS DECIMAL(18, 2))) AS gross_sales,
    AVG(CAST(sod.UnitPrice AS DECIMAL(18, 2))) AS avg_unit_price
FROM xport.Product p
LEFT JOIN xport.ProductCategory pc
    ON p.ProductCategoryID = pc.ProductCategoryID
INNER JOIN xport.SalesOrderDetail sod
    ON p.ProductID = sod.ProductID
GROUP BY
    p.ProductID,
    p.Name,
    pc.Name
HAVING
    SUM(CAST(sod.OrderQty AS INT)) > 0
ORDER BY
    gross_sales DESC,
    units_sold DESC;

