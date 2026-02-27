-- Clean product performance query expected to migrate well and pass validation.

SELECT
    p.ProductID,
    p.Name AS product_name,
    pc.Name AS category_name,
    COUNT(sod.ProductID) AS order_line_count,
    COALESCE(SUM(CAST(sod.OrderQty AS INT)), 0) AS units_sold,
    COALESCE(SUM(CAST(sod.LineTotal AS DECIMAL(18, 2))), 0) AS gross_sales,
    COALESCE(AVG(CAST(sod.UnitPrice AS DECIMAL(18, 2))), 0) AS avg_unit_price
FROM SalesLT.Product p
LEFT JOIN SalesLT.ProductCategory pc
    ON p.ProductCategoryID = pc.ProductCategoryID
LEFT JOIN SalesLT.SalesOrderDetail sod
    ON p.ProductID = sod.ProductID
GROUP BY
    p.ProductID,
    p.Name,
    pc.Name

UNION ALL

SELECT
    CAST(-1 AS INT) AS ProductID,
    'NO_PRODUCT_DATA' AS product_name,
    'NO_CATEGORY' AS category_name,
    0 AS order_line_count,
    CAST(0 AS BIGINT) AS units_sold,
    CAST(0 AS DECIMAL(18, 2)) AS gross_sales,
    CAST(0 AS DECIMAL(18, 2)) AS avg_unit_price
WHERE NOT EXISTS (
    SELECT 1 FROM SalesLT.Product
)

ORDER BY
    gross_sales DESC,
    units_sold DESC;
