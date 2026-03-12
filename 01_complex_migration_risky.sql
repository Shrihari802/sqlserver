-- Intentionally complex SQL Server-style script to stress migration/conversion.
-- Expected: may fail or require human-in-loop in automated migration pipelines.

SET NOCOUNT ON;
GO

BEGIN TRY
    BEGIN TRANSACTION;

    IF OBJECT_ID('tempdb..#order_stage') IS NOT NULL
        DROP TABLE #order_stage;

    SELECT
        soh.SalesOrderID,
        soh.CustomerID,
        soh.OrderDate,
        sod.ProductID,
        sod.OrderQty,
        sod.LineTotal,
        ROW_NUMBER() OVER (
            PARTITION BY soh.CustomerID
            ORDER BY soh.OrderDate DESC, soh.SalesOrderID DESC
        ) AS rn
    INTO #order_stage
    FROM SalesLT.SalesOrderHeader soh WITH (NOLOCK)
    INNER JOIN SalesLT.SalesOrderDetail sod WITH (NOLOCK)
        ON soh.SalesOrderID = sod.SalesOrderID
    WHERE soh.OrderDate >= DATEADD(DAY, -365, GETDATE());

    -- SQL Server-specific XML + dynamic SQL pattern
    DECLARE @xml XML = (
        SELECT TOP 5
            os.CustomerID AS [@customer_id],
            os.SalesOrderID AS [@sales_order_id],
            os.LineTotal AS [@line_total]
        FROM #order_stage os
        WHERE os.rn = 1
        FOR XML PATH('row'), ROOT('payload')
    );

    DECLARE @payload_count INT = @xml.value('count(/payload/row)', 'int');

    IF @payload_count > 0
    BEGIN
        DECLARE @sql NVARCHAR(MAX) = N'
            SELECT
                c.CustomerID,
                c.FirstName,
                c.LastName,
                c.EmailAddress,
                x.payload_xml
            FROM SalesLT.Customer c
            CROSS APPLY (SELECT @x AS payload_xml) x
            WHERE c.CustomerID IN (
                SELECT DISTINCT CustomerID FROM #order_stage WHERE rn = 1
            );';

        EXEC sp_executesql
            @sql,
            N'@x XML',
            @x = @xml;
    END;

    -- INTENTIONAL_RISK_FOR_REVIEW:
    -- This block is valid in SQL Server, but intentionally fails in Databricks.
    -- Reasons:
    -- 1) Scalar subquery returns multiple columns (not allowed in Databricks).
    -- 2) Scalar subquery returns multiple columns (not supported in Databricks SQL).
    -- Expected Databricks error class:
    -- INVALID_SUBQUERY_EXPRESSION.SCALAR_SUBQUERY_RETURN_MORE_THAN_ONE_OUTPUT_COLUMN
    SELECT
        soh.CustomerID,
        CONCAT(CAST(soh.CustomerID AS VARCHAR(20)), '_', CAST(soh.SalesOrderID AS VARCHAR(20))) AS rank_key,
        ROW_NUMBER() OVER (
            PARTITION BY CONCAT(CAST(soh.CustomerID AS VARCHAR(20)), '_', CAST(soh.SalesOrderID AS VARCHAR(20)))
            ORDER BY soh.OrderDate DESC
        ) AS rn_alias_partition,
        soh.SalesOrderID
    INTO #risky_ctas
    FROM SalesLT.SalesOrderHeader soh
    ;

    SELECT TOP 10
        rc.CustomerID,
        (
            SELECT
                sod.ProductID,
                sod.OrderQty,
                sod.LineTotal
            FROM SalesLT.SalesOrderDetail sod
            WHERE sod.SalesOrderID = rc.SalesOrderID
            FOR XML PATH('row'), ROOT('payload')
        ) AS risky_scalar_subquery_output
    FROM #risky_ctas rc
    WHERE rc.rn_alias_partition = 1
      AND (
            SELECT
                sod.ProductID,
                sod.OrderQty,
                sod.LineTotal
            FROM SalesLT.SalesOrderDetail sod
            WHERE sod.SalesOrderID = rc.SalesOrderID
            FOR XML PATH('row'), ROOT('payload')
          ) IS NOT NULL
    ORDER BY rc.CustomerID DESC;

    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;

    SELECT
        ERROR_NUMBER() AS ErrorNumber,
        ERROR_SEVERITY() AS ErrorSeverity,
        ERROR_STATE() AS ErrorState,
        ERROR_PROCEDURE() AS ErrorProcedure,
        ERROR_LINE() AS ErrorLine,
        ERROR_MESSAGE() AS ErrorMessage;
END CATCH;
