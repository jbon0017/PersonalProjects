;WITH
--Sales Header Recursive CTE
Sales (SequenceNo, [SalesOrderNumber],
		[OrderDate],
		[DueDate],
		[CustomerID],
		[Freight],
		[TaxAmt],
		[TotalDue]) 
AS (
	--Get the current version of Sales Order Header
    SELECT 
        0, 
        [SalesOrderNumber],
		[OrderDate],
		[DueDate],
		[CustomerID],
		[Freight],
		[TaxAmt],
		[TotalDue]
	FROM [Sales].[SalesOrderHeader]
    UNION ALL
	--Explode header by each date occurrence in the difference between OrderDate and DueDate
    SELECT    
        SequenceNo + 1, 
        [SalesOrderNumber],
		DATEADD(DAY, 1, [OrderDate]) AS [OrderDate],
		[DueDate],
		[CustomerID],
		[Freight],
		[TaxAmt],
		[TotalDue]
    FROM    
        Sales
    WHERE SequenceNo < DATEDIFF(day,[OrderDate],[DueDate])
)
SELECT *
FROM Sales
ORDER BY SalesOrderNumber, SequenceNo
OPTION (MAXRECURSION 0)


