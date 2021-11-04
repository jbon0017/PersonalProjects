-- Create a table to hold an audit of Errors (if the table does not exist already). 
IF OBJECT_ID (N'dbo.ErrorAudit', N'U') IS NULL 
CREATE TABLE dbo.ErrorAudit (
  [UserName]		nvarchar(200) NOT NULL
 ,[ErrorNumber]		int NOT NULL
 ,[ErrorState]		int NOT NULL
 ,[ErrorSeverity]	int NOT NULL
 ,[ErrorLine]		int NOT NULL
 ,[ErrorProcedure]	nvarchar (200) NULL
 ,[ErrorMessage]	nvarchar (max) NOT NULL
 ,[ErrorTime]		datetime NULL
);  

-- Wrap the MERGE and other commands in TRY/ CATCH and TRANSACTION
BEGIN TRY
	BEGIN TRANSACTION DoMerge
		-- Drop any temp tables if they exist to allow re-generation.
		IF OBJECT_ID('tempdb..#OldValues') IS NOT NULL 
		DROP TABLE #OldValues;
		
		IF OBJECT_ID('tempdb..#ChangedValues') IS NOT NULL 
		DROP TABLE #ChangedValues;
		
		-- Create a temporary table to hold the output actions and values.  
		CREATE TABLE #ChangedValues (
		 [Action]				 nvarchar(50) NULL
		,[New_BusinessEntityID]  int NULL
		,[New_PersonType]		 nchar(2) NULL
		,[New_Title]			 nvarchar(8) NULL
		,[New_FirstName]		 nvarchar(50) NULL
		,[New_MiddleName]		 nvarchar(50) NULL
		,[New_LastName]			 nvarchar(50) NULL
		,[New_EmailPromotion]	 int NULL
		);  

		-- Take record of values before any change and save in Temporary Table #OldValues.
		SELECT 
		 [BusinessEntityID]
		,[PersonType]
		,[Title]
		,[FirstName]
		,[MiddleName]
		,[LastName]
		,[EmailPromotion]
		INTO #OldValues
		FROM [Person].[Person] 
		WHERE [BusinessEntityID] IN (15725,1976,1986);
		
		-- Run MERGE, and save updated/ inserted values and action in Temporary Table #ChangedValues.
		INSERT INTO #ChangedValues
		SELECT 
		 [Action]
		,[New_BusinessEntityID]
		,[New_PersonType]		
		,[New_Title]			
		,[New_FirstName]		
		,[New_MiddleName]		
		,[New_LastName]		
		,[New_EmailPromotion]
		FROM  
		(
		MERGE INTO [Person].[Person] AS targetTable  
		USING (VALUES 
			   (15725,'EM', 'Dr', 'Billy' ,'R' ,'Gomez',0),
			   (1976,'EM', 'Ms', 'Alessia' ,'G' ,'Bianchi',1),
			   (1986,'EM', NULL, 'Austin' ,NULL ,'Powers',2)
			   )
		       as sourceTable (
			   [BusinessEntityID]
			  ,[PersonType]
		      ,[Title]
		      ,[FirstName]
		      ,[MiddleName]
		      ,[LastName]
			  ,[EmailPromotion])  
		ON targetTable.[BusinessEntityID] = sourceTable.[BusinessEntityID]  
		WHEN MATCHED THEN  
		UPDATE 
			SET 
				[Title] = sourceTable.[Title],
				[PersonType] = sourceTable.[PersonType],
				[FirstName] = sourceTable.[FirstName],
				[MiddleName] = sourceTable.[MiddleName],
				[LastName] = sourceTable.[LastName],
				[EmailPromotion] = sourceTable.[EmailPromotion],
				[ModifiedDate] = GETDATE()
		WHEN NOT MATCHED BY TARGET THEN  
		INSERT (
			   [BusinessEntityID]
			  ,[PersonType]
			  ,[Title]
		      ,[FirstName]
		      ,[MiddleName]
		      ,[LastName]
			  ,[EmailPromotion]
			  ,[ModifiedDate]) 
		VALUES (
			   sourceTable.[BusinessEntityID]
			  ,sourceTable.[PersonType]
			  ,sourceTable.[Title]
		      ,sourceTable.[FirstName]
		      ,sourceTable.[MiddleName]
		      ,sourceTable.[LastName]
			  ,sourceTable.[EmailPromotion]
			  ,GETDATE()
			   ) 
		OUTPUT 
			 $action AS [Action]
			,Inserted.[BusinessEntityID] AS [New_BusinessEntityID]
			,Inserted.[PersonType]		 AS [New_PersonType]		
			,Inserted.[Title]			 AS [New_Title]			
			,Inserted.[FirstName]		 AS [New_FirstName]		
			,Inserted.[MiddleName]		 AS [New_MiddleName]		
			,Inserted.[LastName]		 AS [New_LastName]		
			,Inserted.[EmailPromotion]	 AS [New_EmailPromotion]	
		) AS Updates (
		 [Action]
		,[New_BusinessEntityID]
		,[New_PersonType]		
		,[New_Title]			
		,[New_FirstName]		
		,[New_MiddleName]		
		,[New_LastName]		
		,[New_EmailPromotion]	
		);
		
		-- Display action for each row and state of values before and after relevant MERGE action.
		SELECT 
			 C.[Action]
			,O.[BusinessEntityID] AS [Old_BusinessEntityID]
			,C.[New_BusinessEntityID]
			,O.[PersonType] AS [Old_PersonType]		
			,C.[New_PersonType]	
			,O.[Title] AS [Old_Title]			
			,C.[New_Title]	
			,O.[FirstName] AS [Old_FirstName]		
			,C.[New_FirstName]	
			,O.[MiddleName] AS [Old_MiddleName]		
			,C.[New_MiddleName]		 
			,O.[LastName] AS [Old_LastName]		 	
			,C.[New_LastName]	 
			,O.[EmailPromotion] AS [Old_EmailPromotion]	 	 
			,C.[New_EmailPromotion]	 	 		
		FROM #ChangedValues C
		LEFT OUTER JOIN #OldValues O ON C.New_BusinessEntityID = O.BusinessEntityID;
	COMMIT TRANSACTION DoMerge
END TRY
BEGIN CATCH
	
	    INSERT INTO dbo.ErrorAudit ([UserName],[ErrorNumber],[ErrorState],[ErrorSeverity],[ErrorLine],[ErrorProcedure],[ErrorMessage],[ErrorTime])
	    VALUES
		(SUSER_SNAME(),
		 ERROR_NUMBER(),
		 ERROR_STATE(),
		 ERROR_SEVERITY(),
		 ERROR_LINE(),
		 ERROR_PROCEDURE(),
		 ERROR_MESSAGE(),
		 GETDATE());

		-- Transaction uncommittable
		IF (XACT_STATE()) = -1
		  ROLLBACK TRANSACTION DoMerge
 
		-- Transaction committable
		IF (XACT_STATE()) = 1
		  COMMIT TRANSACTION DoMerge
		  
		DECLARE 
		@Message varchar(MAX) = ERROR_MESSAGE(),
        @Severity int = ERROR_SEVERITY(),
        @State int = ERROR_STATE();
 
		RAISERROR(@Message, @Severity, @State);

END CATCH;
GO


SELECT * 
FROM Dbo.ErrorAudit

