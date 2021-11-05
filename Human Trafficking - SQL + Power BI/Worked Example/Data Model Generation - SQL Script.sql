--Create Schema  
CREATE SCHEMA Stage
GO
CREATE SCHEMA Dim
GO
CREATE SCHEMA Fct
GO
CREATE SCHEMA Dimension
GO
CREATE SCHEMA Fact
GO
 
--Create target Stage, Dim, Fct Tables
CREATE TABLE [Stage].[HumanTrafficking](
	[DATA_YEAR]				 [nvarchar](250) NULL,
	[ORI]					 [nvarchar](250) NULL,
	[PUB_AGENCY_NAME]		 [nvarchar](250) NULL,
	[PUB_AGENCY_UNIT]		 [nvarchar](250) NULL,
	[AGENCY_TYPE_NAME]		 [nvarchar](250) NULL,
	[STATE_ABBR]			 [nvarchar](250) NULL,
	[STATE_NAME]			 [nvarchar](250) NULL,
	[DIVISION_NAME]			 [nvarchar](250) NULL,
	[COUNTY_NAME]			 [nvarchar](250) NULL,
	[REGION_NAME]			 [nvarchar](250) NULL,
	[POPULATION_GROUP_CODE]  [nvarchar](250) NULL,
	[POPULATION_GROUP_DESC]  [nvarchar](250) NULL,
	[OFFENSE_SUBCAT_ID]		 [nvarchar](250) NULL,
	[OFFENSE_NAME]			 [nvarchar](250) NULL,
	[OFFENSE_SUBCAT_NAME]	 [nvarchar](250) NULL,
	[ACTUAL_COUNT]			 [nvarchar](250) NULL,
	[UNFOUNDED_COUNT]		 [nvarchar](250) NULL,
	[CLEARED_COUNT]			 [nvarchar](250) NULL,
	[JUVENILE_CLEARED_COUNT] [nvarchar](250) NULL
) ON [PRIMARY]
GO

CREATE TABLE [Fct].[HumanTrafficking](
	[SurrogateID]			 [NVARCHAR](500) NOT NULL,
	[DateKey]				 [INT] NOT NULL,
	[PubAgencyKey]			 [INT] NOT NULL,
	[AgencyTypeKey]			 [INT] NOT NULL,
	[DivisionKey]			 [INT] NOT NULL,
	[CountyKey]				 [INT] NOT NULL,
	[RegionKey]				 [INT] NOT NULL,
	[StateKey]				 [INT] NOT NULL,
	[PopulationGroupKey]	 [INT] NOT NULL,
	[OffenseSubcategoryKey]	 [INT] NOT NULL,
	[OffenseKey]			 [INT] NOT NULL,
	[ActualCount]			 [INT] NOT NULL,
	[UnfoundedCount]		 [INT] NOT NULL,
	[ClearedCount]			 [INT] NOT NULL,
	[JuvenileClearedCount]	 [INT] NOT NULL,
	[CreatedDate]			 [DATETIME] NOT NULL,
	[ModifiedDate]			 [DATETIME] NULL
) ON [PRIMARY]
GO

CREATE TABLE [Dim].[PubAgency](
	[PubAgencyKey]					 [INT] IDENTITY(1,1),
	[OriginatingAgencyIdentifier]	 [nvarchar](25) NOT NULL,
	[PubAgencyName]					 [nvarchar](250) NOT NULL,
	[PubAgencyUnit]					 [nvarchar](250) NOT NULL,
	[CreatedDate]					 [DATETIME] NOT NULL,
	[ModifiedDate]					 [DATETIME] NULL
) ON [PRIMARY]
GO

CREATE TABLE [Dim].[AgencyType](
	[AgencyTypeKey]		 [INT] IDENTITY(1,1),
	[AgencyTypeName]	 [nvarchar](250) NOT NULL,
	[CreatedDate]		 [DATETIME] NOT NULL,
	[ModifiedDate]		 [DATETIME] NULL
) ON [PRIMARY]
GO

CREATE TABLE [Dim].[Date](
	[DateKey]				 [INT] IDENTITY(1,1),
	[Year]					 [INT] NOT NULL,
	[CreatedDate]			 [DATETIME] NOT NULL,
	[ModifiedDate]			 [DATETIME] NULL
) ON [PRIMARY]
GO

CREATE TABLE [Dim].[State](
	[StateKey]				 [INT] IDENTITY(1,1),
	[StateAbbreviation]		 [nvarchar](250) NOT NULL,
	[StateName]				 [nvarchar](250) NOT NULL,
	[CreatedDate]			 [DATETIME] NOT NULL,
	[ModifiedDate]			 [DATETIME] NULL
) ON [PRIMARY]
GO

CREATE TABLE [Dim].[Division](
	[DivisionKey]			 [INT] IDENTITY(1,1),
	[DivisionName]			 [nvarchar](250) NOT NULL,
	[CreatedDate]			 [DATETIME] NOT NULL,
	[ModifiedDate]			 [DATETIME] NULL
) ON [PRIMARY]
GO

CREATE TABLE [Dim].[County](
	[CountyKey]			 [INT] IDENTITY(1,1),
	[CountyName]		 [nvarchar](250) NOT NULL,
	[CreatedDate]		 [DATETIME] NOT NULL,
	[ModifiedDate]		 [DATETIME] NULL
) ON [PRIMARY]
GO

CREATE TABLE [Dim].[Region](
	[RegionKey]			 [INT] IDENTITY(1,1),
	[RegionName]		 [nvarchar](250) NOT NULL,
	[CreatedDate]		 [DATETIME] NOT NULL,
	[ModifiedDate]		 [DATETIME] NULL
) ON [PRIMARY]
GO

CREATE TABLE [Dim].[PopulationGroup](
	[PopulationGroupKey]	[INT] IDENTITY(1,1),
	[PopulationGroupCode]	[nvarchar](25) NOT NULL,
	[PopulationGroupName]	[nvarchar](250) NOT NULL,
	[CreatedDate]			[DATETIME] NOT NULL,
	[ModifiedDate]			[DATETIME] NULL
) ON [PRIMARY]
GO

CREATE TABLE [Dim].[OffenseSubcategory](
	[OffenseSubcategoryKey]		[INT] IDENTITY(1,1),
	[OffenseSubcategoryCode]	[nvarchar](25) NOT NULL,
	[OffenseSubcategoryName]	[nvarchar](250) NOT NULL,
	[CreatedDate]				[DATETIME] NOT NULL,
	[ModifiedDate]				[DATETIME] NULL
) ON [PRIMARY]
GO

CREATE TABLE [Dim].[Offense](
	[OffenseKey]	[INT] IDENTITY(1,1),
	[OffenseName]	[nvarchar](250) NOT NULL,
	[CreatedDate]	[DATETIME] NOT NULL,
	[ModifiedDate]	[DATETIME] NULL
) ON [PRIMARY]
GO

--Create Transform Stored Procedure from Stage to relevant Dims/ Fcts
CREATE PROCEDURE [Stage].[StageToDimPubAgency]
AS
BEGIN
;WITH sourceTable AS
(
SELECT 
	  DISTINCT 
	  ISNULL([ORI]			   , 'Unknown') AS [OriginatingAgencyIdentifier]	
	 ,ISNULL([PUB_AGENCY_NAME] , 'Unknown') AS [PubAgencyName]					
	 ,ISNULL([PUB_AGENCY_UNIT] , 'Unknown') AS [PubAgencyUnit]					
FROM [Stage].[HumanTrafficking]
)
MERGE INTO
        [Dim].[PubAgency] targetTable
    USING
        sourceTable
            ON sourceTable.[OriginatingAgencyIdentifier] = targetTable.[OriginatingAgencyIdentifier]    
    WHEN MATCHED THEN
        UPDATE
        SET
    	targetTable.[PubAgencyName] = sourceTable.[PubAgencyName],
		targetTable.[PubAgencyUnit] = sourceTable.[PubAgencyUnit],
		targetTable.[ModifiedDate]	= GETDATE()
 WHEN NOT MATCHED BY TARGET THEN
        INSERT
            (
    		 [OriginatingAgencyIdentifier]
			,[PubAgencyName]				
			,[PubAgencyUnit]
			,[CreatedDate]
			,[ModifiedDate]
            )
        VALUES
            (
    		 sourceTable.[OriginatingAgencyIdentifier]
			,sourceTable.[PubAgencyName]				
			,sourceTable.[PubAgencyUnit]	
			,GETDATE()
			,NULL
            ); 
END
GO

CREATE PROCEDURE [Stage].[StageToDimOffense]
AS
BEGIN
;WITH sourceTable AS
(
SELECT 
	  DISTINCT 
	  ISNULL([OFFENSE_NAME],'Unknown')	AS [OffenseName]					
FROM [Stage].[HumanTrafficking]
)
MERGE INTO
        [Dim].[Offense] targetTable
    USING
        sourceTable
            ON sourceTable.[OffenseName] = targetTable.[OffenseName]    
 WHEN NOT MATCHED BY TARGET THEN
        INSERT
            (
    		 [OffenseName]
			,[CreatedDate]
			,[ModifiedDate]
            )
        VALUES
            (			
			 sourceTable.[OffenseName]	
			,GETDATE()
			,NULL
            ); 
END
GO

CREATE PROCEDURE [Stage].[StageToDimOffenseSubcategory]
AS
BEGIN
;WITH sourceTable AS
(
SELECT 
	  DISTINCT 
	   ISNULL([OFFENSE_SUBCAT_ID]	, 'Unknown')  AS [OffenseSubcategoryCode]
	  ,ISNULL([OFFENSE_SUBCAT_NAME] , 'Unknown')  AS [OffenseSubcategoryName]
FROM [Stage].[HumanTrafficking]
)
MERGE INTO
        [Dim].[OffenseSubcategory] targetTable
    USING
        sourceTable
            ON sourceTable.[OffenseSubcategoryCode] = targetTable.[OffenseSubcategoryCode]       
 WHEN MATCHED THEN
        UPDATE
        SET
    	targetTable.[OffenseSubcategoryName] = sourceTable.[OffenseSubcategoryName],
		targetTable.[ModifiedDate]	= GETDATE()
 WHEN NOT MATCHED BY TARGET THEN
        INSERT
            (
			 [OffenseSubcategoryCode]
    		,[OffenseSubcategoryName]
			,[CreatedDate]
			,[ModifiedDate]
            )
        VALUES
            (			
			 sourceTable.[OffenseSubcategoryCode]	
			,sourceTable.[OffenseSubcategoryName]	
			,GETDATE()
			,NULL
            ); 
END
GO

CREATE PROCEDURE [Stage].[StageToDimPopulationGroup]
AS
BEGIN
;WITH sourceTable AS
(
SELECT 
	  DISTINCT 
	   ISNULL([POPULATION_GROUP_CODE], 'Unknown')	AS [PopulationGroupCode]
	  ,ISNULL([POPULATION_GROUP_DESC], 'Unknown')	AS [PopulationGroupName]
FROM [Stage].[HumanTrafficking]
WHERE [POPULATION_GROUP_CODE] IS NOT NULL AND [POPULATION_GROUP_DESC] IS NOT NULL
)
MERGE INTO
        [Dim].[PopulationGroup] targetTable
    USING
        sourceTable
            ON sourceTable.[PopulationGroupCode] = targetTable.[PopulationGroupCode]       
 WHEN MATCHED THEN
        UPDATE
        SET
    	targetTable.[PopulationGroupName] = sourceTable.[PopulationGroupName],
		targetTable.[ModifiedDate]	= GETDATE()
 WHEN NOT MATCHED BY TARGET THEN
        INSERT
            (
			 [PopulationGroupCode]
    		,[PopulationGroupName]
			,[CreatedDate]
			,[ModifiedDate]
            )
        VALUES
            (			
			 sourceTable.[PopulationGroupCode]	
			,sourceTable.[PopulationGroupName]	
			,GETDATE()
			,NULL
            ); 
END
GO

CREATE PROCEDURE [Stage].[StageToDimRegion]
AS
BEGIN
;WITH sourceTable AS
(
SELECT 
	  DISTINCT 
	  ISNULL([REGION_NAME], 'Unknown')	AS [RegionName]					
FROM [Stage].[HumanTrafficking]
)
MERGE INTO
        [Dim].[Region] targetTable
    USING
        sourceTable
            ON sourceTable.[RegionName] = targetTable.[RegionName]    
 WHEN NOT MATCHED BY TARGET THEN
        INSERT
            (
    		 [RegionName]
			,[CreatedDate]
			,[ModifiedDate]
            )
        VALUES
            (			
			 sourceTable.[RegionName]	
			,GETDATE()
			,NULL
            ); 
END
GO

CREATE PROCEDURE [Stage].[StageToDimCounty]
AS
BEGIN
;WITH sourceTable AS
(
SELECT 
	  DISTINCT 
	  ISNULL([COUNTY_NAME],'Unknown')	AS [CountyName]					
FROM [Stage].[HumanTrafficking]
)
MERGE INTO
        [Dim].[County] targetTable
    USING
        sourceTable
            ON sourceTable.[CountyName] = targetTable.[CountyName]    
 WHEN NOT MATCHED BY TARGET THEN
        INSERT
            (
    		 [CountyName]
			,[CreatedDate]
			,[ModifiedDate]
            )
        VALUES
            (			
			 sourceTable.[CountyName]	
			,GETDATE()
			,NULL
            ); 
END
GO

CREATE PROCEDURE [Stage].[StageToDimDivision]
AS
BEGIN
;WITH sourceTable AS
(
SELECT 
	  DISTINCT 
	  ISNULL([DIVISION_NAME],'Unknown')	AS [DivisionName]					
FROM [Stage].[HumanTrafficking]
)
MERGE INTO
        [Dim].[Division] targetTable
    USING
        sourceTable
            ON sourceTable.[DivisionName] = targetTable.[DivisionName]    
 WHEN NOT MATCHED BY TARGET THEN
        INSERT
            (
    		 [DivisionName]
			,[CreatedDate]
			,[ModifiedDate]
            )
        VALUES
            (			
			 sourceTable.[DivisionName]	
			,GETDATE()
			,NULL
            ); 
END
GO

CREATE PROCEDURE [Stage].[StageToDimAgencyType]
AS
BEGIN
;WITH sourceTable AS
(
SELECT 
	  DISTINCT 
	  ISNULL([AGENCY_TYPE_NAME],'Unknown')	AS [AgencyTypeName]					
FROM [Stage].[HumanTrafficking]
)
MERGE INTO
        [Dim].[AgencyType] targetTable
    USING
        sourceTable
            ON sourceTable.[AgencyTypeName] = targetTable.[AgencyTypeName]    
 WHEN NOT MATCHED BY TARGET THEN
        INSERT
            (
    		 [AgencyTypeName]
			,[CreatedDate]
			,[ModifiedDate]
            )
        VALUES
            (			
			 sourceTable.[AgencyTypeName]	
			,GETDATE()
			,NULL
            ); 
END
GO

CREATE PROCEDURE [Stage].[StageToDimState]
AS
BEGIN
;WITH sourceTable AS
(
SELECT 
	  DISTINCT 
	  ISNULL([STATE_ABBR],'Unknown')	AS [StateAbbreviation],	
	  ISNULL([STATE_NAME],'Unknown')	AS [StateName]					
FROM [Stage].[HumanTrafficking]
)
MERGE INTO
        [Dim].[State] targetTable
    USING
        sourceTable
            ON sourceTable.[StateAbbreviation] = targetTable.[StateAbbreviation]       
 WHEN MATCHED THEN
        UPDATE
        SET
    	targetTable.[StateName] = sourceTable.[StateName],
		targetTable.[ModifiedDate]	= GETDATE()  
 WHEN NOT MATCHED BY TARGET THEN
        INSERT
            (
			 [StateAbbreviation]
    		,[StateName]
			,[CreatedDate]
			,[ModifiedDate]
            )
        VALUES
            (			
			 sourceTable.[StateAbbreviation]
    		,sourceTable.[StateName]	
			,GETDATE()
			,NULL
            ); 
END
GO

CREATE PROCEDURE [Stage].[StageToDimDate]
AS
BEGIN
;WITH sourceTable AS
(
SELECT 
	  DISTINCT 
	  ISNULL([DATA_YEAR],1900)	AS [Year]					
FROM [Stage].[HumanTrafficking]
)
MERGE INTO
        [Dim].[Date] targetTable
    USING
        sourceTable
            ON sourceTable.[Year] = targetTable.[Year]    
 WHEN NOT MATCHED BY TARGET THEN
        INSERT
            (
    		 [Year]
			,[CreatedDate]
			,[ModifiedDate]
            )
        VALUES
            (			
			 sourceTable.[Year]	
			,GETDATE()
			,NULL
            ); 
END
GO

CREATE PROCEDURE [Stage].[StageToFctHumanTrafficking]
AS
BEGIN
;WITH stageTable AS
(
	SELECT
	 HASHBYTES('SHA2_256',CAST(CONCAT(ISNULL([DATA_YEAR],1900),ISNULL(ORI,'Unknown'),ISNULL([STATE_ABBR],'Unknown'),ISNULL([POPULATION_GROUP_CODE],'Unknown'),ISNULL([OFFENSE_SUBCAT_ID],'Unknown')) AS NVARCHAR(100))) AS [SurrogateID]
	,[DATA_YEAR]				 
	,[ORI]					 
	,[PUB_AGENCY_NAME]		 
	,[PUB_AGENCY_UNIT]		 
	,[AGENCY_TYPE_NAME]		 
	,[STATE_ABBR]			 
	,[STATE_NAME]			 
	,[DIVISION_NAME]			 
	,[COUNTY_NAME]			 
	,[REGION_NAME]			 
	,[POPULATION_GROUP_CODE]  
	,[POPULATION_GROUP_DESC]  
	,[OFFENSE_SUBCAT_ID]		 
	,[OFFENSE_NAME]			 
	,[OFFENSE_SUBCAT_NAME]	 
	,SUM(CAST([ACTUAL_COUNT] AS INT)) AS [ACTUAL_COUNT]
	,SUM(CAST([UNFOUNDED_COUNT] AS INT))	AS [UNFOUNDED_COUNT]
	,SUM(CAST([CLEARED_COUNT] AS INT)) AS [CLEARED_COUNT]
	,SUM(CAST([JUVENILE_CLEARED_COUNT] AS INT)) AS [JUVENILE_CLEARED_COUNT]
	FROM [Stage].[HumanTrafficking]
	GROUP BY
	[DATA_YEAR]				 
	,[ORI]					 
	,[PUB_AGENCY_NAME]		 
	,[PUB_AGENCY_UNIT]		 
	,[AGENCY_TYPE_NAME]		 
	,[STATE_ABBR]			 
	,[STATE_NAME]			 
	,[DIVISION_NAME]			 
	,[COUNTY_NAME]			 
	,[REGION_NAME]			 
	,[POPULATION_GROUP_CODE]  
	,[POPULATION_GROUP_DESC]  
	,[OFFENSE_SUBCAT_ID]		 
	,[OFFENSE_NAME]			 
	,[OFFENSE_SUBCAT_NAME]
),
sourceTable AS
(
SELECT 
	  DISTINCT
	  S.[SurrogateID]
	 ,ISNULL(DT.[DateKey]				,-1) AS  [DateKey]				
	 ,ISNULL(PA.[PubAgencyKey]			,-1) AS  [PubAgencyKey]			
	 ,ISNULL([AT].AgencyTypeKey			,-1) AS  AgencyTypeKey			
	 ,ISNULL(D.[DivisionKey]			,-1) AS  [DivisionKey]			
	 ,ISNULL(C.[CountyKey]				,-1) AS  [CountyKey]				
	 ,ISNULL(R.[RegionKey]				,-1) AS  [RegionKey]				
	 ,ISNULL(ST.StateKey				,-1) AS  StateKey				
	 ,ISNULL(PG.[PopulationGroupKey]	,-1) AS  [PopulationGroupKey]	
	 ,ISNULL(OS.[OffenseSubcategoryKey]	,-1) AS  [OffenseSubcategoryKey]	
	 ,ISNULL(O.[OffenseKey]				,-1) AS  [OffenseKey]				
	 ,S.[ACTUAL_COUNT] AS [ActualCount]			
	 ,S.[UNFOUNDED_COUNT] AS [UnfoundedCount]		
	 ,S.CLEARED_COUNT AS [ClearedCount]			
	 ,S.JUVENILE_CLEARED_COUNT AS [JuvenileClearedCount]
FROM stageTable S
LEFT OUTER JOIN [Dim].[Offense] O ON O.OffenseName = S.OFFENSE_NAME
LEFT OUTER JOIN [Dim].[OffenseSubcategory] OS ON OS.OffenseSubcategoryCode = S.OFFENSE_SUBCAT_ID
LEFT OUTER JOIN [Dim].[PubAgency] PA ON PA.OriginatingAgencyIdentifier = S.ORI
LEFT OUTER JOIN [Dim].[AgencyType] [AT] ON [AT].AgencyTypeName = S.AGENCY_TYPE_NAME
LEFT OUTER JOIN [Dim].[State] ST ON ST.StateAbbreviation = S.STATE_ABBR
LEFT OUTER JOIN [Dim].[Division] D ON D.DivisionName = S.DIVISION_NAME
LEFT OUTER JOIN [Dim].[County] C ON C.CountyName = S.COUNTY_NAME
LEFT OUTER JOIN [Dim].[Date] DT ON DT.[Year] = S.DATA_YEAR
LEFT OUTER JOIN [Dim].[PopulationGroup] PG ON PG.PopulationGroupCode = S.POPULATION_GROUP_CODE
LEFT OUTER JOIN [Dim].[Region] R ON R.RegionName = S.REGION_NAME
)
MERGE INTO
        [Fct].[HumanTrafficking] targetTable
    USING
        sourceTable
            ON sourceTable.[SurrogateID] = targetTable.[SurrogateID]       
 WHEN MATCHED THEN
        UPDATE
        SET
		 targetTable.[DateKey]				 =	 sourceTable.[DateKey]				
		,targetTable.[PubAgencyKey]			 =	 sourceTable.[PubAgencyKey]				
		,targetTable.[AgencyTypeKey]		 =	 sourceTable.[AgencyTypeKey]		
		,targetTable.[DivisionKey]			 =	 sourceTable.[DivisionKey]			
		,targetTable.[CountyKey]			 =	 sourceTable.[CountyKey]				
		,targetTable.[RegionKey]			 =	 sourceTable.[RegionKey]				
		,targetTable.[StateKey]				 =	 sourceTable.[StateKey]				
		,targetTable.[PopulationGroupKey]	 =	 sourceTable.[PopulationGroupKey]	
		,targetTable.[OffenseSubcategoryKey] =	 sourceTable.[OffenseSubcategoryKey]	
		,targetTable.[OffenseKey]			 =	 sourceTable.[OffenseKey]			
		,targetTable.[ActualCount]			 =	 sourceTable.[ActualCount]			
		,targetTable.[UnfoundedCount]		 =	 sourceTable.[UnfoundedCount]		
		,targetTable.[ClearedCount]			 =	 sourceTable.[ClearedCount]			
		,targetTable.[JuvenileClearedCount]	 =	 sourceTable.[JuvenileClearedCount]			
		,targetTable.[ModifiedDate]			 =	 GETDATE()
 WHEN NOT MATCHED BY TARGET THEN
        INSERT
            (
			 [SurrogateID]
			,[DateKey]				
			,[PubAgencyKey]	
			,[AgencyTypeKey]
			,[DivisionKey]			
			,[CountyKey]				
			,[RegionKey]		
			,[StateKey]
			,[PopulationGroupKey]	
			,[OffenseSubcategoryKey]
			,[OffenseKey]			
			,[ActualCount]			
			,[UnfoundedCount]		
			,[ClearedCount]			
			,[JuvenileClearedCount]
			,[CreatedDate]
			,[ModifiedDate]
            )
        VALUES
            (	
			 sourceTable.[SurrogateID]
			,sourceTable.[DateKey]				
			,sourceTable.[PubAgencyKey]		
			,sourceTable.[AgencyTypeKey]
			,sourceTable.[DivisionKey]			
			,sourceTable.[CountyKey]				
			,sourceTable.[RegionKey]	
			,sourceTable.[StateKey]
			,sourceTable.[PopulationGroupKey]	
			,sourceTable.[OffenseSubcategoryKey]
			,sourceTable.[OffenseKey]			
			,sourceTable.[ActualCount]			
			,sourceTable.[UnfoundedCount]		
			,sourceTable.[ClearedCount]			
			,sourceTable.[JuvenileClearedCount]
			,GETDATE()
			,NULL
            ); 
END
GO


--Create views for Model
CREATE VIEW Dimension.AgencyType
AS
SELECT * FROM Dim.AgencyType
GO

CREATE VIEW Dimension.County
AS
SELECT * FROM Dim.County
GO

CREATE VIEW Dimension.[Date]
AS
SELECT * FROM Dim.[Date]
GO

CREATE VIEW Dimension.[Division]
AS
SELECT * FROM Dim.[Division]
GO

CREATE VIEW Dimension.[Offense]
AS
SELECT * FROM Dim.[Offense]
GO

CREATE VIEW Dimension.[OffenseSubcategory]
AS
SELECT * FROM Dim.[OffenseSubcategory]
GO

CREATE VIEW Dimension.[PopulationGroup]
AS
SELECT * FROM Dim.[PopulationGroup]
GO

CREATE VIEW Dimension.[PubAgency]
AS
SELECT * FROM Dim.[PubAgency]
GO

CREATE VIEW Dimension.[Region]
AS
SELECT * FROM Dim.[Region]
GO

CREATE VIEW Dimension.[State]
AS
SELECT * FROM Dim.[State]
GO

CREATE VIEW Fact.[HumanTrafficking]
AS
SELECT * FROM Fct.[HumanTrafficking]
GO

--Execute Transform Stored Procedures
EXEC [Stage].[StageToDimAgencyType]
EXEC [Stage].[StageToDimCounty]
EXEC [Stage].[StageToDimDate]
EXEC [Stage].[StageToDimDivision]
EXEC [Stage].[StageToDimOffense]
EXEC [Stage].[StageToDimOffenseSubcategory]
EXEC [Stage].[StageToDimPopulationGroup]
EXEC [Stage].[StageToDimPubAgency]
EXEC [Stage].[StageToDimRegion]
EXEC [Stage].[StageToDimState]
EXEC [Stage].[StageToFctHumanTrafficking]
