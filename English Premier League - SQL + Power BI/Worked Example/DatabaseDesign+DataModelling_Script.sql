/**** This example has been worked on datasets obtained from the following sources:
--https://www.kaggle.com/martj42/international-football-results-from-1872-to-2017
--https://www.kaggle.com/devinharia/epl-dataset
****/

--Create FuzzyMatching function - referencing and adopting some logic for this function from: https://stackoverflow.com/questions/26259117/sql-server-fuzzy-search-with-percentage-of-match
CREATE FUNCTION [System].[FuzzyMatchWords] (
	@Reference VARCHAR(200)
	,@Target VARCHAR(200)
	)
RETURNS TABLE
AS
RETURN (
		WITH N(n) AS (
				SELECT TOP (
						ISNULL(CASE 
								WHEN DATALENGTH(@Reference) > DATALENGTH(@Target)
									THEN DATALENGTH(@Reference)
								ELSE DATALENGTH(@Target)
								END, 0)
						) ROW_NUMBER() OVER (
						ORDER BY n1.n
						)
				FROM (
					VALUES (1)
						,(1)
						,(1)
						,(1)
						,(1)
						,(1)
						,(1)
						,(1)
						,(1)
						,(1)
					) AS N1(n)
				CROSS JOIN (
					VALUES (1)
						,(1)
						,(1)
						,(1)
						,(1)
						,(1)
						,(1)
						,(1)
						,(1)
						,(1)
					) AS N2(n)
				CROSS JOIN (
					VALUES (1)
						,(1)
					) AS N3(n)
				WHERE @Reference IS NOT NULL
					AND @Target IS NOT NULL
				)
			,Src AS (
				SELECT Reference = CASE 
						WHEN DATALENGTH(@Reference) > DATALENGTH(@Target)
							THEN @Reference
						ELSE @Reference + REPLICATE('_', DATALENGTH(@Target) - DATALENGTH(@Reference))
						END
					,Target = CASE 
						WHEN DATALENGTH(@Target) > DATALENGTH(@Reference)
							THEN @Target
						ELSE @Target + REPLICATE('_', DATALENGTH(@Target) - DATALENGTH(@Reference))
						END
					,WordLength = CASE 
						WHEN DATALENGTH(@Reference) > DATALENGTH(@Target)
							THEN DATALENGTH(@Reference)
						ELSE DATALENGTH(@Target)
						END
				WHERE @Reference IS NOT NULL
					AND @Target IS NOT NULL
					AND @Reference != @Target
				)
			,Scores AS (
				SELECT seq = t1.n
					,Letter = SUBSTRING(s.Reference, t1.n, 1)
					,s.WordLength
					,LetterScore = s.WordLength - ISNULL(MIN(ABS(t1.n - t2.n)), s.WordLength)
				FROM Src AS s
				CROSS JOIN N AS t1
				INNER JOIN N AS t2 ON SUBSTRING(@Target, t2.n, 1) = SUBSTRING(s.Reference, t1.n, 1)
				WHERE @Reference IS NOT NULL
					AND @Target IS NOT NULL
					AND @Reference != @Target
				GROUP BY t1.n
					,SUBSTRING(s.Reference, t1.n, 1)
					,s.WordLength
				)
		SELECT [Score] = 100
		WHERE @Reference = @Target
		
		UNION ALL
		
		SELECT 0
		WHERE @Reference IS NULL
			OR @Target IS NULL
		
		UNION ALL
		
		SELECT CAST(SUM(LetterScore) * 100.0 / MAX(WordLength * WordLength) AS NUMERIC(5, 2))
		FROM Scores
		WHERE @Reference IS NOT NULL
			AND @Target IS NOT NULL
			AND @Reference != @Target
		GROUP BY WordLength
		);
GO

--Creating schemas to be used
CREATE SCHEMA [Stage] AUTHORIZATION [dbo]
GO

CREATE SCHEMA [Clean] AUTHORIZATION [dbo]
GO

CREATE SCHEMA [Warehouse] AUTHORIZATION [dbo]
GO

CREATE SCHEMA [System] AUTHORIZATION [dbo]
GO

CREATE SCHEMA [Dimension] AUTHORIZATION [dbo]
GO

CREATE SCHEMA [Fact] AUTHORIZATION [dbo]
GO

--Create landing Stage data
CREATE TABLE [Stage].[FootballData] (
	MatchDate VARCHAR(50) NULL
	,HomeTeam VARCHAR(150) NULL
	,AwayTeam VARCHAR(150) NULL
	,FullTimeHomeTeamGoals INT NULL
	,FullTimeAwayTeamGoals INT NULL
	,FullTimeResult VARCHAR(5) NULL
	,-- (H=HomeWin,D=Draw,A=AwayWin)
	HalfTimeHomeTeamGoals INT NULL
	,HalfTimeAwayTeamGoals INT NULL
	,HalfTimeResult VARCHAR(5) NULL
	,-- (H=HomeWin,D=Draw,A=AwayWin)
	Referee VARCHAR(500) NULL
	,HomeTeamShots INT NULL
	,AwayTeamShots INT NULL
	,HomeTeamShotsOnTarget INT NULL
	,AwayTeamShotsOnTarget INT NULL
	,HomeTeamFoulsCommitted INT NULL
	,AwayTeamFoulsCommitted INT NULL
	,HomeTeamCorners INT NULL
	,AwayTeamCorners INT NULL
	,HomeTeamYellowCards INT NULL
	,AwayTeamYellowCards INT NULL
	,HomeTeamRedCards INT NULL
	,AwayTeamRedCards INT NULL
	,Season VARCHAR(10) NULL
	)
GO

CREATE TABLE [Stage].[RefereeList] (RefereeName VARCHAR(100) NULL)
GO

CREATE TABLE [Warehouse].[DimReferee] (
	RefereeId INT NULL
	,RefereeName VARCHAR(100) NULL
	,RefereeSurname VARCHAR(100) NULL
	)
GO

--Create Clean data
CREATE TABLE [Clean].[FootballData] (
	RowId INT NOT NULL
	,MatchDate VARCHAR(50) NULL
	,HomeTeam VARCHAR(100) NULL
	,HomeTeamOriginLocation VARCHAR(150) NULL
	,AwayTeam VARCHAR(100) NULL
	,AwayTeamOriginLocation VARCHAR(150) NULL
	,FullTimeHomeTeamGoals INT NULL
	,FullTimeAwayTeamGoals INT NULL
	,FullTimeResult VARCHAR(5) NULL -- (H=HomeWin,D=Draw,A=AwayWin)
	,HalfTimeHomeTeamGoals INT NULL
	,HalfTimeAwayTeamGoals INT NULL
	,HalfTimeResult VARCHAR(5) NULL -- (H=HomeWin,D=Draw,A=AwayWin)
	,Referee VARCHAR(500) NULL
	,HomeTeamShots INT NULL
	,AwayTeamShots INT NULL
	,HomeTeamShotsOnTarget INT NULL
	,AwayTeamShotsOnTarget INT NULL
	,HomeTeamFoulsCommitted INT NULL
	,AwayTeamFoulsCommitted INT NULL
	,HomeTeamCorners INT NULL
	,AwayTeamCorners INT NULL
	,HomeTeamYellowCards INT NULL
	,AwayTeamYellowCards INT NULL
	,HomeTeamRedCards INT NULL
	,AwayTeamRedCards INT NULL
	,Season VARCHAR(10) NULL
	)
GO

CREATE TABLE [Clean].[Referee] (
	NAME VARCHAR(50) NULL
	,SURNAME VARCHAR(50) NULL
	)
GO

CREATE TABLE [Warehouse].[DimReferee] (
	[RefereeId] INT IDENTITY(1, 1) NOT NULL
	,[RefereeName] VARCHAR(50) NULL
	,[RefereeSurname] VARCHAR(50) NULL
	,
	)
GO

CREATE TABLE [Warehouse].[DimTeam] (
	[TeamId] INT IDENTITY(1, 1) NOT NULL
	,[Team] VARCHAR(100) NULL
	,[TeamOriginLocation] VARCHAR(150) NULL
	)
GO

CREATE TABLE [Warehouse].[DimResult] (
	[ResultId] INT IDENTITY(1, 1) NOT NULL
	,[ResultCode] VARCHAR(1) NULL
	,[Result] VARCHAR(10) NULL
	)
GO

CREATE TABLE [Warehouse].[DimHomeAway] (
	[HomeAwayId] INT IDENTITY(1, 1) NOT NULL
	,[HomeAwayCode] VARCHAR(1) NULL
	,[HomeAway] VARCHAR(10) NULL
	)
GO

CREATE TABLE [Warehouse].[DimDate] (
	DateId INT IDENTITY(1, 1) NOT NULL
	,CalendarDate DATE NULL
	,CalendarYear INT NULL
	,CalendarMonth INT NULL
	,CalendarDay INT NULL
	,CalendarWeekDay INT NULL
	,CalendarMonthName VARCHAR(20) NULL
	,CalendarWeekDayName VARCHAR(20) NULL
	,Season VARCHAR(7) NULL
	,SeasonStartDate DATE NULL
	,SeasonFinishDate DATE NULL
	,IsMatchDate BIT NULL
	)
GO

--Create Warehouse data
CREATE TABLE [Warehouse].[FactFootballData] (
	[FootballDataId] INT IDENTITY(1, 1) NOT NULL
	,[MatchId] INT NULL
	,[MatchDateId] INT NULL
	,[TeamId] INT NULL
	,[HomeAwayId] INT NULL
	,[OppositionTeamId] INT NULL
	,[RefereeId] INT NULL
	,[HalfTimeResultId] INT NULL
	,[HalfTimePoints] INT NULL
	,[FullTimeResultId] INT NULL
	,[FullTimePoints] INT NULL
	,[HalfTimeGoals] INT NULL
	,[FullTimeGoals] INT NULL
	,[Corners] INT NULL
	,[FoulsCommitted] INT NULL
	,[Shots] INT NULL
	,[ShotsOnTarget] INT NULL
	,[YellowCards] INT NULL
	,[RedCards] INT NULL
	)
GO

/*Import Raw Data in Stage schema. Azure Data Factory has been used in this case - ARM Template exported.*/
--Data wrangling to get more consistent and accurate team names
IF OBJECT_ID('tempdb..#MappedTeamNames') IS NOT NULL
	DROP TABLE #MappedTeamNames
		--Clean Team Names 
		;

WITH Teams
AS (
	SELECT DISTINCT REPLACE(Team, LTRIM(RTRIM(SUBSTRING(Team, LEN(Team) - CHARINDEX(' ', REVERSE(Team)) + 1, LEN(Team)))), '') AS [CleanTeam]
		,[Team]
	FROM [Stage].[Teams]
		--WHERE [Team] NOT IN ('Manchester City', 'Manchester United', 'Sheffield Wednesday', 'Sheffield United')--,'West Brom', 'West Ham', 'Crystal Palace')
	)
	,TeamsList
AS (
	SELECT DISTINCT REPLACE(Team, LTRIM(RTRIM(SUBSTRING(Team, LEN(Team) - CHARINDEX(' ', REVERSE(Team)) + 1, LEN(Team)))), '') AS [CleanTeam]
		,[Team]
	FROM (
		SELECT [HomeTeam] AS [Team]
		FROM [Stage].[FootballData]
		
		UNION ALL
		
		SELECT [AwayTeam] AS [Team]
		FROM [Stage].[FootballData]
		) TeamsList
	WHERE [Team] IS NOT NULL --AND [Team] NOT IN ('Manchester City', 'Manchester United', 'Sheffield Wednesday', 'Sheffield United')--,'West Brom', 'West Ham', 'Crystal Palace')
	)
	,MatchTeams
AS (
	SELECT T.CleanTeam AS [FactCleanTeam]
		,T.Team AS [FactTeam]
		,TI.CleanTeam AS [SourceCleanTeam]
		,TI.Team AS [SourceTeam]
		,[Score]
	FROM TeamsList T
	CROSS APPLY Teams TI
	CROSS APPLY [System].[FuzzyMatchWords](t.[CleanTeam], TI.[CleanTeam])
	WHERE [Score] > 80
		AND T.Team NOT IN (
			'Sheffield Wednesday'
			,'Sheffield United'
			)
		--ORDER BY [Score] DESC
	)
	,MatchingResults
AS (
	SELECT T.Team AS [FactTeam]
		,ISNULL(M.SourceTeam, CASE 
				WHEN T.Team = 'Man City'
					THEN 'Manchester City'
				WHEN T.Team = 'Man United'
					THEN 'Manchester United'
				WHEN T.Team = 'Wolves'
					THEN 'Wolverhampton Wanderers'
				WHEN T.Team = 'QPR'
					THEN 'Queens Park Rangers'
				WHEN T.Team = 'West Ham'
					THEN 'West Ham United'
				WHEN T.Team = 'West Brom'
					THEN 'West Bromwich Albion'
				WHEN T.Team = 'Brighton'
					THEN 'Brighton & Hove Albion'
				WHEN T.Team = 'Sheffield United'
					THEN 'Sheffield United'
				WHEN T.Team = 'Sheffield Wednesday'
					THEN 'Sheffield Wednesday'
				END) AS [SourceTeam]
	FROM TeamsList T
	LEFT OUTER JOIN MatchTeams M ON T.Team = M.[FactTeam]
	)
SELECT *
INTO #MappedTeamNames
FROM MatchingResults

INSERT INTO [Clean].[FootballData]
SELECT ROW_NUMBER() OVER (
		ORDER BY [MatchDate]
			,[HomeTeam]
			,[AwayTeam]
		) AS [RowId]
	,STUFF('2000', 3, 4, RIGHT([MatchDate], 2)) + '-' + SUBSTRING([MatchDate], 4, 2) + '-' + LEFT([MatchDate], 2) AS [MatchDate]
	,STH.Team AS HomeTeam
	,STH.[Origin Location] AS HomeTeamOriginLocation
	,STA.Team AS AwayTeam
	,STA.[Origin Location] AS AwayTeamOriginLocation
	,FullTimeHomeTeamGoals
	,FullTimeAwayTeamGoals
	,FullTimeResult
	,HalfTimeHomeTeamGoals
	,HalfTimeAwayTeamGoals
	,HalfTimeResult
	,Referee
	,HomeTeamShots
	,AwayTeamShots
	,HomeTeamShotsOnTarget
	,AwayTeamShotsOnTarget
	,HomeTeamFoulsCommitted
	,AwayTeamFoulsCommitted
	,HomeTeamCorners
	,AwayTeamCorners
	,HomeTeamYellowCards
	,AwayTeamYellowCards
	,HomeTeamRedCards
	,AwayTeamRedCards
	,Season
FROM [Stage].[FootballData] SFD
LEFT OUTER JOIN #MappedTeamNames MTA ON MTA.[FactTeam] = SFD.[AwayTeam]
LEFT OUTER JOIN [Stage].[Teams] STA ON STA.[Team] = MTA.[SourceTeam]
LEFT OUTER JOIN #MappedTeamNames MTH ON MTH.[FactTeam] = SFD.[HomeTeam]
LEFT OUTER JOIN [Stage].[Teams] STH ON STH.[Team] = MTH.[SourceTeam]
WHERE [MatchDate] IS NOT NULL
GO

--Create Teams Dimension
MERGE [Warehouse].[DimTeam] AS [Target]
USING (
	SELECT TOP 100 PERCENT [Team]
		,[TeamOriginLocation]
	FROM (
		SELECT 'Unknown' AS [Team]
			,'Unknown' AS [TeamOriginLocation]
			,0 AS [Sort]
		
		UNION ALL
		
		SELECT DISTINCT [Team] AS [Team]
			,[TeamOriginLocation]
			,1 AS [Sort]
		FROM (
			SELECT [HomeTeam] AS [Team]
				,[HomeTeamOriginLocation] AS [TeamOriginLocation]
			FROM [Clean].[FootballData]
			
			UNION ALL
			
			SELECT [AwayTeam] AS [Team]
				,[AwayTeamOriginLocation] AS [TeamOriginLocation]
			FROM [Clean].[FootballData]
			) A
		WHERE [Team] IS NOT NULL
		) TeamsDim
	ORDER BY [Sort]
		,[Team]
	) AS [Source]([Team], [TeamOriginLocation])
	ON [Source].[Team] = [Target].[Team]
WHEN NOT MATCHED BY TARGET
	THEN
		INSERT (
			[Team]
			,[TeamOriginLocation]
			)
		VALUES (
			[Source].[Team]
			,[Source].[TeamOriginLocation]
			)
WHEN NOT MATCHED BY SOURCE
	THEN
		DELETE;

--Create Results Dimension
MERGE [Warehouse].[DimResult] AS [Target]
USING (
	SELECT TOP 100 PERCENT [Code] AS [ResultCode]
		,[Result]
	FROM (
		SELECT 'Unknown' AS [Result]
			,'U' AS [Code]
			,0 AS [Sort]
		
		UNION ALL
		
		SELECT 'Home' AS [Result]
			,'H' AS [Code]
			,1 AS [Sort]
		
		UNION ALL
		
		SELECT 'Draw' AS [Result]
			,'D' AS [Code]
			,2 AS [Sort]
		
		UNION ALL
		
		SELECT 'Away' AS [Result]
			,'A' AS [Code]
			,3 AS [Sort]
		) ResultsDim
	ORDER BY [Sort]
	) AS [Source]([ResultCode], [Result])
	ON [Target].[ResultCode] = [Source].[ResultCode]
WHEN NOT MATCHED BY TARGET
	THEN
		INSERT (
			[ResultCode]
			,[Result]
			)
		VALUES (
			[Source].[ResultCode]
			,[Source].[Result]
			)
WHEN NOT MATCHED BY SOURCE
	THEN
		DELETE;
GO

--Create HomeAway Dimension
MERGE [Warehouse].[DimHomeAway] AS [Target]
USING (
	SELECT TOP 100 PERCENT [HomeAwayCode]
		,[HomeAway]
	FROM (
		SELECT 'Unknown' AS [HomeAway]
			,'U' AS [HomeAwayCode]
			,0 AS [Sort]
		
		UNION ALL
		
		SELECT 'Home' AS [HomeAway]
			,'H' AS [HomeAwayCode]
			,1 AS [Sort]
		
		UNION ALL
		
		SELECT 'Away' AS [HomeAway]
			,'A' AS [HomeAwayCode]
			,2 AS [Sort]
		) HomeAwayDim
	ORDER BY [Sort]
	) AS [Source]([HomeAwayCode], [HomeAway])
	ON [Target].[HomeAwayCode] = [Source].[HomeAwayCode]
WHEN NOT MATCHED BY TARGET
	THEN
		INSERT (
			[HomeAwayCode]
			,[HomeAway]
			)
		VALUES (
			[Source].[HomeAwayCode]
			,[Source].[HomeAway]
			)
WHEN NOT MATCHED BY SOURCE
	THEN
		DELETE;
GO

--Clean Referee ingested list and save in Clean layer
	;

WITH SourceCTE
AS (
	SELECT LTRIM(RTRIM(SUBSTRING([RefereeName], 0, LEN([RefereeName]) - CHARINDEX(' ', REVERSE([RefereeName])) + 1))) AS [Name]
		,LTRIM(RTRIM(SUBSTRING([RefereeName], LEN([RefereeName]) - CHARINDEX(' ', REVERSE([RefereeName])) + 1, LEN([RefereeName])))) AS [Surname]
	FROM [Stage].[RefereeList]
	WHERE [RefereeName] <> 'sum'
	)
INSERT INTO [Clean].[Referee]
SELECT *
FROM SourceCTE

--Populate Referee Dimension from Clean layer
MERGE [Warehouse].[DimReferee] AS [Target]
USING (
	SELECT TOP 100 PERCENT [Name] AS [RefereeName]
		,[Surname] AS [RefereeSurname]
	FROM (
		SELECT 'Unknown' AS [Name]
			,'Unknown' AS [Surname]
			,0 AS [SortOrder]
		
		UNION ALL
		
		SELECT [Name]
			,[Surname]
			,1 AS [SortOrder]
		FROM [Clean].[Referee]
		) A
	ORDER BY [SortOrder]
		,[Surname]
	) AS [Source]([RefereeName], [RefereeSurname])
	ON [Source].[RefereeName] = [Target].[RefereeName]
		AND [Source].[RefereeSurname] = [Target].[RefereeSurname]
WHEN NOT MATCHED BY TARGET
	THEN
		INSERT (
			[RefereeName]
			,[RefereeSurname]
			)
		VALUES (
			[Source].[RefereeName]
			,[Source].[RefereeSurname]
			)
WHEN NOT MATCHED BY SOURCE
	THEN
		DELETE;
GO

--Create Dim Date
IF OBJECT_ID('tempdb..#Date') IS NOT NULL
	DROP TABLE #Date

CREATE TABLE #Date (
	CalendarDate DATE
	,CalendarYear INT
	,CalendarMonth INT
	,CalendarDay INT
	,CalendarWeekDay INT
	,CalendarMonthName VARCHAR(20)
	,CalendarWeekDayName VARCHAR(20)
	)

DECLARE @start DATE = '2000-01-01'

WHILE @start < GETDATE()
BEGIN
	INSERT INTO #Date (
		CalendarDate
		,CalendarYear
		,CalendarMonth
		,CalendarDay
		,CalendarWeekDay
		,CalendarMonthName
		,CalendarWeekDayName
		)
	VALUES (
		@start
		,DATEPART(YY, @start)
		,DATEPART(mm, @start)
		,DATEPART(dd, @start)
		,DATEPART(dw, @start)
		,DATENAME(mm, @start)
		,DATENAME(dw, @start)
		)

	SET @start = DATEADD(dd, 1, @start)
END;

WITH FootballSeasonData
AS (
	SELECT [Season]
		,MIN([MatchDate]) AS [SeasonStartDate]
		,MAX([MatchDate]) AS [SeasonFinishDate]
	FROM [Clean].[FootballData]
	GROUP BY [Season]
	)
MERGE [Warehouse].[DimDate] AS [Target]
USING (
	SELECT TOP 100 PERCENT CalendarDate
		,CalendarYear
		,CalendarMonth
		,CalendarDay
		,CalendarWeekDay
		,CalendarMonthName
		,CalendarWeekDayName
		,Season
		,SeasonStartDate
		,SeasonFinishDate
		,IsMatchDate
	FROM (
		SELECT DISTINCT CalendarDate AS CalendarDate
			,CalendarYear AS CalendarYear
			,CalendarMonth AS CalendarMonth
			,CalendarDay AS CalendarDay
			,CalendarWeekDay AS CalendarWeekDay
			,CalendarMonthName AS CalendarMonthName
			,CalendarWeekDayName AS CalendarWeekDayName
			,ISNULL(F.Season, 'Unknown') AS Season
			,ISNULL(F.SeasonStartDate, '1900-01-01') AS SeasonStartDate
			,ISNULL(F.SeasonFinishDate, '1900-01-01') AS SeasonFinishDate
			,CASE 
				WHEN CM.MatchDate IS NOT NULL
					THEN 1
				ELSE 0
				END AS IsMatchDate
			,[SortOrder]
		FROM (
			SELECT '1900-01-01' AS CalendarDate
				,'1900' AS CalendarYear
				,'01' AS CalendarMonth
				,'01' AS CalendarDay
				,1 AS CalendarWeekDay
				,'Unknown' AS CalendarMonthName
				,'Unknown' AS CalendarWeekDayName
				,0 AS [SortOrder]
			
			UNION ALL
			
			SELECT *
				,1 AS [SortOrder]
			FROM #Date
			) D
		LEFT OUTER JOIN FootballSeasonData F ON D.CalendarDate BETWEEN F.[SeasonStartDate]
				AND F.[SeasonFinishDate]
		LEFT OUTER JOIN (
			SELECT MatchDate
			FROM [Clean].[FootballData]
			GROUP BY MatchDate
			) CM ON CM.MatchDate = D.CalendarDate
		) A
	ORDER BY [SortOrder]
		,[CalendarDate]
	) AS [Source](CalendarDate, CalendarYear, CalendarMonth, CalendarDay, CalendarWeekDay, CalendarMonthName, CalendarWeekDayName, Season, SeasonStartDate, SeasonFinishDate, IsMatchDate)
	ON [Source].CalendarDate = [Target].CalendarDate
WHEN MATCHED
	THEN
		UPDATE
		SET [Target].Season = [Source].Season
			,[Target].SeasonStartDate = [Source].SeasonStartDate
			,[Target].SeasonFinishDate = [Source].SeasonFinishDate
			,[Target].IsMatchDate = [Source].IsMatchDate
WHEN NOT MATCHED BY TARGET
	THEN
		INSERT (
			CalendarDate
			,CalendarYear
			,CalendarMonth
			,CalendarDay
			,CalendarWeekDay
			,CalendarMonthName
			,CalendarWeekDayName
			,Season
			,SeasonStartDate
			,SeasonFinishDate
			,IsMatchDate
			)
		VALUES (
			[Source].CalendarDate
			,[Source].CalendarYear
			,[Source].CalendarMonth
			,[Source].CalendarDay
			,[Source].CalendarWeekDay
			,[Source].CalendarMonthName
			,[Source].CalendarWeekDayName
			,[Source].Season
			,[Source].SeasonStartDate
			,[Source].SeasonFinishDate
			,[Source].IsMatchDate
			)
WHEN NOT MATCHED BY SOURCE
	THEN
		DELETE;

--Fact Population
--Data wrangling to get more consistent and accurate referee name
IF OBJECT_ID('tempdb..#RefereeLinkNameSurname') IS NOT NULL
	DROP TABLE #RefereeLinkNameSurname

SELECT F.*
	,C.NAME AS [CleanName]
	,C.SURNAME AS [CleanSurname]
INTO #RefereeLinkNameSurname
FROM [Clean].[FootballData] F
LEFT OUTER JOIN [Clean].[Referee] C ON F.Referee = C.NAME + ' ' + C.SURNAME;

WITH DeDuplicatedInitials
AS (
	SELECT *
		,LEFT(NAME, 1) + ' ' + SURNAME AS [RefereeName]
	FROM [Clean].[Referee] R
	WHERE LEFT(R.NAME, 1) + ' ' + R.SURNAME IN (
			SELECT LEFT(C.NAME, 1) + ' ' + C.SURNAME AS [RefereeName]
			FROM [Clean].[Referee] C
			GROUP BY LEFT(C.NAME, 1) + ' ' + C.SURNAME
			HAVING COUNT(*) = 1
			)
	)
UPDATE #RefereeLinkNameSurname
SET [CleanName] = ISNULL([CleanName], C.NAME)
	,[CleanSurname] = ISNULL([CleanSurname], C.SURNAME)
FROM #RefereeLinkNameSurname S
LEFT OUTER JOIN DeDuplicatedInitials C ON REPLACE(S.Referee, '?', '') = C.[RefereeName]
	AND S.[CleanName] IS NULL
WHERE S.CleanName IS NULL
GO

;

WITH DeDuplicatedSurnames
AS (
	SELECT *
		,LEFT(NAME, 1) + ' ' + SURNAME AS [RefereeName]
	FROM [Clean].[Referee] R
	WHERE LEFT(R.NAME, 1) + ' ' + R.SURNAME IN (
			SELECT C.SURNAME
			FROM [Clean].[Referee] C
			GROUP BY C.SURNAME
			HAVING COUNT(*) = 1
			)
	)
UPDATE #RefereeLinkNameSurname
SET [CleanName] = ISNULL([CleanName], C.NAME)
	,[CleanSurname] = ISNULL([CleanSurname], C.SURNAME)
FROM #RefereeLinkNameSurname S
LEFT OUTER JOIN DeDuplicatedSurnames C ON REPLACE(S.Referee, '?', '') = C.[RefereeName]
	AND S.[CleanName] IS NULL
WHERE S.CleanName IS NULL
GO

--Fuzzy Match on Surname
UPDATE #RefereeLinkNameSurname
SET [CleanName] = ISNULL([CleanName], R.NAME)
	,[CleanSurname] = ISNULL([CleanSurname], R.SURNAME)
FROM #RefereeLinkNameSurname AS S
CROSS APPLY [Clean].[Referee] AS R
CROSS APPLY [System].FuzzyMatchWords(SURNAME, LTRIM(RTRIM(SUBSTRING([Referee], LEN([Referee]) - CHARINDEX(' ', REVERSE([Referee])) + 1, LEN([Referee]))))) AS f
WHERE [Score] > 80
	AND [CleanName] IS NULL
GO

--Fuzzy Match on Name initial and Surname
UPDATE #RefereeLinkNameSurname
SET [CleanName] = ISNULL(CF.[CleanName], R.NAME)
	,[CleanSurname] = ISNULL(CF.[CleanSurname], R.SURNAME)
FROM #RefereeLinkNameSurname CF
LEFT OUTER JOIN (
	SELECT *
		,RANK() OVER (
			PARTITION BY [RowId] ORDER BY [Score] DESC
			) AS [Rank]
	FROM #RefereeLinkNameSurname AS S
	CROSS APPLY [Clean].[Referee] AS R
	CROSS APPLY [System].FuzzyMatchWords(LEFT([NAME], 1) + SURNAME, LEFT([Referee], 1) + LTRIM(RTRIM(SUBSTRING([Referee], LEN([Referee]) - CHARINDEX(' ', REVERSE([Referee])) + 1, LEN([Referee]))))) AS f
	WHERE [CleanName] IS NULL
		AND [MatchDate] IS NOT NULL
	) R ON CF.RowId = R.[RowId]
WHERE [Rank] = 1
	AND [Score] >= 80
GO

--Check if columns already exist in Fact table, if not add as NULL
IF NOT EXISTS (
		SELECT *
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = 'Clean'
			AND TABLE_NAME = 'FootballData'
			AND COLUMN_NAME = 'RefereeName_Cleaned'
		)
BEGIN
	ALTER TABLE [Clean].[FootballData] ADD RefereeName_Cleaned VARCHAR(150) NULL
END;

IF NOT EXISTS (
		SELECT *
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = 'Clean'
			AND TABLE_NAME = 'FootballData'
			AND COLUMN_NAME = 'RefereeSurname_Cleaned'
		)
BEGIN
	ALTER TABLE [Clean].[FootballData] ADD RefereeSurname_Cleaned VARCHAR(150) NULL
END;

--Update Clean.FootballData with cleaned Referee Name and Surname to be used for linking Referee Data
UPDATE FFD
SET FFD.RefereeName_Cleaned = RS.CleanName
	,FFD.RefereeSurname_Cleaned = RS.CleanSurname
FROM [Clean].[FootballData] FFD
LEFT OUTER JOIN #RefereeLinkNameSurname RS ON FFD.RowId = RS.RowId

MERGE [Warehouse].[FactFootballData] AS [Target]
USING (
	SELECT TOP 100 PERCENT HASHBYTES('SHA2_256', CONVERT(VARCHAR(20), [MatchId]) + CONVERT(VARCHAR(20), [MatchDateId]) + CONVERT(VARCHAR(20), [TeamId]) + CONVERT(VARCHAR(20), [HomeAwayId]) + CONVERT(VARCHAR(20), [OppositionTeamId]) + CONVERT(VARCHAR(20), [RefereeId]) + CONVERT(VARCHAR(20), [HalfTimeResultId]) + CONVERT(VARCHAR(20), [HalfTimePoints]) + CONVERT(VARCHAR(20), [FullTimeResultId]) + CONVERT(VARCHAR(20), [FullTimePoints]) + CONVERT(VARCHAR(20), [HalfTimeGoals]) + CONVERT(VARCHAR(20), [FullTimeGoals]) + CONVERT(VARCHAR(20), [Corners]) + CONVERT(VARCHAR(20), [FoulsCommitted]) + CONVERT(VARCHAR(20), [Shots]) + CONVERT(VARCHAR(20), [ShotsOnTarget]) + CONVERT(VARCHAR(20), [YellowCards]) + CONVERT(VARCHAR(20), [RedCards])) AS [HashKey]
		,[MatchId]
		,[MatchDateId]
		,[TeamId]
		,[HomeAwayId]
		,[OppositionTeamId]
		,[RefereeId]
		,[HalfTimeResultId]
		,[HalfTimePoints]
		,[FullTimeResultId]
		,[FullTimePoints]
		,[HalfTimeGoals]
		,[FullTimeGoals]
		,[Corners]
		,[FoulsCommitted]
		,[Shots]
		,[ShotsOnTarget]
		,[YellowCards]
		,[RedCards]
	FROM (
		--Home Team Data
		SELECT FFD.[RowId] AS [MatchId]
			,ISNULL(DD.[DateId], (
					SELECT TOP 1 [DateId]
					FROM [Warehouse].[DimDate]
					WHERE [CalendarYear] = '1900'
					)) AS [MatchDateId]
			,ISNULL(TH.[TeamId], (
					SELECT TOP 1 [TeamId]
					FROM [Warehouse].[DimTeam]
					WHERE [Team] = 'Unknown'
					)) AS [TeamId]
			,(
				SELECT TOP 1 [HomeAwayId]
				FROM [Warehouse].[DimHomeAway]
				WHERE [HomeAwayCode] = 'H'
				) AS [HomeAwayId]
			,ISNULL(TA.[TeamId], (
					SELECT TOP 1 [TeamId]
					FROM [Warehouse].[DimTeam]
					WHERE [Team] = 'Unknown'
					)) AS [OppositionTeamId]
			,ISNULL(R.RefereeId, (
					SELECT TOP 1 [RefereeId]
					FROM [Warehouse].[DimReferee]
					WHERE [RefereeName] = 'Unknown'
					)) AS [RefereeId]
			,ISNULL(DHR.ResultId, (
					SELECT TOP 1 [ResultId]
					FROM [Warehouse].[DimResult]
					WHERE [ResultCode] = 'U'
					)) AS [HalfTimeResultId]
			,CASE 
				WHEN DHR.[ResultCode] = 'H'
					THEN 3
				WHEN DHR.[ResultCode] = 'A'
					THEN 0
				WHEN DHR.[ResultCode] = 'D'
					THEN 1
				END AS [HalfTimePoints]
			,ISNULL(DFR.ResultId, (
					SELECT TOP 1 [ResultId]
					FROM [Warehouse].[DimResult]
					WHERE [ResultCode] = 'U'
					)) AS FullTimeResultId
			,CASE 
				WHEN DFR.[ResultCode] = 'H'
					THEN 3
				WHEN DFR.[ResultCode] = 'A'
					THEN 0
				WHEN DFR.[ResultCode] = 'D'
					THEN 1
				END AS [FullTimePoints]
			,FFD.HalfTimeHomeTeamGoals AS [HalfTimeGoals]
			,FFD.[FullTimeHomeTeamGoals] AS [FullTimeGoals]
			,FFD.HomeTeamCorners AS [Corners]
			,FFD.HomeTeamFoulsCommitted AS [FoulsCommitted]
			,FFD.HomeTeamShots AS [Shots]
			,FFD.HomeTeamShotsOnTarget AS [ShotsOnTarget]
			,FFD.HomeTeamYellowCards AS [YellowCards]
			,FFD.HomeTeamRedCards AS [RedCards]
		FROM [Clean].[FootballData] FFD
		LEFT OUTER JOIN [Warehouse].[DimReferee] R ON R.RefereeSurname = FFD.RefereeSurname_Cleaned
			AND R.RefereeName = FFD.RefereeName_Cleaned
		LEFT OUTER JOIN [Warehouse].[DimResult] DFR ON DFR.ResultCode = FFD.FullTimeResult
		LEFT OUTER JOIN [Warehouse].[DimResult] DHR ON DHR.ResultCode = FFD.HalfTimeResult
		LEFT OUTER JOIN [Warehouse].[DimDate] DD ON DD.CalendarDate = FFD.[MatchDate]
		LEFT OUTER JOIN [Warehouse].[DimTeam] TA ON FFD.[AwayTeam] = TA.Team
		LEFT OUTER JOIN [Warehouse].[DimTeam] TH ON FFD.[HomeTeam] = TH.Team
		
		UNION ALL
		
		--Away Team Data
		SELECT FFD.[RowId] AS [MatchId]
			,ISNULL(DD.[DateId], (
					SELECT TOP 1 [DateId]
					FROM [Warehouse].[DimDate]
					WHERE [CalendarYear] = '1900'
					)) AS [MatchDateId]
			,ISNULL(TA.[TeamId], (
					SELECT TOP 1 [TeamId]
					FROM [Warehouse].[DimTeam]
					WHERE [Team] = 'Unknown'
					)) AS [TeamId]
			,(
				SELECT TOP 1 [HomeAwayId]
				FROM [Warehouse].[DimHomeAway]
				WHERE [HomeAwayCode] = 'A'
				) AS [HomeAwayId]
			,ISNULL(TH.[TeamId], (
					SELECT TOP 1 [TeamId]
					FROM [Warehouse].[DimTeam]
					WHERE [Team] = 'Unknown'
					)) AS [OppositionTeamId]
			,ISNULL(R.RefereeId, (
					SELECT TOP 1 [RefereeId]
					FROM [Warehouse].[DimReferee]
					WHERE [RefereeName] = 'Unknown'
					)) AS [RefereeId]
			,ISNULL(DHR.ResultId, (
					SELECT TOP 1 [ResultId]
					FROM [Warehouse].[DimResult]
					WHERE [ResultCode] = 'U'
					)) AS [HalfTimeResultId]
			,CASE 
				WHEN DHR.[ResultCode] = 'H'
					THEN 0
				WHEN DHR.[ResultCode] = 'A'
					THEN 3
				WHEN DHR.[ResultCode] = 'D'
					THEN 1
				END AS [HalfTimePoints]
			,ISNULL(DFR.ResultId, (
					SELECT TOP 1 [ResultId]
					FROM [Warehouse].[DimResult]
					WHERE [ResultCode] = 'U'
					)) AS FullTimeResultId
			,CASE 
				WHEN DFR.[ResultCode] = 'H'
					THEN 0
				WHEN DFR.[ResultCode] = 'A'
					THEN 3
				WHEN DFR.[ResultCode] = 'D'
					THEN 1
				END AS [FullTimePoints]
			,FFD.HalfTimeAwayTeamGoals AS [HalfTimeGoals]
			,FFD.[FullTimeAwayTeamGoals] AS [FullTimeGoals]
			,FFD.AwayTeamCorners AS [Corners]
			,FFD.AwayTeamFoulsCommitted AS [FoulsCommitted]
			,FFD.AwayTeamShots AS [Shots]
			,FFD.AwayTeamShotsOnTarget AS [ShotsOnTarget]
			,FFD.AwayTeamYellowCards AS [YellowCards]
			,FFD.AwayTeamRedCards AS [RedCards]
		FROM [Clean].[FootballData] FFD
		LEFT OUTER JOIN [Warehouse].[DimReferee] R ON R.RefereeSurname = FFD.RefereeSurname_Cleaned
			AND R.RefereeName = FFD.RefereeName_Cleaned
		LEFT OUTER JOIN [Warehouse].[DimResult] DFR ON DFR.ResultCode = FFD.FullTimeResult
		LEFT OUTER JOIN [Warehouse].[DimResult] DHR ON DHR.ResultCode = FFD.HalfTimeResult
		LEFT OUTER JOIN [Warehouse].[DimDate] DD ON DD.CalendarDate = FFD.[MatchDate]
		LEFT OUTER JOIN [Warehouse].[DimTeam] TA ON FFD.[AwayTeam] = TA.Team
		LEFT OUTER JOIN [Warehouse].[DimTeam] TH ON FFD.[HomeTeam] = TH.Team
		) FootballData
	ORDER BY [MatchDateId]
		,[MatchId]
	) AS [Source]([HashKey], [MatchId], [MatchDateId], [TeamId], [HomeAwayId], [OppositionTeamId], [RefereeId], [HalfTimeResultId], [HalfTimePoints], [FullTimeResultId], [FullTimePoints], [HalfTimeGoals], [FullTimeGoals], [Corners], [FoulsCommitted], [Shots], [ShotsOnTarget], [YellowCards], [RedCards])
	ON [Source].[HashKey] = HASHBYTES('SHA2_256', CONVERT(VARCHAR(20), [Target].[MatchId]) + CONVERT(VARCHAR(20), [Target].[MatchDateId]) + CONVERT(VARCHAR(20), [Target].[TeamId]) + CONVERT(VARCHAR(20), [Target].[HomeAwayId]) + CONVERT(VARCHAR(20), [Target].[OppositionTeamId]) + CONVERT(VARCHAR(20), [Target].[RefereeId]) + CONVERT(VARCHAR(20), [Target].[HalfTimeResultId]) + CONVERT(VARCHAR(20), [Target].[HalfTimePoints]) + CONVERT(VARCHAR(20), [Target].[FullTimeResultId]) + CONVERT(VARCHAR(20), [Target].[FullTimePoints]) + CONVERT(VARCHAR(20), [Target].[HalfTimeGoals]) + CONVERT(VARCHAR(20), [Target].[FullTimeGoals]) + CONVERT(VARCHAR(20), [Target].[Corners]) + CONVERT(VARCHAR(20), [Target].[FoulsCommitted]) + CONVERT(VARCHAR(20), [Target].[Shots]) + CONVERT(VARCHAR(20), [Target].[ShotsOnTarget]) + CONVERT(VARCHAR(20), [Target].[YellowCards]) + CONVERT(VARCHAR(20), [Target].[RedCards]))
WHEN NOT MATCHED BY TARGET
	THEN
		INSERT (
			[MatchId]
			,[MatchDateId]
			,[TeamId]
			,[HomeAwayId]
			,[OppositionTeamId]
			,[RefereeId]
			,[HalfTimeResultId]
			,[HalfTimePoints]
			,[FullTimeResultId]
			,[FullTimePoints]
			,[HalfTimeGoals]
			,[FullTimeGoals]
			,[Corners]
			,[FoulsCommitted]
			,[Shots]
			,[ShotsOnTarget]
			,[YellowCards]
			,[RedCards]
			)
		VALUES (
			[Source].[MatchId]
			,[Source].[MatchDateId]
			,[Source].[TeamId]
			,[Source].[HomeAwayId]
			,[Source].[OppositionTeamId]
			,[Source].[RefereeId]
			,[Source].[HalfTimeResultId]
			,[Source].[HalfTimePoints]
			,[Source].[FullTimeResultId]
			,[Source].[FullTimePoints]
			,[Source].[HalfTimeGoals]
			,[Source].[FullTimeGoals]
			,[Source].[Corners]
			,[Source].[FoulsCommitted]
			,[Source].[Shots]
			,[Source].[ShotsOnTarget]
			,[Source].[YellowCards]
			,[Source].[RedCards]
			)
WHEN NOT MATCHED BY SOURCE
	THEN
		DELETE;
GO

--Create Views to use for cube modelling
CREATE VIEW [Dimension].[Date]
AS
SELECT *
FROM [Warehouse].[DimDate]
GO

CREATE VIEW [Dimension].[Referee]
AS
SELECT *
FROM [Warehouse].[DimReferee]
GO

CREATE VIEW [Dimension].[FullTimeResult]
AS
SELECT [ResultId] AS [FTResultId]
	,[ResultCode] AS [FTResultCode]
	,[Result] AS [FTResult]
FROM [Warehouse].[DimResult]
GO

CREATE VIEW [Dimension].[HalfTimeResult]
AS
SELECT [ResultId] AS [HTResultId]
	,[ResultCode] AS [HTResultCode]
	,[Result] AS [HTResult]
FROM [Warehouse].[DimResult]
GO

CREATE VIEW [Dimension].[Team]
AS
SELECT *
FROM [Warehouse].[DimTeam]
GO

CREATE VIEW [Dimension].[OppositionTeam]
AS
SELECT TeamId AS [OppositionTeamId]
	,Team AS [OppositionTeam]
	,TeamOriginLocation AS [OppositionTeamOriginLocation]
FROM [Warehouse].[DimTeam]
GO

CREATE VIEW [Fact].[FootballData]
AS
SELECT *
FROM [Warehouse].[FactFootballData]
GO

CREATE VIEW [Dimension].[Match]
AS
SELECT *
FROM [Warehouse].[DimMatch]
GO

--On master database create a login named ReportingLogin, with a password:
--USE [master]
--CREATE LOGIN ReportingLogin WITH password='****';
--Create user based on login ReportingLogin
CREATE USER ReportingLogin
FROM LOGIN ReportingLogin
WITH DEFAULT_SCHEMA = [dbo];
GO

--Grant access only to fact/dim views
GRANT SELECT
	ON [Dimension].[Date]
	TO ReportingLogin;
GO

GRANT SELECT
	ON [Dimension].[Referee]
	TO ReportingLogin;
GO

GRANT SELECT
	ON [Dimension].[FullTimeResult]
	TO ReportingLogin;
GO

GRANT SELECT
	ON [Dimension].[HalfTimeResult]
	TO ReportingLogin;
GO

GRANT SELECT
	ON [Dimension].[Team]
	TO ReportingLogin;
GO

GRANT SELECT
	ON [Dimension].[OppositionTeam]
	TO ReportingLogin;
GO

GRANT SELECT
	ON [Dimension].[Match]
	TO ReportingLogin;
GO

GRANT SELECT
	ON [Fact].[FootballData]
	TO ReportingLogin;
GO

------Update Fact with Dimensions Ids
----MERGE [Warehouse].[FactFootballData] AS [Target]
----USING
----(
----SELECT DISTINCT
----	 HASHBYTES('SHA2_256', CONVERT(VARCHAR(100),DD.[DateId])+CONVERT(VARCHAR(100),TH.[TeamId])+CONVERT(VARCHAR(100),TA.[TeamId])+CONVERT(VARCHAR(100),F.FullTimeHomeTeamGoals)+CONVERT(VARCHAR(100),F.FullTimeAwayTeamGoals)+CONVERT(VARCHAR(100),F.FullTimeResult)+CONVERT(VARCHAR(100),F.HalfTimeHomeTeamGoals)+CONVERT(VARCHAR(100),F.HalfTimeAwayTeamGoals)+CONVERT(VARCHAR(100),F.HalfTimeResult)+CONVERT(VARCHAR(100),RefereeId)+CONVERT(VARCHAR(100),F.HomeTeamShots)+CONVERT(VARCHAR(100),F.AwayTeamShots)+CONVERT(VARCHAR(100),F.HomeTeamShotsOnTarget)+CONVERT(VARCHAR(100),F.AwayTeamShotsOnTarget)+CONVERT(VARCHAR(100),F.HomeTeamFoulsCommitted)+CONVERT(VARCHAR(100),F.AwayTeamFoulsCommitted)+CONVERT(VARCHAR(100),F.HomeTeamCorners)+CONVERT(VARCHAR(100),F.AwayTeamCorners)+CONVERT(VARCHAR(100),F.HomeTeamYellowCards)+CONVERT(VARCHAR(100),F.AwayTeamYellowCards)+CONVERT(VARCHAR(100),F.HomeTeamRedCards)+CONVERT(VARCHAR(100),F.AwayTeamRedCards)) AS HashKey
----	,ISNULL(DD.[DateId],(SELECT TOP 1 [DateId] FROM [Warehouse].[DimDate] WHERE [CalendarYear] = '1900' ) ) AS [MatchDateId]
----	,ISNULL(TH.[TeamId],(SELECT TOP 1 [TeamId] FROM [Warehouse].[DimTeam] WHERE [Team] = 'Unknown' ) ) AS [HomeTeamId]
----	,ISNULL(TA.[TeamId],(SELECT TOP 1 [TeamId] FROM [Warehouse].[DimTeam] WHERE [Team] = 'Unknown' ) ) AS [AwayTeamId]
----	,F.[FullTimeHomeTeamGoals]
----	,F.[FullTimeAwayTeamGoals]
----	,ISNULL(RTF.ResultId,(SELECT TOP 1 [RefereeId] FROM [Warehouse].[DimResult] WHERE [ResultCode] = 'U' ) ) AS FullTimeResultId
----	,F.[HalfTimeHomeTeamGoals]
----	,F.[HalfTimeAwayTeamGoals]
----	,ISNULL(RTH.ResultId,(SELECT TOP 1 [RefereeId] FROM [Warehouse].[DimResult] WHERE [ResultCode] = 'U' ) ) AS HalfTimeResultId
----	,ISNULL(R.RefereeId,(SELECT TOP 1 [RefereeId] FROM [Warehouse].[DimReferee] WHERE [RefereeName] = 'Unknown' ) ) AS RefereeId
----	,F.[HomeTeamShots]
----	,F.[AwayTeamShots]
----	,F.[HomeTeamShotsOnTarget]
----	,F.[AwayTeamShotsOnTarget]
----	,F.[HomeTeamFoulsCommitted]
----	,F.[AwayTeamFoulsCommitted]
----	,F.[HomeTeamCorners]
----	,F.[AwayTeamCorners]
----	,F.[HomeTeamYellowCards]
----	,F.[AwayTeamYellowCards]
----	,F.[HomeTeamRedCards]
----	,F.[AwayTeamRedCards]
----FROM [Clean].[FootballData] F
----LEFT OUTER JOIN #RefereeLinkNameSurname RS ON F.RowId = RS.RowId
----LEFT OUTER JOIN [Warehouse].[DimReferee] R ON R.RefereeSurname = RS.CleanSurname AND R.RefereeName = RS.CleanName
----LEFT OUTER JOIN [Warehouse].[DimResult] RTF ON RTF.ResultCode = F.FullTimeResult
----LEFT OUTER JOIN [Warehouse].[DimResult] RTH ON RTH.ResultCode = F.HalfTimeResult
----LEFT OUTER JOIN [Warehouse].[DimDate] DD ON DD.CalendarDate = F.[MatchDate]
----LEFT OUTER JOIN [Warehouse].[DimTeam] TA ON F.[AwayTeam] = TA.Team
----LEFT OUTER JOIN [Warehouse].[DimTeam] TH ON F.[HomeTeam] = TH.Team
----) AS [Source] 
----	(
----		 HashKey
----		,MatchDateId					
----		,HomeTeamId					
----		,AwayTeamId					
----		,FullTimeHomeTeamGoals		
----		,FullTimeAwayTeamGoals		
----		,FullTimeResultId				
----		,HalfTimeHomeTeamGoals		
----		,HalfTimeAwayTeamGoals		
----		,HalfTimeResultId			
----		,RefereeId						
----		,HomeTeamShots				
----		,AwayTeamShots				
----		,HomeTeamShotsOnTarget		
----		,AwayTeamShotsOnTarget		
----		,HomeTeamFoulsCommitted		
----		,AwayTeamFoulsCommitted		
----		,HomeTeamCorners				
----		,AwayTeamCorners				
----		,HomeTeamYellowCards			
----		,AwayTeamYellowCards			
----		,HomeTeamRedCards			
----		,AwayTeamRedCards	
----	)
----ON 
----HASHBYTES('SHA2_256', CONVERT(VARCHAR(100),[Target].MatchDateId)+CONVERT(VARCHAR(100),[Target].HomeTeamId)+CONVERT(VARCHAR(100),[Target].AwayTeamId)+CONVERT(VARCHAR(100),[Target].FullTimeHomeTeamGoals)+CONVERT(VARCHAR(100),[Target].FullTimeAwayTeamGoals)+CONVERT(VARCHAR(100),[Target].FullTimeResult)+CONVERT(VARCHAR(100),[Target].HalfTimeHomeTeamGoals)+CONVERT(VARCHAR(100),[Target].HalfTimeAwayTeamGoals)+CONVERT(VARCHAR(100),[Target].HalfTimeResult)+CONVERT(VARCHAR(100),[Target].RefereeId)+CONVERT(VARCHAR(100),[Target].HomeTeamShots)+CONVERT(VARCHAR(100),[Target].AwayTeamShots)+CONVERT(VARCHAR(100),[Target].HomeTeamShotsOnTarget)+CONVERT(VARCHAR(100),[Target].AwayTeamShotsOnTarget)+CONVERT(VARCHAR(100),[Target].HomeTeamFoulsCommitted)+CONVERT(VARCHAR(100),[Target].AwayTeamFoulsCommitted)+CONVERT(VARCHAR(100),[Target].HomeTeamCorners)+CONVERT(VARCHAR(100),[Target].AwayTeamCorners)+CONVERT(VARCHAR(100),[Target].HomeTeamYellowCards)+CONVERT(VARCHAR(100),[Target].AwayTeamYellowCards)+CONVERT(VARCHAR(100),[Target].HomeTeamRedCards)+CONVERT(VARCHAR(100),[Target].AwayTeamRedCards))
----=
----[Source].HashKey
----WHEN NOT MATCHED BY TARGET
----    THEN	
----		INSERT (MatchDateId,HomeTeamId,AwayTeamId,FullTimeHomeTeamGoals,FullTimeAwayTeamGoals,FullTimeResult,HalfTimeHomeTeamGoals,HalfTimeAwayTeamGoals,HalfTimeResult,RefereeId,HomeTeamShots,AwayTeamShots,HomeTeamShotsOnTarget,AwayTeamShotsOnTarget,HomeTeamFoulsCommitted,AwayTeamFoulsCommitted,HomeTeamCorners,AwayTeamCorners,HomeTeamYellowCards,AwayTeamYellowCards,HomeTeamRedCards,AwayTeamRedCards)
----		VALUES (MatchDateId,HomeTeamId,AwayTeamId,FullTimeHomeTeamGoals,FullTimeAwayTeamGoals,FullTimeResult,HalfTimeHomeTeamGoals,HalfTimeAwayTeamGoals,HalfTimeResult,RefereeId,HomeTeamShots,AwayTeamShots,HomeTeamShotsOnTarget,AwayTeamShotsOnTarget,HomeTeamFoulsCommitted,AwayTeamFoulsCommitted,HomeTeamCorners,AwayTeamCorners,HomeTeamYellowCards,AwayTeamYellowCards,HomeTeamRedCards,AwayTeamRedCards)
----WHEN NOT MATCHED BY SOURCE
----	THEN
----		DELETE;
----SELECT * FROM [Warehouse].[FactFootballData] ORDER BY [MatchDateId]
----SELECT * FROM [Warehouse].[DimDate] WHERE [IsMatchDate] = 1
----GO

--Create Match dimension unique to define each match
CREATE TABLE [Warehouse].[DimMatch] (
	[MatchId] INT NULL
	,[MatchDate] VARCHAR(10) NULL
	,[Match] VARCHAR(500) NULL
	,[Season] VARCHAR(10) NULL
	)
GO

MERGE INTO [Warehouse].[DimMatch] AS [Target]
USING (
	SELECT [RowId] AS [MatchId]
		,[MatchDate]
		,[HomeTeam] + ' vs ' + [AwayTeam] AS [Match]
		,[Season]
	FROM [Clean].[FootballData]
	GROUP BY [RowId]
		,[MatchDate]
		,[HomeTeam]
		,[AwayTeam]
		,[Season]
	) AS [Source]([MatchId], [MatchDate], [Match], [Season])
	ON [Target].[MatchId] = [Source].[MatchId]
WHEN NOT MATCHED BY TARGET
	THEN
		INSERT (
			[MatchId]
			,[MatchDate]
			,[Match]
			,[Season]
			)
		VALUES (
			[Source].[MatchId]
			,[Source].[MatchDate]
			,[Source].[Match]
			,[Source].[Season]
			)
WHEN NOT MATCHED BY SOURCE
	THEN
		DELETE;

--To be downloaded from https://en.wikipedia.org/wiki/List_of_Premier_League_clubs
CREATE TABLE [Stage].[Teams] (
	[Team] VARCHAR(150) NULL
	,[Origin Location] VARCHAR(250) NULL
	)
GO

INSERT INTO [Stage].[Teams] (
	[Team]
	,[Origin Location]
	) (
	SELECT 'Arsenal' AS [Team]
	,'London (Holloway)' AS [Origin Location]

UNION
	
	SELECT 'Aston Villa' AS [Team]
	,'Birmingham (Aston)' AS [Origin Location]

UNION
	
	SELECT 'Barnsley' AS [Team]
	,'Barnsley' AS [Origin Location]

UNION
	
	SELECT 'Birmingham City' AS [Team]
	,'Birmingham (Bordesley)' AS [Origin Location]

UNION
	
	SELECT 'Blackburn Rovers' AS [Team]
	,'Blackburn' AS [Origin Location]

UNION
	
	SELECT 'Blackpool' AS [Team]
	,'Blackpool' AS [Origin Location]

UNION
	
	SELECT 'Bolton Wanderers' AS [Team]
	,'Bolton' AS [Origin Location]

UNION
	
	SELECT 'Bournemouth' AS [Team]
	,'Bournemouth' AS [Origin Location]

UNION
	
	SELECT 'Bradford City' AS [Team]
	,'Bradford' AS [Origin Location]

UNION
	
	SELECT 'Brighton & Hove Albion' AS [Team]
	,'Brighton' AS [Origin Location]

UNION
	
	SELECT 'Burnley' AS [Team]
	,'Burnley' AS [Origin Location]

UNION
	
	SELECT 'Cardiff City' AS [Team]
	,'Cardiff' AS [Origin Location]

UNION
	
	SELECT 'Charlton Athletic' AS [Team]
	,'London (Charlton)' AS [Origin Location]

UNION
	
	SELECT 'Chelsea' AS [Team]
	,'London (Fulham)' AS [Origin Location]

UNION
	
	SELECT 'Crystal Palace' AS [Team]
	,'London (Selhurst)' AS [Origin Location]

UNION
	
	SELECT 'Coventry City' AS [Team]
	,'Coventry' AS [Origin Location]

UNION
	
	SELECT 'Derby County' AS [Team]
	,'Derby' AS [Origin Location]

UNION
	
	SELECT 'Everton' AS [Team]
	,'Liverpool (Walton)' AS [Origin Location]

UNION
	
	SELECT 'Fulham' AS [Team]
	,'London (Fulham)' AS [Origin Location]

UNION
	
	SELECT 'Huddersfield Town' AS [Team]
	,'Huddersfield' AS [Origin Location]

UNION
	
	SELECT 'Hull City' AS [Team]
	,'Kingston upon Hull' AS [Origin Location]

UNION
	
	SELECT 'Ipswich Town' AS [Team]
	,'Ipswich' AS [Origin Location]

UNION
	
	SELECT 'Leeds United' AS [Team]
	,'Leeds' AS [Origin Location]

UNION
	
	SELECT 'Leicester City' AS [Team]
	,'Leicester' AS [Origin Location]

UNION
	
	SELECT 'Liverpool' AS [Team]
	,'Liverpool (Anfield)' AS [Origin Location]

UNION
	
	SELECT 'Manchester City' AS [Team]
	,'Manchester' AS [Origin Location]

UNION
	
	SELECT 'Manchester United' AS [Team]
	,'Manchester (Old Trafford)' AS [Origin Location]

UNION
	
	SELECT 'Middlesbrough' AS [Team]
	,'Middlesbrough' AS [Origin Location]

UNION
	
	SELECT 'Newcastle United' AS [Team]
	,'Newcastle upon Tyne' AS [Origin Location]

UNION
	
	SELECT 'Norwich City' AS [Team]
	,'Norwich' AS [Origin Location]

UNION
	
	SELECT 'Nottingham Forest' AS [Team]
	,'West Bridgford' AS [Origin Location]

UNION
	
	SELECT 'Oldham Athletic' AS [Team]
	,'Oldham' AS [Origin Location]

UNION
	
	SELECT 'Portsmouth' AS [Team]
	,'Portsmouth' AS [Origin Location]

UNION
	
	SELECT 'Queens Park Rangers' AS [Team]
	,'London (Shepherd''s Bush)' AS [Origin Location]

UNION
	
	SELECT 'Reading' AS [Team]
	,'Reading' AS [Origin Location]

UNION
	
	SELECT 'Sheffield United' AS [Team]
	,'Sheffield (Highfield)' AS [Origin Location]

UNION
	
	SELECT 'Sheffield Wednesday' AS [Team]
	,'Sheffield (Owlerton)' AS [Origin Location]

UNION
	
	SELECT 'Southampton' AS [Team]
	,'Southampton' AS [Origin Location]

UNION
	
	SELECT 'Stoke City' AS [Team]
	,'Stoke-on-Trent' AS [Origin Location]

UNION
	
	SELECT 'Sunderland' AS [Team]
	,'Sunderland' AS [Origin Location]

UNION
	
	SELECT 'Swansea City' AS [Team]
	,'Swansea' AS [Origin Location]

UNION
	
	SELECT 'Swindon Town' AS [Team]
	,'Swindon' AS [Origin Location]

UNION
	
	SELECT 'Tottenham Hotspur' AS [Team]
	,'London (Tottenham)' AS [Origin Location]

UNION
	
	SELECT 'Watford' AS [Team]
	,'Watford' AS [Origin Location]

UNION
	
	SELECT 'West Bromwich Albion' AS [Team]
	,'West Bromwich' AS [Origin Location]

UNION
	
	SELECT 'West Ham United' AS [Team]
	,'London (Stratford)' AS [Origin Location]

UNION
	
	SELECT 'Wigan Athletic' AS [Team]
	,'Wigan' AS [Origin Location]

UNION
	
	SELECT 'Wimbledon' AS [Team]
	,'London (Wimbledon)' AS [Origin Location]

UNION
	
	SELECT 'Wolverhampton Wanderers' AS [Team]
	,'Wolverhampton' AS [Origin Location]
	)
SELECT *
	,ROW_NUMBER() OVER (
		PARTITION BY Season ORDER BY [FTPoints] DESC
		) AS [FTClassification]
	,ROW_NUMBER() OVER (
		PARTITION BY Season ORDER BY [HTPoints] DESC
		) AS [HTClassification]
FROM (
	SELECT TH.Team
		,D.Season
		,SUM(F.FullTimePoints) AS [FTPoints]
		,SUM(F.HalfTimePoints) AS [HTPoints]
		,COUNT(*) AS [Number of Games]
	FROM [Warehouse].[FactFootballData] F
	LEFT OUTER JOIN [Warehouse].[DimMatch] D ON F.MatchId = D.MatchId
	LEFT OUTER JOIN [Warehouse].DimTeam TA ON TA.TeamId = F.OppositionTeamId
	LEFT OUTER JOIN [Warehouse].DimTeam TH ON TH.TeamId = F.HomeTeamId
	GROUP BY TH.Team
		,D.Season
	) GroupDetails
