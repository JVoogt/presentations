ALTER DATABASE TemporalDemo
SET SINGLE_USER
WITH ROLLBACK IMMEDIATE;

USE Master;
GO
DROP DATABASE IF EXISTS TemporalDemo

CREATE DATABASE TemporalDemo;
GO

USE TemporalDemo;
GO

/*

Create Temporal Table

*/
IF OBJECT_ID('Author') IS NOT NULL
	ALTER TABLE  Author
	SET (SYSTEM_VERSIONING = OFF)

DROP TABLE IF EXISTS Author
DROP TABLE IF EXISTS Author_History
GO

CREATE TABLE Author 
(
    Id INT IDENTITY(1,2) PRIMARY KEY NOT NULL,
	Name VARCHAR(50) NOT NULL,
	Surname VARCHAR(50) NOT NULL,
    StartTime datetime2 GENERATED ALWAYS AS ROW START NOT NULL, 
    EndTime datetime2 GENERATED ALWAYS AS ROW END NOT NULL,   
    PERIOD FOR SYSTEM_TIME (StartTime,EndTime) 
)
WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.Author_History) );
GO

/*

With HIDDEN system Period Columns

*/

IF OBJECT_ID('Books') IS NOT NULL
	ALTER TABLE  Books
	SET (SYSTEM_VERSIONING = OFF)

DROP TABLE IF EXISTS Books
DROP TABLE IF EXISTS Books_History
GO

CREATE TABLE Books 
(
    Id INT IDENTITY(1,1) PRIMARY KEY NOT NULL,
	AuthorId INT NOT NULL,
	Name VARCHAR(50) NOT NULL,
	ISBN VARCHAR(50) NOT NULL,
    StartTime datetime2 GENERATED ALWAYS AS ROW START HIDDEN NOT NULL, 
    EndTime datetime2 GENERATED ALWAYS AS ROW END HIDDEN NOT NULL,   
    PERIOD FOR SYSTEM_TIME (StartTime,EndTime)   ,
	CONSTRAINT FK_Author_Id FOREIGN KEY (AuthorId) 
    REFERENCES Author (Id) 
)
WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.Books_History));
GO

/*

Show differences

*/

SELECT TOP 100 *
FROM Author

SELECT TOP 100 *
FROM Books

/*

Modify Data

*/

UPDATE Author
SET Surname = CONCAT(Surname,'_',LEFT(Name,1))
WHERE Name LIKE 'T%'

UPDATE B
SET Name = CONCAT(A.Name,'_',B.Name)
FROM Books B
JOIN Author A ON A.Id = B.AuthorId
WHERE A.Name LIKE 'A%'

/*

Show Results
Temporal Query

*/
SELECT *
FROM Author
WHERE Name LIKE 'T%'
ORDER BY Id, EndTime

SELECT *
FROM Author
FOR SYSTEM_TIME ALL
WHERE Name LIKE 'T%'
ORDER BY Id, EndTime

SELECT B.*
FROM Books B
JOIN Author A ON A.Id = B.AuthorId
WHERE A.Name LIKE 'A%'

/*

JOIN Temporal Tables

*/

;WITH cte
AS
(
	SELECT *
	FROM Books
	FOR SYSTEM_TIME AS OF '2016-03-07 18:42:18'
)
SELECT *
FROM cte c
JOIN Author A ON A.Id = c.AuthorId
WHERE A.Name LIKE 'A%'

;WITH cte
AS
(
	SELECT *
	FROM Books
)
SELECT *
FROM cte c
JOIN Author A ON A.Id = c.AuthorId
WHERE A.Name LIKE 'A%'

/*

Add System-Versioning to existing tables

*/
SELECT *
INTO Address
FROM AdventureWorks2016.Person.Address AS A

SELECT *
FROM Address AS A

SELECT *
FROM INFORMATION_SCHEMA.TABLES AS T

ALTER TABLE [Address]
   ADD 
      StartTime datetime2(0) GENERATED ALWAYS AS ROW START HIDDEN  
           CONSTRAINT DF_SysStart DEFAULT '2018-10-01'--GETDATE()
      , EndTime datetime2(0) GENERATED ALWAYS AS ROW END HIDDEN  
           CONSTRAINT DF_SysEnd DEFAULT CONVERT(datetime2 (0), '9999-12-31 23:59:59'), 
      PERIOD FOR SYSTEM_TIME (StartTime, EndTime); 
GO 

--TABLE NEEDS TO HAVE A PRIMARY KEY
ALTER TABLE [Address]
	ADD PRIMARY KEY (AddressId)

ALTER TABLE Address 
   SET (SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.Address_History)) 
;

/*

Modify Data

*/

SELECT * FROM Address
SELECT * FROM Address_History

UPDATE Address
SET PostalCode = 01111
WHERE City = 'Paris'

SELECT TOP 100 * FROM Address WHERE City = 'Paris'
SELECT * FROM Address_History 

SELECT  SCH.name + '.' + TBL.name AS TableName
      , IDX.type
FROM    sys.tables AS TBL
        INNER JOIN sys.schemas AS SCH ON TBL.schema_id = SCH.schema_id
        INNER JOIN sys.indexes AS IDX ON TBL.object_id = IDX.object_id 
ORDER BY TableName

/*

Fix your corrupted data

*/

WITH cte
AS
(
SELECT	AddressId
,		PostalCode
FROM	Address
FOR SYSTEM_TIME AS OF '2016-03-07 18:55'
WHERE	City = 'Paris'
)
UPDATE A
SET PostalCode = c.PostalCode
FROM Address A
JOIN cte c ON c.AddressId = A.AddressId

/*

Check your rollback?

*/

SELECT	*
FROM	Address
WHERE	City = 'Paris'

/*

Show All data changes

*/

SELECT	*
,		StartTime
,		EndTime
,		LAG(PostalCode, 1,NULL) OVER (PARTITION BY AddressId ORDER BY EndTime) AS PreviousQuota
FROM	Address
FOR SYSTEM_TIME ALL
WHERE	City = 'Paris'
ORDER BY AddressId, EndTime

SELECT TOP 1000
        P.BusinessEntityID AS ID
      , P.FirstName
      , P.LastName
INTO	Person
FROM    AdventureWorks2016.Person.Person AS P

/*

Manually create History Table

*/

DROP TABLE IF EXISTS Person_History;
GO

CREATE TABLE dbo.Person_History
       (
         ID INT NOT NULL
       , FirstName NVARCHAR(50) NOT NULL
       , LastName NVARCHAR(50) NOT NULL
       , StartTime DATETIME2(0) NOT NULL
       , EndTime DATETIME2(0) NOT NULL DEFAULT CONVERT(datetime2 (0), '9999-12-31 23:59:59')
       );
GO

/*

Create Cluster Columnstore index on history table

*/

CREATE CLUSTERED COLUMNSTORE INDEX idx_Person_Id
   ON Person_History; 
CREATE NONCLUSTERED INDEX idx_Person_Id_PERIOD_COLUMNS 
   ON Person_History (EndTime, StartTime, ID); 
GO 

SELECT TOP 10 * FROM dbo.Person

ALTER TABLE dbo.Person
   ADD 
      StartTime datetime2(0) GENERATED ALWAYS AS ROW START HIDDEN  
           CONSTRAINT DF_Person_Start DEFAULT SYSUTCDATETIME()
      , EndTime datetime2(0) GENERATED ALWAYS AS ROW END HIDDEN  
           CONSTRAINT DF_Person_End DEFAULT CONVERT(datetime2 (0), '9999-12-31 23:59:59'), 
      PERIOD FOR SYSTEM_TIME (StartTime, EndTime); 
GO

--TABLE NEEDS TO HAVE A PRIMARY KEY
ALTER TABLE Person
	ADD PRIMARY KEY (Id);
GO

ALTER TABLE dbo.Person 
   SET (SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.Person_History));
GO

/*

Show Indexes

*/

SELECT  SCH.name + '.' + TBL.name AS TableName
      , IDX.type
      , TBL.temporal_type_desc
      , IDX.name
FROM    sys.tables AS TBL
        INNER JOIN sys.schemas AS SCH ON TBL.schema_id = SCH.schema_id
        INNER JOIN sys.indexes AS IDX ON TBL.object_id = IDX.object_id
ORDER BY TableName

SELECT * FROM Person AS P
SELECT * FROM dbo.Person_History AS PH

/*

Alter Schema

*/
ALTER TABLE Person
	ADD Initial VARCHAR(5)

SELECT * FROM Person AS P
SELECT * FROM dbo.Person_History AS PH

UPDATE dbo.Person
SET FirstName = CONCAT(Firstname,'-')

DBCC TRACEON (10422,-1);



/*

Create function to stretch history table

*/

CREATE FUNCTION dbo.fn_StretchBySystemEndTime(@systemEndTime datetime2) 
RETURNS TABLE 
WITH SCHEMABINDING  
AS  
RETURN SELECT 1 AS is_eligible 
  WHERE @systemEndTime < CONVERT(datetime2, '2015-11-01T00:00:00', 101) ;

/*

Enable stretch for History table
Needs to have data in

*/

--EXEC sp_configure 'remote data archive' , '1';  
--GO

--RECONFIGURE;  
--GO  

--ALTER TABLE Person
--SET ( 
--        REMOTE_DATA_ARCHIVE = ON (MIGRATION_STATE = OUTBOUND)
--	)

ALTER TABLE Books_History
SET ( 
        REMOTE_DATA_ARCHIVE = ON 
                ( 
                        FILTER_PREDICATE = dbo.fn_StretchBySystemEndTime (EndTime)
                                , MIGRATION_STATE = OUTBOUND 
                )
        ) 
;