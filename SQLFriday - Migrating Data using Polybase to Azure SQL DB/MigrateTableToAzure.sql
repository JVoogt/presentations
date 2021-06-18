
CREATE PROCEDURE [dbo].[MoveTableToAzure]
(
@SourceDB			VARCHAR(200),
@SourceScheme		VARCHAR(200) = 'dbo',
@SourceTable		VARCHAR(200),
@DestinationScheme	VARCHAR(200),
@DestinationTable	VARCHAR(200),
@Distribution		VARCHAR(200) = '',
@Index				VARCHAR(200) = 'HEAP',
@AllowedRejectedRecords		INT	= 0,
@SASToken			VARCHAR(200) = '<ADD YOU SAS TOKEN HERE>',
@FileFormat			VARCHAR(20) = 'Parquet_FF'
@DataSource			VARCHAR(20) = 'Parquet_DS'
)
AS

/*

██╗   ██╗ █████╗ ██████╗ ██╗ █████╗ ██████╗ ██╗     ███████╗███████╗
██║   ██║██╔══██╗██╔══██╗██║██╔══██╗██╔══██╗██║     ██╔════╝██╔════╝
██║   ██║███████║██████╔╝██║███████║██████╔╝██║     █████╗  ███████╗
╚██╗ ██╔╝██╔══██║██╔══██╗██║██╔══██║██╔══██╗██║     ██╔══╝  ╚════██║
 ╚████╔╝ ██║  ██║██║  ██║██║██║  ██║██████╔╝███████╗███████╗███████║
  ╚═══╝  ╚═╝  ╚═╝╚═╝  ╚═╝╚═╝╚═╝  ╚═╝╚═════╝ ╚══════╝╚══════╝╚══════╝
                                                                    
*/

DECLARE @SQL nvarchar(max)
DECLARE @FQDN varchar(1000)
DECLARE @SourceTableNameSinkFQDN varchar(1000)
DECLARE @SourceTableNameFQDN varchar(1000)
DECLARE @UniqKey char(8)

SET @UniqKey= LEFT(NEWID(),8)
SET @FQDN = REPLACE(REPLACE(REPLACE(REPLACE(CONCAT(@SourceDB,'/', @SourceScheme, @SourceTable, CONVERT(CHAR(8),GETDATE(),112)),'\','_'),' ','_'),' ','_'),'-','_') + '_' + @UniqKey
SET @SourceTableNameSinkFQDN = CONCAT(QUOTENAME(@SourceDB),'.',QUOTENAME(@SourceScheme),'.',QUOTENAME(CONCAT(@SourceTable,'_Sink_',CONVERT(CHAR(8),GETDATE(),112))))
SET @SourceTableNameFQDN = CONCAT(QUOTENAME(@SourceDB),'.',QUOTENAME(@SourceScheme),'.',QUOTENAME(@SourceTable))
SET @Distribution = CASE WHEN @Distribution = '' THEN 'ROUND_ROBIN' WHEN @Distribution = 'REPLICATE' THEN 'REPLICATE' ELSE 'HASH(' + @Distribution + ')' END


/*

███████╗██╗  ██╗████████╗███████╗██████╗ ███╗   ██╗ █████╗ ██╗         ████████╗ █████╗ ██████╗ ██╗     ███████╗    ███████╗███╗   ███╗██████╗ 
██╔════╝╚██╗██╔╝╚══██╔══╝██╔════╝██╔══██╗████╗  ██║██╔══██╗██║         ╚══██╔══╝██╔══██╗██╔══██╗██║     ██╔════╝    ██╔════╝████╗ ████║██╔══██╗
█████╗   ╚███╔╝    ██║   █████╗  ██████╔╝██╔██╗ ██║███████║██║            ██║   ███████║██████╔╝██║     █████╗      ███████╗██╔████╔██║██████╔╝
██╔══╝   ██╔██╗    ██║   ██╔══╝  ██╔══██╗██║╚██╗██║██╔══██║██║            ██║   ██╔══██║██╔══██╗██║     ██╔══╝      ╚════██║██║╚██╔╝██║██╔═══╝ 
███████╗██╔╝ ██╗   ██║   ███████╗██║  ██║██║ ╚████║██║  ██║███████╗       ██║   ██║  ██║██████╔╝███████╗███████╗    ███████║██║ ╚═╝ ██║██║     
╚══════╝╚═╝  ╚═╝   ╚═╝   ╚══════╝╚═╝  ╚═╝╚═╝  ╚═══╝╚═╝  ╚═╝╚══════╝       ╚═╝   ╚═╝  ╚═╝╚═════╝ ╚══════╝╚══════╝    ╚══════╝╚═╝     ╚═╝╚═╝     
                                                                                                                                               
*/

EXEC('
DECLARE @SQL nvarchar(max)
DECLARE @CreateTable varchar(max)

SET @SQL = 
''
SELECT TOP 1 @CreateTableOut = ''''create external table '+@SourceDB+'.'''' + QuoteName(t.TABLE_SCHEMA) + ''''.'''' + QuoteName(so.name + ''''_Sink_'''' + CONVERT(CHAR(8),GETDATE(),112)) +  ''''('''' + LEFT(o.List, Len(o.List) - 1) + '''') WITH (LOCATION=''''''''/files/'+@FQDN+'/'''''''',DATA_SOURCE = ['+@DataSource+'],FILE_FORMAT = ['+@FileFormat+'],REJECT_TYPE = VALUE,REJECT_VALUE = '+@AllowedRejectedRecords+');  '''' --AS SQL_CREATE_TABLE
FROM '+@SourceDB+'.sys.tables so
JOIN '+@SourceDB+'.sys.schemas ss ON		so.schema_id = ss.schema_id
CROSS APPLY (
	SELECT ''''  ['''' + column_name + ''''] '''' + data_type + CASE data_type
			WHEN ''''sql_variant''''
				THEN ''''''''
			WHEN ''''text''''
				THEN ''''''''
			WHEN ''''ntext''''
				THEN ''''''''
			WHEN ''''decimal''''
				THEN ''''('''' + cast(numeric_precision AS VARCHAR) + '''', '''' + cast(numeric_scale AS VARCHAR) + '''')''''
			ELSE coalesce(''''('''' + CASE 
						WHEN character_maximum_length = - 1
							THEN ''''MAX''''
						ELSE cast(character_maximum_length AS VARCHAR)
						END + '''')'''', '''''''')
			END  + '''','''' 
	FROM '+@SourceDB+'.information_schema.columns 
	WHERE	TABLE_NAME = so.name
	AND		TABLE_SCHEMA = ss.name
	AND		ISNULL(CHARACTER_MAXIMUM_LENGTH,1) > 0
	ORDER BY ordinal_position
	FOR XML PATH('''''''')
	) o(list)
LEFT JOIN '+@SourceDB+'.information_schema.tables t ON t.Table_name = so.Name AND t.Table_Schema = ss.name
WHERE type = ''''U''''
	AND so.name NOT IN (''''dtproperties'''')
	AND Table_Catalog = @SourceDB
	AND Table_Schema = @SourceScheme
	AND Table_Name = @SourceTable
''

DECLARE @ParmDefinition nvarchar(500);  
  
SET @ParmDefinition = N''@SourceDB VARCHAR(200), @SourceScheme VARCHAR(200), @SourceTable VARCHAR(200), @CreateTableOut varchar(max) OUTPUT'';  
EXEC sp_executesql @SQL, @ParmDefinition, @SourceDB = '+@SourceDB+', @SourceScheme = '+@SourceScheme+', @SourceTable = '+@SourceTable+', @CreateTableOut = @CreateTable OUTPUT;

EXEC(@CreateTable);
PRINT(@CreateTable)


')


DECLARE @ColumnHeaders varchar(max)
SET @SQL = '

SELECT @ColumnHeadersOUT = LEFT(o.List, Len(o.List) - 1) + ''''
FROM
(
	SELECT QUOTENAME(column_name)+ '',''
	FROM '+@SourceDB+'.information_schema.columns
	WHERE table_name = @SourceTable
	AND		table_schema = @SourceScheme
	AND		ISNULL(CHARACTER_MAXIMUM_LENGTH,1) > 0
	ORDER BY ordinal_position
	FOR XML PATH('''')
) o(list)
'
DECLARE @ParmDefinition nvarchar(500);  
  
SET @ParmDefinition = N'@SourceTable VARCHAR(200),@SourceScheme VARCHAR(200), @ColumnHeadersOUT varchar(max) OUTPUT';  
EXEC sp_executesql @SQL, @ParmDefinition, @SourceTable = @SourceTable, @SourceScheme = @SourceScheme, @ColumnHeadersOUT = @ColumnHeaders OUTPUT;

/*

███████╗██╗███╗   ██╗██╗  ██╗    ████████╗ █████╗ ██████╗ ██╗     ███████╗
██╔════╝██║████╗  ██║██║ ██╔╝    ╚══██╔══╝██╔══██╗██╔══██╗██║     ██╔════╝
███████╗██║██╔██╗ ██║█████╔╝        ██║   ███████║██████╔╝██║     █████╗  
╚════██║██║██║╚██╗██║██╔═██╗        ██║   ██╔══██║██╔══██╗██║     ██╔══╝  
███████║██║██║ ╚████║██║  ██╗       ██║   ██║  ██║██████╔╝███████╗███████╗
╚══════╝╚═╝╚═╝  ╚═══╝╚═╝  ╚═╝       ╚═╝   ╚═╝  ╚═╝╚═════╝ ╚══════╝╚══════╝
                                                                          
*/

PRINT('
INSERT INTO '+@SourceTableNameSinkFQDN+' ('+@ColumnHeaders+')
SELECT	'+@ColumnHeaders+'
FROM	'+@SourceTableNameFQDN+'
')

EXEC('
INSERT INTO '+@SourceTableNameSinkFQDN+' ('+@ColumnHeaders+')
SELECT	'+@ColumnHeaders+'
FROM	'+@SourceTableNameFQDN+'
')

/*

███████╗██╗  ██╗████████╗███████╗██████╗ ███╗   ██╗ █████╗ ██╗         ████████╗ █████╗ ██████╗ ██╗     ███████╗     █████╗ ██████╗ ██╗    ██╗
██╔════╝╚██╗██╔╝╚══██╔══╝██╔════╝██╔══██╗████╗  ██║██╔══██╗██║         ╚══██╔══╝██╔══██╗██╔══██╗██║     ██╔════╝    ██╔══██╗██╔══██╗██║    ██║
█████╗   ╚███╔╝    ██║   █████╗  ██████╔╝██╔██╗ ██║███████║██║            ██║   ███████║██████╔╝██║     █████╗      ███████║██║  ██║██║ █╗ ██║
██╔══╝   ██╔██╗    ██║   ██╔══╝  ██╔══██╗██║╚██╗██║██╔══██║██║            ██║   ██╔══██║██╔══██╗██║     ██╔══╝      ██╔══██║██║  ██║██║███╗██║
███████╗██╔╝ ██╗   ██║   ███████╗██║  ██║██║ ╚████║██║  ██║███████╗       ██║   ██║  ██║██████╔╝███████╗███████╗    ██║  ██║██████╔╝╚███╔███╔╝
╚══════╝╚═╝  ╚═╝   ╚═╝   ╚══════╝╚═╝  ╚═╝╚═╝  ╚═══╝╚═╝  ╚═╝╚══════╝       ╚═╝   ╚═╝  ╚═╝╚═════╝ ╚══════╝╚══════╝    ╚═╝  ╚═╝╚═════╝  ╚══╝╚══╝ 
                                                                                                                                              

*/

EXEC('
DECLARE @SQL nvarchar(max)
DECLARE @CreateTable varchar(max)

SET @SQL = 
''
SELECT TOP 1 @CreateTableOut = ''''create table '+@DestinationScheme+'.'+@DestinationTable+'       ('''' + LEFT(o.List, Len(o.List) - 1) + '''') WITH (DISTRIBUTION = ' + @Distribution + ', ' + @Index + ' );  '''' 
FROM '+@SourceDB+'.sys.tables so
JOIN	'+@SourceDB+'.sys.schemas ss ON so.schema_id = ss.schema_id
CROSS APPLY (
	SELECT ''''  ['''' + column_name + ''''] '''' + data_type + CASE data_type
			WHEN ''''sql_variant''''
				THEN ''''''''
			WHEN ''''text''''
				THEN ''''''''
			WHEN ''''ntext''''
				THEN ''''''''
			WHEN ''''decimal''''
				THEN ''''('''' + cast(numeric_precision AS VARCHAR) + '''', '''' + cast(numeric_scale AS VARCHAR) + '''')''''
			ELSE coalesce(''''('''' + CASE 
						WHEN character_maximum_length = - 1
							THEN ''''MAX''''
						ELSE cast(character_maximum_length AS VARCHAR)
						END + '''')'''', '''''''')
			END  +  '''','''' 
	FROM '+@SourceDB+'.information_schema.columns
	WHERE	table_name = so.name
	AND		TABLE_SCHEMA = ss.name
	AND		ISNULL(CHARACTER_MAXIMUM_LENGTH,1) > 0
	ORDER BY ordinal_position
	FOR XML PATH('''''''')
	) o(list)
LEFT JOIN '+@SourceDB+'.information_schema.tables t ON t.Table_name = so.Name AND t.Table_Schema = ss.name
WHERE type = ''''U''''
	AND so.name NOT IN (''''dtproperties'''')
	AND Table_Catalog = @SourceDB
	AND Table_Schema = @SourceScheme
	AND Table_Name = @SourceTable
''

DECLARE @ParmDefinition nvarchar(500);  
DECLARE @max_title varchar(30);  
  
SET @ParmDefinition = N''@SourceDB VARCHAR(200), @SourceScheme VARCHAR(200), @SourceTable VARCHAR(200), @CreateTableOut varchar(max) OUTPUT'';  
EXEC sp_executesql @SQL, @ParmDefinition, @SourceDB = '+@SourceDB+', @SourceScheme = '+@SourceScheme+', @SourceTable = '+@SourceTable+', @CreateTableOut = @CreateTable OUTPUT;

EXEC(@CreateTable) AT P3ADW;
PRINT(@CreateTable)


');

SET @SQL = '
COPY INTO '+@DestinationScheme+'.'+@DestinationTable+'
FROM ''https://zajbpdetblob02.blob.core.windows.net/det1/files/'+@FQDN+'/''
WITH (
    FILE_FORMAT = Parquet_FF,
    CREDENTIAL = (IDENTITY= ''Shared Access Signature'', SECRET='+ @SASToken +')
)
OPTION(LABEL=''DATA MOVE: '+@DestinationScheme+'.'+@DestinationTable+''')
'

PRINT(@SQL)
EXEC(@SQL) AT P3ADW

/*

███████╗██╗  ██╗████████╗███████╗██████╗ ███╗   ██╗ █████╗ ██╗          ██████╗██╗     ███████╗ █████╗ ███╗   ██╗██╗   ██╗██████╗ 
██╔════╝╚██╗██╔╝╚══██╔══╝██╔════╝██╔══██╗████╗  ██║██╔══██╗██║         ██╔════╝██║     ██╔════╝██╔══██╗████╗  ██║██║   ██║██╔══██╗
█████╗   ╚███╔╝    ██║   █████╗  ██████╔╝██╔██╗ ██║███████║██║         ██║     ██║     █████╗  ███████║██╔██╗ ██║██║   ██║██████╔╝
██╔══╝   ██╔██╗    ██║   ██╔══╝  ██╔══██╗██║╚██╗██║██╔══██║██║         ██║     ██║     ██╔══╝  ██╔══██║██║╚██╗██║██║   ██║██╔═══╝ 
███████╗██╔╝ ██╗   ██║   ███████╗██║  ██║██║ ╚████║██║  ██║███████╗    ╚██████╗███████╗███████╗██║  ██║██║ ╚████║╚██████╔╝██║     
╚══════╝╚═╝  ╚═╝   ╚═╝   ╚══════╝╚═╝  ╚═╝╚═╝  ╚═══╝╚═╝  ╚═╝╚══════╝     ╚═════╝╚══════╝╚══════╝╚═╝  ╚═╝╚═╝  ╚═══╝ ╚═════╝ ╚═╝     
                                                                                                                                  
*/

SET @SQL = 'DROP EXTERNAL TABLE ' + @SourceTableNameSinkFQDN
PRINT(@SQL)
EXEC(@SQL)
