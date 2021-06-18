SELECT 
    CASE [make] WHEN 'alfa-romero' THEN 'Alfa Romeo' 
        ELSE [make] 
    END AS Make,
    LEFT(NEWID(),8) mf_code
INTO [AI42].[dbo].ManufacturingDetails
FROM [AI42].[dbo].[Cars]
WHERE   [body_style] = 'hatchback'
GROUP BY [make]
ORDER BY [make]

DROP TABLE ManufaturingDetails

UPDATE [AI42].[dbo].[Cars]
SET [make] = 'Alfa Romeo' 
WHERE [make] = 'alfa-romero'


SELECT  TOP(10)
        mf_code		
        ,c.make	
        ,fuel_type	
        ,aspiration	
        ,num_of_doors	
        ,body_style
FROM    [AI42].[dbo].[ManufaturingDetails] md
JOIN    [AI42].[dbo].[Cars] c ON c.Make = md.Make
ORDER BY NEWID()

GO

CREATE PROCEDURE CreateNewManufaturingCode (@make varchar(100))
AS
BEGIN

/*
    Check if a MF Code exists, then update else create a new mf_code
*/
    IF EXISTS (SELECT * FROM [AI42].[dbo].[ManufaturingDetails] WHERE make = @make)
    BEGIN
        UPDATE [AI42].[dbo].[ManufaturingDetails]
        SET mf_code = LEFT(NEWID(),8)
        WHERE make = @make
    END
    ELSE
        INSERT INTO [AI42].[dbo].[ManufaturingDetails] (mf_code, make) 
        VALUES(LEFT(NEWID(),8),@make)

/*
    Retrun records for new mf_code
*/
    SELECT  TOP(10)
            mf_code		
            ,c.make	
            ,fuel_type	
            ,aspiration	
            ,num_of_doors	
            ,body_style
    FROM    [AI42].[dbo].[ManufaturingDetails] md
    JOIN    [AI42].[dbo].[Cars] c ON c.make = md.make
    WHERE   md.make = @make

END
GO


CREATE VIEW ExecutiveOverviewDataset
AS
    SELECT  mf_code	
            ,fuel_type	
            ,CASE num_of_doors 
                WHEN 'two' THEN 2
                WHEN 'four' THEN 4
                ELSE NULL END AS num_of_doors
            ,body_style
    FROM    [AI42].[dbo].[ManufaturingDetails] md
    JOIN    [AI42].[dbo].[Cars] c ON c.Make = md.Make
GO

WITH CTE
AS
(
    SELECT  mf_code
            ,body_style
            ,AVG(num_of_doors) avg_num_of_doors
    FROM    ExecutiveOverviewDataset
    WHERE   body_style IN ('sedan','hatchback')
    GROUP BY mf_code
            ,body_style
)
SELECT  *
FROM    CTE A
PIVOT
(
    AVG(avg_num_of_doors) FOR body_style IN ([hatchback],[sedan])
)PVT




INSERT INTO Cars (make, body_type, num_of_cylinders) 
VALUES ('FORD', 'PICKUP', 8);
GO

UPDATE  Cars
SET     num_of_cylinders = 8
WHERE   make = 'FORD'
AND     num_of_cylinders = 6;
GO

DELETE FROM Cars
WHERE make = 'FORD';
GO

WITH CTE
AS
(
    SELECT  mf_code
            ,body_style
            ,AVG(num_of_doors) avg_num_of_doors
    FROM    ExecutiveOverviewDataset
    GROUP BY mf_code
            ,body_style
)
SELECT  DISTINCT body_style
FROM    CTE A
WHERE   avg_num_of_doors >= 4


SELECT  make,
        fuel_type,
        body_style,
        city_mpg,
        RANK() 
            OVER(PARTITION BY make 
                ORDER BY city_mpg DESC) AS mpg_ranked,
        DENSE_RANK() 
            OVER(PARTITION BY make 
                ORDER BY city_mpg DESC) AS mpg_dense_ranked,
        ROW_NUMBER() 
            OVER(PARTITION BY make 
                ORDER BY city_mpg DESC) AS mpg_row_number
FROM    Cars