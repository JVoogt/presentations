USE AI42;
GO

SELECT TOP 10 *
FROM    Cars
WHERE   horsepower = '?'

SELECT TOP 10 *
FROM    cars
WHERE   TRY_CAST(horsepower AS INT) IS NULL 


SELECT TOP 10 column1
        ,make
        ,TRY_CAST(horsepower AS INT) AS horsepower
        ,TRY_CAST(peak_rpm AS INT)
        ,TRY_CAST(price AS INT)
FROM    cars
WHERE   TRY_CAST(horsepower AS INT) IS NULL 
OR      TRY_CAST(peak_rpm AS INT) IS NULL 
OR      TRY_CAST(price AS INT) IS NULL 



SELECT  mf_code	
        ,fuel_type	
        ,CASE num_of_doors 
            WHEN 'two' THEN 2
            WHEN 'four' THEN 4
            ELSE NULL END AS num_of_doors
        ,body_style
FROM    [AI42].[dbo].[ManufaturingDetails] md
JOIN    [AI42].[dbo].[Cars] c ON c.Make = md.Make




DROP VIEW ExecutiveOverviewDataset
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



SELECT  *
FROM    ExecutiveOverviewDataset;


SELECT  mf_code
        ,body_style
        ,AVG(num_of_doors) avg_num_of_doors
FROM    ExecutiveOverviewDataset
WHERE   body_style IN ('sedan','hatchback')
GROUP BY mf_code
        ,body_style;



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