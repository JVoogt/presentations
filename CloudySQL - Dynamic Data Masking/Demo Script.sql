/*
ALTER DATABASE DynamicDataMaskingDemo
SET SINGLE_USER
WITH ROLLBACK IMMEDIATE;

USE Master;
GO
DROP DATABASE IF EXISTS DynamicDataMaskingDemo

CREATE DATABASE DynamicDataMaskingDemo;
GO
*/
SET NOCOUNT ON;

USE DynamicDataMaskingDemo;
GO

DROP TABLE IF EXISTS Accounts

/*
CREATE TABLE statement with DDM(Dynamic Data Masking)
*/

CREATE TABLE Accounts
([Id]       INT IDENTITY(1, 1)
				PRIMARY KEY
				NOT NULL,
 [AuthorId] INT NOT NULL,
 [Name]     VARCHAR(50) MASKED 
				WITH(FUNCTION = 'partial(3,"~",3)')
						NOT NULL,
 [ISBN]     VARCHAR(50) NOT NULL
)

/*
Insert test data
*/
SET IDENTITY_INSERT [dbo].[Accounts] ON 
INSERT [dbo].[Accounts] ([Id], [AuthorId], [Name], [ISBN]) VALUES (399, 1945, N'Monwerpower', N'054-81-1834')
INSERT [dbo].[Accounts] ([Id], [AuthorId], [Name], [ISBN]) VALUES (401, 511, N'Monbanilor', N'245-52-6401')
INSERT [dbo].[Accounts] ([Id], [AuthorId], [Name], [ISBN]) VALUES (403, 265, N'Emdudistor', N'230-37-1594')
INSERT [dbo].[Accounts] ([Id], [AuthorId], [Name], [ISBN]) VALUES (405, 1795, N'Hapbanor', N'848-13-3263')
INSERT [dbo].[Accounts] ([Id], [AuthorId], [Name], [ISBN]) VALUES (407, 357, N'Cippickistor', N'781-28-9432')
INSERT [dbo].[Accounts] ([Id], [AuthorId], [Name], [ISBN]) VALUES (409, 1765, N'Dopbanicar', N'038-63-3936')
INSERT [dbo].[Accounts] ([Id], [AuthorId], [Name], [ISBN]) VALUES (411, 1089, N'Lomcadamax', N'344-07-5944')
INSERT [dbo].[Accounts] ([Id], [AuthorId], [Name], [ISBN]) VALUES (413, 1571, N'Frotinedgantor', N'649-34-4230')
INSERT [dbo].[Accounts] ([Id], [AuthorId], [Name], [ISBN]) VALUES (415, 1029, N'Tupvenedower', N'597-99-4109')
INSERT [dbo].[Accounts] ([Id], [AuthorId], [Name], [ISBN]) VALUES (417, 705, N'Doperefantor', N'459-56-5834')
INSERT [dbo].[Accounts] ([Id], [AuthorId], [Name], [ISBN]) VALUES (419, 45, N'Partumantor', N'300-52-9455')
SET IDENTITY_INSERT [dbo].[Accounts] OFF

/*
ALTER TABLE statement with DDM(Dynamic Data Masking)
*/
ALTER TABLE Accounts ALTER COLUMN ISBN ADD MASKED WITH(FUNCTION = 'default()');  
GO

ALTER TABLE Accounts ALTER COLUMN AuthorId ADD MASKED WITH (FUNCTION = 'random(1, 5)')

SELECT *
FROM   Accounts
GO

DROP USER IF EXISTS DummyUser;
GO

CREATE USER DummyUser WITHOUT LOGIN;

GRANT SELECT ON Accounts TO DummyUser; 
GO
GRANT SHOWPLAN TO DummyUser; 
GO

EXECUTE AS USER = 'DummyUser'

SELECT *
FROM   Accounts

REVERT
GO