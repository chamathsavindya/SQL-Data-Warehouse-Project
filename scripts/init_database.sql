/* 
=================================================================================
Create database and Schemas
=================================================================================

Script Purpose:
	This script creates a new database name 'DataWarehouse' after checking if it already exists.
	if the database exists, it is dropped and recreated. Aditionally, the script sets up three schemas within 
	databae: 'bronze', 'silver' and 'gold'

WARNING:
	Running this script will drop the entire 'DataWarehouse' database if it exists.
	All data in the database will be permanently deleted. Proceed with caution and ensure you have
	proper backups before running this script.

	*/
	
USE master;
GO

-- Drop and recreate the 'Datawarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;

-- Create 'DataWarehouse' database
CREATE DATABASE DataWarehouse;

USE DataWarehouse
GO

-- Create schemas
CREATE SCHEMA bronse;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
