/* Created data loading procedure using SQL. 
   insert data into tables using 'BULK INSERT' method and before loading the data truncateed tables.
   Measured time duration of loading data into each table.
   Measured time duration of executing whole data loding procedure .

   WARNING : 
		This procedure, first truncate/remove data of each table before loading the data.
		If data exist in tables, they will be lost.
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @start_time_whole DATETIME, @end_time_whole DATETIME
	BEGIN TRY 
		SET @start_time_whole = GETDATE(); 
		PRINT '======================================================================================'
		PRINT 'Loading bronze layer'
		PRINT '======================================================================================'

		PRINT'---------------------------------------------------------------------------------------'
		PRINT 'Loading CRM tables'
		PRINT'---------------------------------------------------------------------------------------'



		-- remove all data from the crm_cust_info table and insert csv file to the table 
		SET @start_time = GETDATE();
		PRINT '>>Truncating table:bronze.crm_cust_info'
		TRUNCATE TABLE bronze.crm_cust_info
		PRINT '>>Inserting data into table:bronze.crm_cust_info'
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\chama\OneDrive\Desktop\SQL course\SQL DWH project scratch\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' +CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)
		PRINT'---------------------------------------------------------------------------------------'

		-- remove all data from the crm_prd_info table and insert csv file to the table 
		SET @start_time = GETDATE();
		PRINT '>>Truncating table:bronze.crm_prd_info'
		TRUNCATE TABLE bronze.crm_prd_info

		PRINT '>>Inserting data into table:bronze.bronze.crm_prd_info'
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\chama\OneDrive\Desktop\SQL course\SQL DWH project scratch\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' +CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)
		PRINT'---------------------------------------------------------------------------------------'

		-- remove all data from the crm_sales_details table and insert csv file to the table 
		SET @start_time = GETDATE();
		PRINT '>>Truncating table:bronze.crm_sales_details'
		TRUNCATE TABLE bronze.crm_sales_details

		PRINT '>>Inserting data into table:bronze.bronze.crm_sales_details'
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\chama\OneDrive\Desktop\SQL course\SQL DWH project scratch\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' +CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)
		PRINT'---------------------------------------------------------------------------------------'


		PRINT'---------------------------------------------------------------------------------------'
		PRINT 'Loading ERP tables'
		PRINT'---------------------------------------------------------------------------------------'

		-- remove all data from the  table erp_cust_az12 and insert csv file to the table
		SET @start_time = GETDATE();
		PRINT '>>Truncating table:bronze.erp_cust_az12'
		TRUNCATE TABLE bronze.erp_cust_az12

		PRINT '>>Inserting data into table:bronze.bronze.erp_cust_az12'
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\chama\OneDrive\Desktop\SQL course\SQL DWH project scratch\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' +CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)
		PRINT'---------------------------------------------------------------------------------------'

		-- remove all data from the  table erp_loc_a101 and insert csv file to the table
		SET @start_time = GETDATE();
		PRINT '>>Truncating table:bronze.erp_loc_a101'
		TRUNCATE TABLE bronze.erp_loc_a101

		PRINT '>>Inserting data into table:bronze.bronze.erp_loc_a101'
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\chama\OneDrive\Desktop\SQL course\SQL DWH project scratch\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' +CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)
		PRINT'---------------------------------------------------------------------------------------'

		-- remove all data from the  table erp_px_cat_g1v2 and insert csv file to the table 
		SET @start_time = GETDATE();
		PRINT '>>Truncating table:bronze.erp_px_cat_g1v2'
		TRUNCATE TABLE bronze.erp_px_cat_g1v2

		PRINT '>>Inserting data into table:bronze.bronze.erp_px_cat_g1v2'
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\chama\OneDrive\Desktop\SQL course\SQL DWH project scratch\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' +CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)
		PRINT'---------------------------------------------------------------------------------------'

		SET @end_time_whole = GETDATE();
		PRINT '>> Load Duration of entire bronze layer: ' +CAST(DATEDIFF(SECOND,@start_time_whole,@end_time_whole) AS NVARCHAR)
	END TRY 
	BEGIN CATCH 
		PRINT '============================================================================'
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error message' + ERROR_MESSAGE();
		PRINT 'Error Number' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT '============================================================================'
	END CATCH
END

