--EXEC silver.load_silver

/*
=========================================================================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
=========================================================================================================================

This stored procedure performs the etl process to poulate the 'silver' schema tables from the ' bronze schema.
First truncated the tables and then Inserted transformed data from bronze to silver tables.

*/
-------------------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @start_time_whole DATETIME, @end_time_whole DATETIME
	BEGIN TRY 
		SET @start_time_whole = GETDATE(); 
		PRINT '======================================================================================'
		PRINT 'Loading Silver layer'
		PRINT '======================================================================================'

		PRINT'---------------------------------------------------------------------------------------'
		PRINT 'Loading CRM tables'
		PRINT'---------------------------------------------------------------------------------------'
		
		-- silver.crm_cust_info table
		-- remove all data from the crm_cust_info table and insert csv file to the table 
		SET @start_time = GETDATE();
		PRINT '>> Truncating table: silver.crm_cust_info'
		TRUNCATE TABLE silver.crm_cust_info;

		PRINT '>> INSERTING data INTO silver.crm_cust_info'
		INSERT INTO silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_material_status,
			cst_gndr,
			cst_create_date )
		SELECT 
			cst_id,
			cst_key,
			TRIM(cst_firstname) cst_firstname,
			TRIM(cst_lastname) cst_lastname,
			CASE
				WHEN UPPER(TRIM(cst_material_status)) = 'M'THEN 'married' -- normalized marital status and handles null values
				WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'single'
				ELSE 'n/a'
			END cst_material_status,
			CASE
				WHEN UPPER(TRIM(cst_gndr)) = 'M'THEN 'male' -- normalized gender info and handles null values
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'female'
				ELSE 'n/a'
			END cst_gndr,
			cst_create_date
		FROM (
		SELECT 
			*,
			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) rank_by_date
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL ) T
		WHERE rank_by_date = 1 -- rank cst_create date decending order and pick only first value to avoid duplicates

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' +CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)
		PRINT'---------------------------------------------------------------------------------------'
		-------------------------------------------------------------------------------------------------------------------------------------
		-------------------------------------------------------------------------------------------------------------------------------------

		-- crm_prd_info table
		-- remove all data from the crm_prd_info table and insert csv file to the table 
		SET @start_time = GETDATE();
		PRINT '>>TRUNCATING TABLE silver.crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info;

		PRINT '>>INSERTING DATA INTO silver.crm_prd_info'
		INSERT INTO silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_df
			)
		SELECT
			prd_id,
			REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
			SUBSTRING(prd_key,7,LEN(prd_key)) prd_key,
			prd_nm,
			ISNULL(prd_cost,0) prd_cost,
			CASE UPPER(TRIM(prd_line)) 
			WHEN 'M' THEN 'mountain'
			WHEN 'R' THEN 'road'
			WHEN 'S' THEN 'other sales'
			WHEN 'T' THEN 'touring'
			ELSE 'n/a'
			END prd_line,
			prd_start_dt,
			DATEADD(DAY,-1,LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)) prd_end_df
		FROM bronze.crm_prd_info

		SET @end_time = GETDATE();		 
		PRINT '>> Load Duration: ' +CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)
		PRINT'---------------------------------------------------------------------------------------'
		 -------------------------------------------------------------------------------------------------------------------------------------
		 -------------------------------------------------------------------------------------------------------------------------------------

		 -- crm_sales_details table
		 -- remove all data from the crm_sales_details table and insert csv file to the table 
		 SET @start_time = GETDATE();
		 PRINT '>>Truncatng table silver.crm_sales_details'
		 TRUNCATE TABLE silver.crm_sales_details;

		 PRINT '>>Inserting data into v=silver.crm_sales_details'
		 INSERT INTO silver.crm_sales_details(
			sls_ord_num ,
			sls_prd_key,
			sls_cust_id ,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price)
		 SELECT
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE 
				WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END sls_order_dt,
			CASE 
				WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END sls_ship_dt,
			CASE 
				WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END sls_due_dt,

			CASE 
				WHEN sls_sales IS null OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
				THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END sls_sales,
			sls_quantity,
			CASE 
				WHEN sls_price IS NULL OR sls_price = 0 THEN sls_sales/NULLIF(sls_quantity,0)
				WHEN sls_price <0 THEN ABS(sls_price)
				ELSE sls_price
			END sls_price
		FROM bronze.crm_sales_details
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' +CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)
		PRINT'---------------------------------------------------------------------------------------'

		-------------------------------------------------------------------------------------------------------------------------------------
		-------------------------------------------------------------------------------------------------------------------------------------
		PRINT'---------------------------------------------------------------------------------------'
		PRINT 'Loading ERP tables'
		PRINT'---------------------------------------------------------------------------------------'
		-- erp_cust_az12 table
		-- remove all data from the  table erp_cust_az12 and insert csv file to the table
		SET @start_time = GETDATE();
		PRINT '>>Truncaing table silver.erp_cust_az12'
		TRUNCATE TABLE silver.erp_cust_az12

		PRINT '>>Inseting data into silver.erp_cust_az12'
		INSERT INTO silver.erp_cust_az12(
			cid,
			bdate,
			gen
			)
		SELECT 
			CASE
				WHEN SUBSTRING(cid,1,3) LIKE 'NAS%'THEN SUBSTRING(cid,4,LEN(cid)) -- Remove 'NAS' prefix if present
				ELSE cid
			END cid,
			CASE
				WHEN bdate > GETDATE() THEN NULL -- set future birthdates to null
				ELSE bdate
			END bdate,
			CASE
				WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'male' -- Normalize gender values and handle unknown cases
				WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'female'
				ELSE 'n/a'
			END gen
		FROM bronze.erp_cust_az12

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' +CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)
		PRINT'---------------------------------------------------------------------------------------'
		--------------------------------------------------------------------------------------------------------------------------------
		--------------------------------------------------------------------------------------------------------------------------------

		-- erp_loc_a101 table
		-- remove all data from the  table erp_loc_a101 and insert csv file to the table
		SET @start_time = GETDATE();
		PRINT ' >> Tri=uncating table silver.erp_loc_a101'
		TRUNCATE TABLE silver.erp_loc_a101;

		PRINT '>> Inserting data into silver.erp_loc_a101'
		INSERT INTO silver.erp_loc_a101 (
			cid,
			cntry 
		)
		SELECT 
			REPLACE(TRIM(cid),'-','') cid, -- replaced unneccesary value with empty space
			CASE
				WHEN UPPER(TRIM(cntry)) IN ('USA','US', 'UNITED STATES') THEN 'United States' -- Normalize and handle missing or blank country codes
				WHEN UPPER(TRIM(cntry)) IN ('DE', 'GERMANY') THEN 'Germany'
				WHEN UPPER(TRIM(cntry)) = ' ' OR cntry IS NULL THEN 'n/a'
				ELSE TRIM(cntry)
			END cntry
		FROM bronze.erp_loc_a101

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' +CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)
		PRINT'---------------------------------------------------------------------------------------'

		--------------------------------------------------------------------------------------------------------------------------------
		--------------------------------------------------------------------------------------------------------------------------------

		-- erp_px_cat_g1v2 table
		-- remove all data from the  table erp_px_cat_g1v2 and insert csv file to the table 
		SET @start_time = GETDATE();
		PRINT '>>Truncating table silver.erp_px_cat_g1v2'
		TRUNCATE TABLE silver.erp_px_cat_g1v2;

		PRINT '>> Inserting data into silver.erp_px_cat_g1v2'
		INSERT INTO silver.erp_px_cat_g1v2(
			id,
			cat,
			subcat,
			maintenance
		)
		SELECT
		*
		FROM bronze.erp_px_cat_g1v2

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' +CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)
		PRINT'---------------------------------------------------------------------------------------'

		SET @end_time_whole = GETDATE();
		PRINT '>> Load Duration of entire silver layer: ' +CAST(DATEDIFF(SECOND,@start_time_whole,@end_time_whole) AS NVARCHAR)
	END TRY 
	BEGIN CATCH 
		PRINT '============================================================================'
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error message' + ERROR_MESSAGE();
		PRINT 'Error Number' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT '============================================================================'
	END CATCH
END
