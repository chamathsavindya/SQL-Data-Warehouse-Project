/*
===========================================================================================================================
QUALITY CHECKS
===========================================================================================================================

This script performs various quality checks for data consistency, accuracy, and standardization across the 'silver' schemas. 
It includes checks for: 
	- Null and duplicate primary keys
	- unwanted spaces in string fileds
	- Data standardization and consistancy
	- invaliddate ranges and orders
	- data consistancy between reated fields

When to run:
	- Run these checks after data loading to silver layer

*/





------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------

-- bronze.crm_cust_info table

-- Check for nulls or Duplicates in primary Key
-- Expectation: No Results

SELECT 
	cst_id,
	COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

-- Check for unwanted spacesx
-- Expectation: No Results
SELECT 
	cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

SELECT 
	cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)


-- Data Standardization & Consistency
SELECT DISTINCT cst_material_status
FROM silver.crm_cust_info

SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info


------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------

-- silver.crm_prd_info table

-- Check for nulls or Duplicates in primary Key
-- Expectation: No Results

SELECT 
	prd_id,
	COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

-- Check for unwanted spacesx
-- Expectation: No Results
SELECT 
	prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- Check nulls or negative numbers
-- expectation: No result

SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0

-- Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

-- Check for invalid date orders
SELECT  *
FROM silver.crm_prd_info
WHERE prd_end_df < prd_start_dt


SELECT *
FROM silver.crm_prd_info

-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------

-- crm.sales_details table

-- check data consistancy: Between sales, quantity and price
-- >> sales = quantity * price
-- >> Values must not be NULL, zero or negative
-- if sales is  negative, zero or null, derive it using quantity and price
-- if price os zero or null, calculate it using sales and quantity
-- if price is negative, convert it to positive


SELECT 
	sls_quantity,
	sls_price,
	sls_sales
FROM silver.crm_sales_details
WHERE 
	sls_sales != (sls_quantity*sls_price) 
	OR sls_sales IS NULL or sls_quantity IS NULL or sls_price IS NULL
	OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales


-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------

-- erp_cust_az12 table

-- null check
SELECT *
FROM silver.erp_cust_az12
WHERE cid IS NULL

SELECT *
FROM silver.erp_cust_az12
WHERE bdate IS NULL

SELECT *
FROM silver.erp_cust_az12
WHERE gen IS NULL


-- Check invalid birth days

SELECT *
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

--Data standardization and consistancy
SELECT DISTINCT gen
FROM silver.erp_cust_az12

--------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------

-- erp_loc_a101 table

-- data consistancy across tables
SELECT *
FROM silver.erp_loc_a101

SELECT cst_key
FROM silver.crm_cust_info

--Data standardization and consistancy
SELECT DISTINCT cntry
FROM silver.erp_loc_a101


--------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------

-- erp_px_cat_g1v2 table

SELECT *
FROM silver.erp_px_cat_g1v2

-- checking for the unwanted spaces
SELECT *
FROM silver.erp_px_cat_g1v2
WHERE 
	TRIM(cat) != cat
	OR TRIM(subcat) != subcat
	OR TRIM(maintenance) != maintenance

--Data standardization and consistancy
SELECT DISTINCT
	cat
FROM silver.erp_px_cat_g1v2

SELECT DISTINCT
	maintenance
FROM silver.erp_px_cat_g1v2
