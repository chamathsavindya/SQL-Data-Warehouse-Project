/*
================================================================================================================
DDL Script: Create Gold Views
================================================================================================================

This script creates views for the gold layer.
The gold layer represents the final fact and dimention tables (star Schema)

Each view transformaion and combine data from the silver layer to create clean
business ready dataset.

View can be queried directly for analytics and reporting.
*/



-- =============================================================================================================
-- Join all customer info tables and create object(view)
-- =============================================================================================================
IF OBJECT_ID('gold.dim_customers','V') IS NOT NULL
	DROP VIEW gold.dim_customers
GO
CREATE VIEW gold.dim_customers AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	CASE
		WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr 
		ELSE COALESCE(ca.gen,'n/a')
	END as gender,
	ca.bdate AS birthdate,
	la.cntry AS country,
	ci.cst_material_status AS marital_status,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS la
ON ci.cst_key = la.cid 

-- =============================================================================================================
-- Join all product related info tables and create object(view)
-- =============================================================================================================
IF OBJECT_ID('gold.dim_products','V') IS NOT NULL
	DROP VIEW gold.dim_products
GO
CREATE VIEW gold.dim_products AS
SELECT
	ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt) AS product_key,
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance,
	pn.prd_cost AS product_cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date 
FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_px_cat_g1v2 AS pc
ON pn.cat_id = pc.id
WHERE pn.prd_end_df IS NULL-- filter out all historical data

-- =============================================================================================================
-- Create a fact table with joining two dimention tables
-- =============================================================================================================
IF OBJECT_ID('gold.fact_sales','V') IS NOT NULL
	DROP VIEW gold.fact_sales
GO
CREATE VIEW gold.fact_sales AS
SELECT
	sd.sls_ord_num AS order_number,
	pd.product_key,
	ct.customer_key,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS ship_date,
	sd.sls_due_dt AS due_date,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price,
	sd.sls_sales AS sales
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_customers AS ct
ON sd.sls_cust_id = ct.customer_id
LEFT JOIN gold.dim_products AS pd
ON sd.sls_prd_key = pd.product_number


SELECT *
FROM gold.dim_products
