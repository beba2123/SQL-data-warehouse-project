/*
===============================================================================
DDL Script: Create Gold Tables
===============================================================================
Script Purpose:
    This script creates Views in the 'Gold' Layer, and the gold layer represents the
final dimension and fact tables (star schem)
    Each view performs transformations and combines data from the silver layer to produce 
a clean, enriched and business ready data-set

Usage:
	- these Views can be queried directly for analystics and reporting.
===============================================================================
*/

-- data aggregation, integration in the gold layer
CREATE VIEW gold.dim_customers AS 

SELECT

ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,
cst_id AS Customer_id,
cst_key AS  Customer_number,
cst_firstname AS  First_name,
cst_lastname AS Last_name,
cst_martial_status AS marital_Status,
loc.CNTRY AS Country,
CASE
	WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr  --TAKING cst_gndr as the master table.
	ELSE COALESCE(ca.GEN, 'n/a')  --handling the null value in the GEN Column.
END AS Gender,
ca.BDATE AS Birthdate,
cst_create_date AS create_date
FROM silver.crm_cust_info AS ci

LEFT JOIN silver.erp_cust_az12 AS ca
ON		ci.cst_key = ca.CID

LEFT JOIN silver.erp_loc_a101 AS loc
ON		ci.cst_key = loc.CID;


CREATE VIEW gold.dim_products AS 

SELECT 
ROW_NUMBER() OVER(ORDER BY prd_start_dt, prd_key) As Product_key,
prd_id AS Product_id, 
prd_key AS Product_number,
prd_nm AS Product_name,
cat_id AS Catagory_id,
prd_cat.CAT AS Catagory,
prd_cat.SUBCAT AS Subcatagory,
prd_cat.MAINTENANCE AS Maintenance,
prd_cost AS production_cost,
prd_line AS Production_line,
prd_start_dt AS Start_date
FROM silver.crm_prod_info as prod 
LEFT JOIN silver.erp_px_cat_g1v2 AS prd_cat
ON prod.cat_id = prd_cat.ID
WHERE prd_end_dt IS NULL;

CREATE VIEW gold.fact_sales AS 

SELECT 
sls_ord_num AS Order_number,
dim.Product_key,  ---basicly we just connected the dimensions withe the FACT using the surgate key.
dim_cus.customer_key,
sls_order_dt AS Order_date,
sls_ship_dt AS Shipping_date,
sls_due_dt AS Due_date,
sls_sales AS Sales_amount,
sls_quantity AS Quantity,
sls_price AS Price
FROM silver.crm_sales_details sls
LEFT JOIN gold.dim_products AS dim
ON sls.sls_prd_key = dim.Product_number
LEFT JOIN gold.dim_customers AS dim_cus
ON sls.sls_cust_id = dim_cus.Customer_id

SELECT * FROM gold.fact_sales

SELECT * FROM gold.dim_customers

SELECT * FROM gold.dim_products


