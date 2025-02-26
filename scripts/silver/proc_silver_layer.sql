/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from the bronze layer. 
    It performs the following actions:
    - Truncates the silver table.
    - Insert, Transform, Normalize and Cleaned the date from Bronze layer to silver layer.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC silver.load_bronze;
===============================================================================
*/


CREATE OR ALTER PROCEDURE silver.load_silver AS 
BEGIN

	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
	SET @batch_start_time = GETDATE()
	PRINT '==============================================';
	PRINT 'Loading Silver Layer';
	PRINT '==============================================';


	PRINT '----------------------------------------------';
	PRINT 'Loading CRM Tables';
	PRINT '----------------------------------------------';

SET @start_time = GETDATE();
PRINT '>> Truncate table: silver.crm_cust_info';
TRUNCATE TABLE silver.crm_cust_info;
PRINT '>> Insert Data Into: silver.crm_cust_info';
INSERT INTO silver.crm_cust_info(
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_martial_status,
cst_gndr,
cst_create_date
)

SELECT cst_id, cst_key, TRIM(cst_firstname) AS cst_firstname,  -- Remove unwanted space to ensure data consistency
			TRIM(cst_lastname) AS cst_lastname,
			CASE  WHEN UPPER(TRIM(cst_martial_status)) = 'M' THEN 'Married'
				  WHEN UPPER(TRIM(cst_martial_status)) = 'S' THEN 'Single'
				  ELSE 'N/A' END  AS cst_martial_status,  -- Normalize the martial status to readable format
				CASE 
						WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
						WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
						ELSE 'N/A'       
							END AS cst_gndr,cst_create_date		-- Normalize gender values to readable format 
FROM (
SELECT *,
	RANK() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL
) AS RA_ORDER
	WHERE flag_last = 1;  -- Removing the duplicates by identifing and retaining the most relevant value.
SET @end_time = GETDATE();

PRINT '>> Load Duration: ' +  CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds' ;
PRINT '-------------------------------------------';


SET @start_time = GETDATE();
PRINT '>> Truncate table: silver.crm_prod_info';
TRUNCATE TABLE silver.crm_prod_info;
PRINT '>> Insert Data Into: silver.crm_prod_info';
INSERT INTO silver.crm_prod_info(
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)

SELECT
prd_id,
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost, 0) AS prd_cost,
(CASE UPPER(TRIM(prd_line))
	WHEN 'M' THEN 'Mountain'
	WHEN 'R' THEN 'Road'
	WHEN 'S' THEN 'Other Sales'
	WHEN 'T' THEN 'Touring'
	ELSE 'n/a'

END) AS prd_line,  -- WE Normalize it into readable format
CAST(prd_start_dt AS date) AS prd_start_dt, --We change it to date format
CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt ASC)-1 AS DATE) AS prd_end_dt  -- we change the end date to be one day before the start date.
FROM bronze.crm_prod_info;
SET @end_time = GETDATE();
PRINT '>> Load Duration: ' +  CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds' ;
PRINT '-------------------------------------------';


SET @start_time = GETDATE();
PRINT '>> Truncate table: silver.crm_sales_details';
TRUNCATE TABLE silver.crm_sales_details;
PRINT '>> Insert Data Into: silver.crm_sales_details';
INSERT INTO silver.crm_sales_details(
		
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
)

SELECT sls_ord_num, 
sls_prd_key,   -- WE MAKE SURE THAT  EVERY prd_key is present in the prod_info
sls_cust_id,  -- we also do the same cust_info i try to see which is not present in cust_info and also all the id are present.
CASE 
	WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt,  -- converting the order date column to date column
CASE
	WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
END AS sls_ship_dt,
CASE
	WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
END AS sls_due_dt,
CASE
	WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
	THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales

END AS sls_sales,
sls_quantity,
CASE
	WHEN sls_price IS NULL OR sls_price <= 0
			THEN ABS(sls_sales) / NULLIF(sls_quantity,0)
	ELSE sls_price
END AS sls_price


FROM bronze.crm_sales_details;

SET @end_time = GETDATE();

PRINT '>> Load Duration: ' +  CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds' ;
PRINT '-------------------------------------------';

SET @start_time = GETDATE();
PRINT '>> Truncate table: silver.erp_cust_az12';
TRUNCATE TABLE silver.erp_cust_az12;
PRINT '>> Insert Data Into: silver.erp_cust_az12';
INSERT INTO silver.erp_cust_az12(
	cid, bdate, gen
)


SELECT
CASE
	WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID)) 
	END AS CID,
	
CASE 
	WHEN bdate < '1925-01-01' OR bdate > GETDATE() THEN NULL
	ELSE bdate 
	END AS bdate,
CASE 
	WHEN UPPER(TRIM(gen)) = 'F' THEN 'Male'
	WHEN UPPER(TRIM(gen)) = 'M' THEN 'Female'
	WHEN gen = 'NULL' OR gen = ''  THEN 'n/a'
	ELSE gen
END AS gen
FROM bronze.erp_cust_az12

SET @end_time = GETDATE();
PRINT '>> Load Duration: ' +  CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds' ;
PRINT '-------------------------------------------';


SET @start_time = GETDATE();
PRINT '>> Truncate table: silver.erp_loc_a101';
TRUNCATE TABLE silver.erp_loc_a101;
PRINT '>> Insert Data Into: silver.erp_loc_a101';
INSERT INTO silver.erp_loc_a101(
CID, CNTRY
)
SELECT 
REPLACE(CID, '-', '') AS CID,
CASE
	WHEN UPPER(TRIM(CNTRY))= 'DE' THEN 'Germany'
	WHEN UPPER(TRIM(CNTRY)) IN ('USA', 'US') THEN 'United States'
	WHEN CNTRY  IN ('NULL', '' ) THEN 'n/a'
	ELSE CNTRY
END AS CNTRY
FROM bronze.erp_loc_a101;

SET @end_time = GETDATE();
PRINT '>> Load Duration: ' +  CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds' ;
PRINT '-------------------------------------------';


	PRINT '----------------------------------------------';
	PRINT 'Loading ERP Tables';
	PRINT '----------------------------------------------';

SET @start_time = GETDATE();
PRINT '>> Truncate table: silver.erp_px_cat_loc_g1v2';
TRUNCATE TABLE silver.erp_px_cat_g1v2;
PRINT '>> Insert Data Into: silver.erp_loc_g1v2';
INSERT INTO silver.erp_px_cat_g1v2(
id,
cat,
subcat,
maintenance

)


SELECT * FROM bronze.erp_px_cat_g1v2

SET @end_time = GETDATE();
PRINT '>> Load Duration: ' +  CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds' ;
PRINT '-------------------------------------------';

SET @batch_end_time = GETDATE();
PRINT '===================---------------==============='
PRINT 'Loading Silver layer is completed';
PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds' ;
PRINT '---------===================-----------------===========-----';

END TRY

  --TO KNOW IF THERE IS ERROR IN THE ETL PROCESS AND WHAT TYPES OF AN ERROR IT IS IF THERE IS ?
BEGIN CATCH
	PRINT '==================================================';

	PRINT 'ERROR OCCURED DURING THE LOADING ON SILVER LAYER';
	PRINT 'Error Message' + ERROR_MESSAGE();
	PRINT 'Error Number' + CAST (ERROR_NUMBER() AS NVARCHAR);
	PRINT 'Error Number' + CAST (ERROR_STATE() AS NVARCHAR);

	PRINT '==================================================';

END CATCH

END;
