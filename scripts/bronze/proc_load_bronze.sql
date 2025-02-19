/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
EXEC bronze.load_bronze
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
	SET @batch_start_time = GETDATE();
	PRINT '==============================================';
	PRINT 'Loading Bronze Layer';
	PRINT '==============================================';


	PRINT '----------------------------------------------';
	PRINT 'Loading CRM Tables';
	PRINT '----------------------------------------------';
SET @start_time = GETDATE();
PRINT '>> Trancate Table: bronze.crm_cust_info';
TRUNCATE TABLE bronze.crm_cust_info;

PRINT '>> Inserting Data Into: bronze.crm_cust_info';
BULK INSERT bronze.crm_cust_info
FROM 'D:\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
WITH(
	FIRSTROW = 2, --THE HEADER IS THE FIRST ROW
	FIELDTERMINATOR = ',', -- THE DATA IS SEPARATED IN COMMA
	TABLOCK
);
SET @end_time = GETDATE();
PRINT '>> Load Duration: ' +  CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds' ;
PRINT '-------------------------------------------';

SET @start_time = GETDATE();
PRINT '>> Trancate Table: bronze.crm_prod_info';
TRUNCATE TABLE bronze.crm_prod_info;

PRINT '>> Inserting Data Into: bronze.crm_prod_info';
BULK INSERT bronze.crm_prod_info
FROM 'D:\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
WITH(
	FIRSTROW = 2, --THE HEADER IS THE FIRST ROW
	FIELDTERMINATOR = ',', -- THE DATA IS SEPARATED IN COMMA
	TABLOCK
);
SET @end_time = GETDATE();
PRINT '>> Load Duration: ' +  CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds' ;
PRINT '-------------------------------------------';

SET @start_time = GETDATE();
PRINT '>> Trancate Table: bronze.crm_sales_details';
TRUNCATE TABLE bronze.crm_sales_details;

PRINT '>> Inserting Data Into: bronze.crm_sales_details';
BULK INSERT bronze.crm_sales_details
FROM 'D:\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
WITH(
	FIRSTROW = 2, --THE HEADER IS THE FIRST ROW
	FIELDTERMINATOR = ',', -- THE DATA IS SEPARATED IN COMMA
	TABLOCK
);
SET @end_time = GETDATE();
PRINT '>> Load Duration: ' +  CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds' ;
PRINT '-------------------------------------------';

	PRINT '----------------------------------------------';
	PRINT 'Loading ERP Tables';
	PRINT '----------------------------------------------';

SET @start_time = GETDATE();
PRINT '>> Trancate Table: bronze.erp_cust_az12';
TRUNCATE TABLE bronze.erp_cust_az12;

PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
BULK INSERT bronze.erp_cust_az12
FROM 'D:\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
WITH(
	FIRSTROW = 2, --THE HEADER IS THE FIRST ROW
	FIELDTERMINATOR = ',', -- THE DATA IS SEPARATED IN COMMA
	TABLOCK
);
SET @end_time = GETDATE();
PRINT '>> Load Duration: ' +  CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds' ;
PRINT '-------------------------------------------';

SET @start_time = GETDATE();
PRINT '>> Trancate Table: bronze.erp_loc_a101';
TRUNCATE TABLE bronze.erp_loc_a101;

PRINT '>> Insert Table: bronze.erp_loc_a101';
BULK INSERT bronze.erp_loc_a101
FROM 'D:\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
WITH(
	FIRSTROW = 2, --THE HEADER IS THE FIRST ROW
	FIELDTERMINATOR = ',', -- THE DATA IS SEPARATED IN COMMA
	TABLOCK
);
SET @end_time = GETDATE();
PRINT '>> Load Duration: ' +  CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds' ;
PRINT '-------------------------------------------';

SET @start_time = GETDATE();
PRINT '>> Truncate Table: bronze.erp_px_cat_g1v2';
TRUNCATE TABLE bronze.erp_px_cat_g1v2;

PRINT '>> Insert Table: bronze.erp_px_cat_g1v2';
BULK INSERT bronze.erp_px_cat_g1v2
FROM 'D:\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
WITH(
	FIRSTROW = 2, --THE HEADER IS THE FIRST ROW
	FIELDTERMINATOR = ',', -- THE DATA IS SEPARATED IN COMMA
	TABLOCK
);
SET @end_time = GETDATE();
PRINT '>> Load Duration: ' +  CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds' ;
PRINT '-------------------------------------------';

SET @batch_end_time = GETDATE();
PRINT '===================---------------==============='
PRINT 'Loading Bronze layer is completed';
PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds' ;
PRINT '---------===================-----------------===========-----';

END TRY
  --TO KNOW IF THERE IS ERROR IN THE ETL PROCESS AND WHAT TYPES OF AN ERROR IT IS IF THERE IS ?
BEGIN CATCH
	PRINT '==================================================';

	PRINT 'ERROR OCCURED DURING THE LOADING ON BRONZE LAYER';
	PRINT 'Error Message' + ERROR_MESSAGE();
	PRINT 'Error Number' + CAST (ERROR_NUMBER() AS NVARCHAR);
	PRINT 'Error Number' + CAST (ERROR_STATE() AS NVARCHAR);

	PRINT '==================================================';

END CATCH
END
