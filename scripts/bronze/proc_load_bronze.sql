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
	
	CREATE OR ALTER PROCEDURE bronze.load_bronze AS
	BEGIN 

		DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
		BEGIN TRY	
	
			SET @batch_start_time = GETDATE();
			PRINT '================================================';

			PRINT 'Truncating the table: bronze.crm_cust_info'
			
			TRUNCATE TABLE bronze.crm_cust_info;

			PRINT 'Table: bronze.crm_cust_info Truncated'
			PRINT '-----------------------------------------'


			PRINT 'Inserting the data into table: bronze.crm_cust_info'
			PRINT '-----------------------------------------'
			SET @start_time = GETDATE();
			BULK INSERT bronze.crm_cust_info
			FROM 'C:\Users\Ravi Shankar\Desktop\DE Prep\Projects\SQL\datasets\source_crm\cust_info.csv'
			WITH (
					FIRSTROW =2,
					FIELDTERMINATOR = ',',
					TABLOCK
			      )
			SET @end_time = GETDATE();
			PRINT 'Loading time for bronze.crm_cust_info: '+CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR)+' seconds';



			PRINT 'Truncating the table: bronze.crm_prd_info'
			TRUNCATE TABLE bronze.crm_prd_info;

			PRINT 'Table: bronze.crm_prd_info Truncated'
			PRINT '-----------------------------------------'


			PRINT 'Inserting the data into table: bronze.crm_prd_info'
			PRINT '-----------------------------------------'
			SET @start_time = GETDATE();
			BULK INSERT bronze.crm_prd_info
			FROM 'C:\Users\Ravi Shankar\Desktop\DE Prep\Projects\SQL\datasets\source_crm\prd_info.csv'
			WITH (
					FIRSTROW =2,
					FIELDTERMINATOR = ',',
					TABLOCK
			      )
			SET @end_time = GETDATE();
			PRINT 'Loading time for bronze.crm_prd_info: '+CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) +' seconds';




			PRINT 'Truncating the table: bronze.crm_sales_details'
			TRUNCATE TABLE bronze.crm_sales_details;

			PRINT 'Table: bronze.crm_sales_details Truncated'
			PRINT '-----------------------------------------'


			PRINT 'Inserting the data into table: bronze.crm_sales_details'
			PRINT '-----------------------------------------'
			SET @start_time = GETDATE();
			BULK INSERT bronze.crm_sales_details
			FROM 'C:\Users\Ravi Shankar\Desktop\DE Prep\Projects\SQL\datasets\source_crm\sales_details.csv'
			WITH (
					FIRSTROW =2,
					FIELDTERMINATOR = ',',
					TABLOCK
			      )
			SET @end_time = GETDATE();
			PRINT 'Loading time for bronze.crm_sales_details: '+CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR)+' seconds';





			PRINT 'Truncating the table: bronze.erp_cust_az12'
			TRUNCATE TABLE bronze.erp_cust_az12;

			PRINT 'Table: bronze.erp_cust_az12 Truncated'
			PRINT '-----------------------------------------'


			PRINT 'Inserting the data into table: bronze.erp_cust_az12'
			PRINT '-----------------------------------------'
			SET @start_time = GETDATE();
			BULK INSERT bronze.erp_cust_az12
			FROM 'C:\Users\Ravi Shankar\Desktop\DE Prep\Projects\SQL\datasets\source_erp\CUST_AZ12.csv'
			WITH (
					FIRSTROW =2,
					FIELDTERMINATOR = ',',
					TABLOCK
			      )
			SET @end_time = GETDATE();
			PRINT 'Loading time for bronze.erp_cust_az12: '+CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR)+' seconds';




			PRINT 'Truncating the table: bronze.erp_loc_a101'
			TRUNCATE TABLE bronze.erp_loc_a101;

			PRINT 'Table: bronze.erp_loc_a101 Truncated'
			PRINT '-----------------------------------------'


			PRINT 'Inserting the data into table: bronze.erp_loc_a101'
			PRINT '-----------------------------------------'
			SET @start_time = GETDATE();
			BULK INSERT bronze.erp_loc_a101
			FROM 'C:\Users\Ravi Shankar\Desktop\DE Prep\Projects\SQL\datasets\source_erp\LOC_A101.csv'
			WITH (
					FIRSTROW =2,
					FIELDTERMINATOR = ',',
					TABLOCK
			      )
			SET @end_time = GETDATE();
			PRINT 'Loading time for bronze.erp_loc_a101: '+CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR)+' seconds';



			PRINT 'Truncating the table: bronze.erp_px_cat_g1v2'
			TRUNCATE TABLE bronze.erp_px_cat_g1v2;

			PRINT 'Table: bronze.erp_px_cat_g1v2 Truncated'
			PRINT '-----------------------------------------'


			PRINT 'Inserting the data into table: bronze.erp_px_cat_g1v2'
			PRINT '-----------------------------------------'
			SET @start_time = GETDATE();
			BULK INSERT bronze.erp_px_cat_g1v2
			FROM 'C:\Users\Ravi Shankar\Desktop\DE Prep\Projects\SQL\datasets\source_erp\PX_CAT_G1V2.csv'
			WITH (
					FIRSTROW =2,
					FIELDTERMINATOR = ',',
					TABLOCK
			      )
			SET @end_time = GETDATE();
			PRINT 'Loading time for bronze.erp_px_cat_g1v2: '+CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR)+' seconds';


			SET @batch_end_time = GETDATE();
			PRINT '=========================================='
			PRINT 'Loading Bronze Layer is Completed';
			PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
			PRINT '=========================================='

		END TRY
		BEGIN CATCH 
				PRINT '=========================================='
				PRINT 'Error during the loading of Bronze layer'
				PRINT 'Error message' + ERROR_message();
				PRINT 'Error messgae' + CAST(ERROR_STATE() AS NVARCHAR);
				PRINT 'Error messgae' + CAST(ERROR_NUMBER() AS NVARCHAR);
				PRINT 'Error messgae' + CAST(ERROR_LINE() AS NVARCHAR);
		END CATCH

	END
