/*
====================================================================
Script Purpose:
====================================================================
    This stored procedure performs check and analyse the data  
    populate the tables from the 'bronze' schema.
*/

    -- Check for Null or Duplicate of Primary keys 
    -- Expectation: No Results


select
cst_id, count(*)
from bronze.crm_cust_info
group by cst_id
having count(*) > 1


    --ROW_NUMBER() OVER () as flag_last: This is a window function. It's used to assign a unique number to each row within a partition.
    --OVER (Partition by cst_id) : This divides the dataset into groups where the  column has the same value. Each group will be processed independently.
    --ORDER BY cst_create_date DESC:  Within each partition (group of rows with the same ), the rows are ordered by the  column in descending order (latest dates first).
    --Result: After applying the ROW_NUMBER() function, each row gets a sequential number (starting from 1) in the order specified.

select *,
ROW_NUMBER() OVER (Partition by cst_id ORDER BY cst_create_date DESC) as flag_last
from bronze.crm_cust_info
where cst_id = 29466

-- Taking only unique cst_id values 
-- Considering the values only when flag_last = 1 for cst_id
-- Aliased Table(t): The inner query creates a temporary table (aliased as ) containing all rows and the calculated  column.

SELECT * FROM (

select *,
ROW_NUMBER() OVER (PARTITION by cst_id ORDER by cst_create_date DESC) as flag_last
from bronze.crm_cust_info
    
)t WHERE flag_last = 1


-- Quality Checks 
-- Unwanted spaces in string values
-- If the original value is not equal to the same value after trimming. It means there are spaces
    
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname ! = TRIM(cst_firstname)

    -- It has some spaces in the first name
    
SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname ! = TRIM(cst_lastname)

 -- It has some spaces in the last name

SELECT cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr ! = TRIM(cst_gndr)

-- Putting in the Aliased table called "t"
    
SELECT 
			cst_id, 
			cst_key, 
			TRIM(cst_firstname) AS cst_firstname, 
			TRIM(cst_lastname) AS cst_lastname, 
			cst_marital_status, 
			cst_gndr,
			cst_create_date
FROM (

	SELECT *,
	ROW_NUMBER() OVER (PARTITION by cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info
    
)t WHERE flag_last = 1



