Use master;
Go

--Drop and recreate Database 
If Exists (Select 1 from sys.databases Where name ='DataWareHouse')
	Begin
		Alter Database DataWareHouse Set Single_User with Rollback Immediate;
		Drop Database DataWareHouse;
	End;
Go
--Creating Database
Create Database DataWareHouse;
Go

use DataWareHouse;
Go

--Creating Necessary Schemas

-- Create Bronze schema if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Bronze')
BEGIN
    BEGIN TRY
        EXEC('CREATE SCHEMA Bronze');
        PRINT 'Schema Bronze created.';
    END TRY
    BEGIN CATCH
        PRINT 'Error creating schema Bronze.';
        PRINT ERROR_MESSAGE();
    END CATCH
END

-- Create Silver schema
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Silver')
BEGIN
    BEGIN TRY
        EXEC('CREATE SCHEMA Silver');
        PRINT 'Schema Silver created.';
    END TRY
    BEGIN CATCH
        PRINT 'Error creating schema Silver.';
        PRINT ERROR_MESSAGE();
    END CATCH
END

-- Create Gold schema
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Gold')
BEGIN
    BEGIN TRY
        EXEC('CREATE SCHEMA Gold');
        PRINT 'Schema Gold created.';
    END TRY
    BEGIN CATCH
        PRINT 'Error creating schema Gold.';
        PRINT ERROR_MESSAGE();
    END CATCH
END


----Bronze Layer

If OBJECT_ID ('bronze.crm_cust_info','U') is not NULL 
	Drop Table bronze.crm_cust_info;

Create table bronze.crm_cust_info
(
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname  NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gender NVARCHAR(50),
	cst_date DATE
);


If OBJECT_ID ('bronze.crm_prd_info','U') is not NULL 
	Drop Table bronze.crm_prd_info;

Create table bronze.crm_prd_info
(
	prd_id INT,
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATETIME,
	prd_end_dt DATETIME
);

If OBJECT_ID ('bronze.crm_sales_details','U') is not NULL 
	Drop Table bronze.crm_sales_details;

Create table bronze.crm_sales_details
(
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
);


If OBJECT_ID ('bronze.erp_loc_a101','U') is not NULL 
	Drop Table bronze.erp_loc_a101;

Create table bronze.erp_loc_a101
(
	cid NVARCHAR(50),
	cntry NVARCHAR(50)
);


If OBJECT_ID ('bronze.erp_cust_az12','U') is not NULL 
	Drop Table bronze.erp_cust_az12;

Create table bronze.erp_cust_az12
(
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(50)
);


If OBJECT_ID ('bronze.erp_px_cat_g1v2','U') is not NULL 
	Drop Table bronze.erp_px_cat_g1v2;

Create table bronze.erp_px_cat_g1v2
(
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintainance NVARCHAR(50),
);



--BULK inserts
Go

Create or Alter Procedure bronze.load_bronze as

	Begin
		DECLARE @start_time DATETIME,
				@end_time DATETIME,
				@total_start DATETIME,
				@total_end DATETIME
		set @total_start=Getdate();
		Print'===================================================='
		Print'Loading Bronze Layer'
		Print'===================================================='
	--CUST_INFO

		PRINT CHAR(13) + CHAR(10);
		Print'----------------------------------------------------'
		Print'Loading CRM Tables'
		Print'----------------------------------------------------'

	BEGIN TRY
		Set @start_time=GETDATE();
		PRINT CHAR(13) + CHAR(10);
		Print'>>Truncating Table : bronze.crm_cust_info'
		Truncate Table bronze.crm_cust_info;
		Print'>>Inserting Data into: bronze.crm_cust_info'
		BULK INSERT bronze.crm_cust_info
			From 'D:\SQLBaraa\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
			With 
			(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
		Set @end_time=GETDATE();
		PRINT CHAR(13) + CHAR(10);
		PRINT'>>Load Time:'+Cast(Datediff(second,@start_time,@end_time)AS NVARCHAR)+'seconds'

	END TRY

	BEGIN CATCH
		PRINT 'Error Loading the file crm_cust_info'
		Print Error_Message();
	END CATCH

	--PRD_INFO
	BEGIN TRY
		Set @start_time=GETDATE();
		PRINT CHAR(13) + CHAR(10);
		Print'>>Truncating Table: bronze.crm_prd_info'
		Truncate Table bronze.crm_prd_info;
		Print'>>Inserting Data into: bronze.crm_prd_info'
		BULK INSERT bronze.crm_prd_info
			From 'D:\SQLBaraa\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
			With 
			(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);	
		Set @end_time=GETDATE();
		PRINT CHAR(13) + CHAR(10);
		PRINT'>>Load Time:'+Cast(Datediff(second,@start_time,@end_time)AS NVARCHAR)+'seconds'
	END TRY
	BEGIN CATCH
		PRINT 'Error Loading the file crm_prd_info'
		Print Error_Message();
	END CATCH

	--SALES_DETAILS
	BEGIN TRY
		Set @start_time=GETDATE();
		PRINT CHAR(13) + CHAR(10);
		Print'>>Truncating Table: bronze.crm_sales_details'
		Truncate Table bronze.crm_sales_details;
		Print'>>Inserting Data into: bronze.crm_sales_details'
		BULK INSERT bronze.crm_sales_details
			From 'D:\SQLBaraa\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
			With 
			(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
		PRINT CHAR(13) + CHAR(10);
		Set @end_time=GETDATE();
		PRINT'>>Load Time:'+Cast(Datediff(second,@start_time,@end_time)AS NVARCHAR)+'seconds'

	END TRY
	BEGIN CATCH
		PRINT 'Error Loading the file bronze.crm_sales_details'
		Print Error_Message();
	END CATCH


		PRINT CHAR(13) + CHAR(10);
		Print'----------------------------------------------------'
		Print'Loading ERP Tables'
		Print'----------------------------------------------------'
	--CUST_AZ12
	BEGIN TRY
		Set @start_time=GETDATE();
		PRINT CHAR(13) + CHAR(10);
		Print'>>Truncating Table: bronze.erp_cust_az12'
		Truncate Table bronze.erp_cust_az12;
		Print'>>Inserting Data into: bronze.erp_cust_az12'
		BULK INSERT bronze.erp_cust_az12
			From 'D:\SQLBaraa\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
			With 
			(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
		Set @end_time=GETDATE();
		PRINT CHAR(13) + CHAR(10);
		PRINT'>>Load Time:'+Cast(Datediff(second,@start_time,@end_time)AS NVARCHAR)+'seconds'
 
	END TRY
	BEGIN CATCH
		PRINT 'Error Loading the file erp_cust_az12'
		Print Error_Message();
	END CATCH

	--LOC_A101
	BEGIN TRY
		Set @start_time=GETDATE();
		PRINT CHAR(13) + CHAR(10);
		Print'>>Truncating Table: bronze.erp_loc_a101'
		Truncate Table bronze.erp_loc_a101;
		Print'>>Inserting Data into: bronze.erp_loc_a101'
		BULK INSERT bronze.erp_loc_a101
			From 'D:\SQLBaraa\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
			With 
			(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
		Set @end_time=GETDATE();
		PRINT CHAR(13) + CHAR(10);
		PRINT'>>Load Time:'+Cast(Datediff(second,@start_time,@end_time)AS NVARCHAR)+'seconds'
 
	END TRY
	BEGIN CATCH
		PRINT 'Error Loading the file erp_loc_a101'
		Print Error_Message();
	END CATCH
	--PX_CAT_G1V2
	BEGIN TRY
		Set @start_time=GETDATE();
		PRINT CHAR(13) + CHAR(10);
		Print'>>Truncating Table: bronze.erp_px_cat_g1v2'
		Truncate Table bronze.erp_px_cat_g1v2;
		Print'>>Inserting Data into: bronze.erp_px_cat_g1v2'
		BULK INSERT bronze.erp_px_cat_g1v2
			From 'D:\SQLBaraa\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
			With 
			(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
		Set @end_time=GETDATE();
		PRINT CHAR(13) + CHAR(10);
		PRINT'>>Load Time:'+Cast(Datediff(second,@start_time,@end_time)AS NVARCHAR)+'seconds'
	END TRY
	BEGIN CATCH
		PRINT 'Error Loading the file erp_px_cat_g1v2'
		Print Error_Message();
	END CATCH
		PRINT CHAR(13) + CHAR(10);
		set @total_end=Getdate()
		PRINT'>>Total Load Time:'+Cast(Datediff(second,@total_start,@total_end)AS NVARCHAR)+'seconds'
END
GO

EXEC bronze.load_bronze



--Silver Layer

If OBJECT_ID ('silver.crm_cust_info','U') is not NULL 
	Drop Table silver.crm_cust_info;

Create table silver.crm_cust_info
(
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname  NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gender NVARCHAR(50),
	cst_date DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

If OBJECT_ID ('silver.crm_prd_info','U') is not NULL 
	Drop Table silver.crm_prd_info;

Create table silver.crm_prd_info
(
	prd_id INT,
	cat_id NVARCHAR(50),
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

If OBJECT_ID ('silver.crm_sales_details','U') is not NULL 
	Drop Table silver.crm_sales_details;

Create table silver.crm_sales_details
(
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt DATE,
	sls_ship_dt DATE,
	sls_due_dt DATE,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);


If OBJECT_ID ('silver.erp_loc_a101','U') is not NULL 
	Drop Table silver.erp_loc_a101;

Create table silver.erp_loc_a101
(
	cid NVARCHAR(50),
	cntry NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);


If OBJECT_ID ('silver.erp_cust_az12','U') is not NULL 
	Drop Table silver.erp_cust_az12;

Create table silver.erp_cust_az12
(
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);


If OBJECT_ID ('silver.erp_px_cat_g1v2','U') is not NULL 
	Drop Table silver.erp_px_cat_g1v2;

Create table silver.erp_px_cat_g1v2
(
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintainance NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
Go
-------------------Inserting into Silver Layer tables

Create Or Alter Procedure silver.load_silver As
Begin
		DECLARE @start_time DATETIME,
				@end_time DATETIME,
				@total_start DATETIME,
				@total_end DATETIME
	Begin Try
		set @total_start=Getdate();
		Print'===================================================='
		Print'Loading Silver Layer'
		Print'===================================================='
	---Table crm_cust_info
		PRINT CHAR(13) + CHAR(10);
		Print'----------------------------------------------------'
		Print'Loading CRM Tables'
		Print'----------------------------------------------------'
		Set @start_time=GETDATE();
		PRINT CHAR(13) + CHAR(10);
		Print'>>Truncating Table: silver.crm_cust_info'
		Truncate Table silver.crm_cust_info
		Print'>>Inserting Data into: silver.crm_cust_info'
		Insert into silver.crm_cust_info
		(
			cst_id ,
			cst_key,
			cst_firstname,
			cst_lastname ,
			cst_marital_status ,
			cst_gender ,
			cst_date 
		)
		Select	
			cst_id,
			cst_key,
			Trim(cst_firstname) As cst_firstname,
			Trim(cst_lastname) As cst_lastname,
			Case 
				When Upper(Trim(cst_marital_status)) = 'S' Then 'Single'
				When Upper(Trim(cst_marital_status)) = 'M' Then 'Married'
				Else 'n/a'
			End cst_marital_status,
			Case 
				When Upper(Trim(cst_gender)) = 'F' Then 'Female'
				When Upper(Trim(cst_gender)) = 'M' Then 'Male'
				Else 'n/a'
			End cst_gender,
			cst_date
		From
		(
			Select 
				*,
				ROW_NUMBER() over(partition by cst_id order by cst_date DESC) as flag
			From Bronze.crm_cust_info
		)t
		Where flag=1 and cst_id is Not Null
		Set @end_time=GETDATE();
		PRINT CHAR(13) + CHAR(10);
		PRINT'>>Load Time:'+Cast(Datediff(second,@start_time,@end_time)AS NVARCHAR)+'seconds'


		---Table crm_prd_info
		Set @start_time=GETDATE();
		Print'>>Truncating Table: silver.crm_prd_info'
		Truncate Table silver.crm_prd_info

		Print'>>Inserting Data into: silver.crm_prd_info'
		Insert Into silver.crm_prd_info
		(
			prd_id ,
			cat_id ,
			prd_key ,
			prd_nm ,
			prd_cost ,
			prd_line ,
			prd_start_dt,
			prd_end_dt
		)

		Select 
			prd_id,
			Replace(Substring(prd_key,1,5),'-','_') as cat_id,
			Substring(prd_key,7,Len(prd_key)) as prd_key,
			prd_nm,
			Coalesce(prd_cost,0) as prd_cost,
			Case Upper(Trim(prd_line))
				When 'M' then 'Mountain'
				When 'R' then 'Road'
				When 'S' then 'Other Sales'
				When 'T' then 'Touring'
				Else 'n/a'
			End as prd_line,
			Cast(prd_start_dt as Date) as prd_start_dt,
			Cast(Lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)-1 as Date) as prd_end_dt
		From bronze.crm_prd_info
		Set @end_time=GETDATE();
		PRINT CHAR(13) + CHAR(10);
		PRINT'>>Load Time:'+Cast(Datediff(second,@start_time,@end_time)AS NVARCHAR)+'seconds'
		---Table crm_sales_details
		Set @start_time=GETDATE();
		Print'>>Truncating Table: silver.crm_sales_details'
		Truncate Table silver.crm_sales_details

		Print'>>Inserting Data into: silver.crm_sales_details'
		Insert Into silver.crm_sales_details
		(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_price,
			sls_quantity
		)

		Select
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			Case When sls_order_dt =0 Or Len(sls_order_dt)!=8 Then Null
				 Else Cast(Cast(sls_order_dt as Varchar)as Date)
			End as sls_order_dt,
			Case When sls_ship_dt =0 Or Len(sls_ship_dt)!=8 Then Null
				 Else Cast(Cast(sls_ship_dt as Varchar)as Date)
			End as sls_ship_dt,
			Case When sls_due_dt =0 Or Len(sls_due_dt)!=8 Then Null
				 Else Cast(Cast(sls_due_dt as Varchar)as Date)
			End as sls_due_dt,
			Case when sls_sales is Null Or sls_sales <=0 Or sls_sales != sls_quantity*abs(sls_price) 
					Then  sls_quantity*sls_price
				 Else sls_sales
			End as sls_sales,
			Case when sls_price is Null Or sls_price <=0 
					Then sls_sales/Nullif(sls_quantity,0)
				 Else sls_price
			End as sls_price,
			sls_quantity
		from bronze.crm_sales_details
		Set @end_time=GETDATE();
		PRINT CHAR(13) + CHAR(10);
		PRINT'>>Load Time:'+Cast(Datediff(second,@start_time,@end_time)AS NVARCHAR)+'seconds'

		---Table silver.erp_cust_az12
		Set @start_time=GETDATE();
		PRINT CHAR(13) + CHAR(10);
			Print'----------------------------------------------------'
			Print'Loading ERP Tables'
			Print'----------------------------------------------------'
		Print'>>Truncating Table: silver.erp_cust_az12'
		Truncate Table silver.erp_cust_az12

		Print'>>Inserting Data into: silver.erp_cust_az12'
		Insert Into silver.erp_cust_az12
		(
			cid,
			bdate,
			gen 
		)
		Select
			Case When cid Like 'NAS%' Then Substring(cid,4,Len(cid)) 
				 Else cid
			End As cid,
			Case When bdate> Getdate() Then Null
				 Else bdate
			End As bdate,
			Case When Upper(Trim(gen)) In ('F','FEMALE') Then 'Female'
				 When Upper(Trim(gen)) In ('M','MALE') Then 'Male'
				 Else 'n/a'
			End As gen
		from bronze.erp_cust_az12
		Set @end_time=GETDATE();
		PRINT CHAR(13) + CHAR(10);
		PRINT'>>Load Time:'+Cast(Datediff(second,@start_time,@end_time)AS NVARCHAR)+'seconds'

		--Table silver.erp_loc_a101
		Set @start_time=GETDATE();
		Print'>>Truncating Table: silver.erp_loc_a101'
		Truncate Table silver.erp_loc_a101

		Print'>>Inserting Data Into: silver.erp_loc_a101'
		Insert Into silver.erp_loc_a101
		(
			cid,
			cntry
		)

		Select
			Replace(cid,'-','')cid,
			Case When Trim(cntry) = 'DE' Then 'Germany'
				 When Trim(cntry) in ('US','USA') Then 'United States'
				 When Trim(cntry) = '' or Trim(cntry) is Null Then 'n/a'
				 Else Trim(cntry)
			End as cntry
		from bronze.erp_loc_a101
		Set @end_time=GETDATE();
		PRINT CHAR(13) + CHAR(10);
		PRINT'>>Load Time:'+Cast(Datediff(second,@start_time,@end_time)AS NVARCHAR)+'seconds'

		--Table silver.erp_px_cat_g1v2
		Set @start_time=GETDATE();
		Print'>>Truncating Table: silver.erp_px_cat_g1v2'
		Truncate Table silver.erp_px_cat_g1v2

		Print'>>Inserting Data into: silver.erp_px_cat_g1v2'
		Insert into silver.erp_px_cat_g1v2(id,cat,subcat,maintainance)
		select
			id,
			cat,
			subcat,
			maintainance
		from bronze.erp_px_cat_g1v2
		Set @end_time=GETDATE();
		PRINT CHAR(13) + CHAR(10);
		PRINT'>>Load Time:'+Cast(Datediff(second,@start_time,@end_time)AS NVARCHAR)+'seconds'

		PRINT CHAR(13) + CHAR(10);
			set @total_end=Getdate()
			PRINT'>>Total Load Time:'+Cast(Datediff(second,@total_start,@total_end)AS NVARCHAR)+'seconds'
End Try
Begin Catch
	Print'====================================='
	Print'Error Occured During Loading Silver Layer'
	Print'Error Message'+Error_Message()
	Print'Error Number'+Cast(Error_Number() As NVARCHAR)
	Print'Error Line'+Cast(Error_Line() As NVARCHAR)
	Print'Error State'+Cast(Error_State()As NVARCHAR)
End Catch
End
Go

Exec silver.load_silver
Go
--Gold layer
-- Customer Dimension View
BEGIN TRY
    IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
        PRINT 'Altering existing view: gold.dim_customers';
    ELSE
        PRINT 'Creating new view: gold.dim_customers';

    EXEC('
    CREATE OR ALTER VIEW gold.dim_customers AS
    SELECT 
        ROW_NUMBER() OVER (ORDER BY cst_id) AS Customer_key,
        ci.cst_id AS Customer_id,
        ci.cst_key AS Customer_number,
        ci.cst_firstname AS First_name,
        ci.cst_lastname AS Last_name,
        la.cntry AS Country,
        ci.cst_marital_status AS Marital_status,
        CASE 
            WHEN ci.cst_gender != ''n/a'' THEN ci.cst_gender
            ELSE COALESCE(ca.gen, ''n/a'')
        END AS Gender,
        ca.bdate AS Birthdate,
        ci.cst_date AS Date_created
    FROM silver.crm_cust_info ci
    LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
    LEFT JOIN silver.erp_loc_a101 la ON ci.cst_key = la.cid
    ');
END TRY
BEGIN CATCH
    PRINT 'Error creating or altering gold.dim_customers view:';
    PRINT ERROR_MESSAGE();
END CATCH;
GO

-- Product Dimension View
BEGIN TRY
    IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
        PRINT 'Altering existing view: gold.dim_products';
    ELSE
        PRINT 'Creating new view: gold.dim_products';

    EXEC('
    CREATE OR ALTER VIEW gold.dim_products AS
    SELECT 
        ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS Product_key,
        pn.prd_id AS Product_id,
        pn.prd_key AS Product_number,
        pn.prd_nm AS Product_name,
        pn.cat_id AS Category_id,
        pc.cat AS Category,
        pc.subcat AS Subcategory,
        pc.maintainance AS Maintainance,
        pn.prd_cost AS Cost,
        pn.prd_line AS Product_line,
        pn.prd_start_dt AS Start_date
    FROM silver.crm_prd_info pn
    LEFT JOIN silver.erp_px_cat_g1v2 pc ON pn.cat_id = pc.id
    WHERE prd_end_dt IS NULL
    ');
END TRY
BEGIN CATCH
    PRINT 'Error creating or altering gold.dim_products view:';
    PRINT ERROR_MESSAGE();
END CATCH;
GO

-- Sales Fact View
BEGIN TRY
    IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
        PRINT 'Altering existing view: gold.fact_sales';
    ELSE
        PRINT 'Creating new view: gold.fact_sales';

    EXEC('
    CREATE OR ALTER VIEW gold.fact_sales AS
    SELECT
        sd.sls_ord_num AS Order_number,
        pr.Product_key,
        cu.Customer_key,
        sd.sls_order_dt AS Order_date,
        sd.sls_ship_dt AS Ship_date,
        sd.sls_due_dt AS Due_date,
        sd.sls_sales AS Sales,
        sd.sls_quantity AS Quantity,
        sd.sls_price AS Price
    FROM silver.crm_sales_details sd
    LEFT JOIN gold.dim_customers cu ON sd.sls_cust_id = cu.Customer_id
    LEFT JOIN gold.dim_products pr ON sd.sls_prd_key = pr.Product_number
    ');
END TRY
BEGIN CATCH
    PRINT 'Error creating or altering gold.fact_sales view:';
    PRINT ERROR_MESSAGE();
END CATCH;
GO



----Analytics

--Exploring where all Cuntries where our customners come from

Select Distinct	
	Country
From gold.dim_customers
Go
--Explore all Categories and Subcategories in Product Range

Select Distinct
	Category,
	Subcategory,
	Product_name
From gold.dim_products
Go

--Identifying the earliest and Latest order dates in our data

Select 
	Min(Order_date) as First_order,
	Max(order_date) as Last_order,
	DATEDIFF(year,Min(order_date),Max(order_date)) as Order_range_years
From gold.fact_sales
Go
---Finding the youngest and oldest customers

Select
	Min(Birthdate) as Oldest_customer_bdate,
	DATEDIFF(year,Min(Birthdate),GETDATE()) as Oldest_customer,
	Max(Birthdate) as Youngest_customer_bdate,
	DATEDIFF(year,Max(Birthdate),GETDATE()) as Youngest_customer
From
	gold.dim_customers
Go


------Exploring Big Business Numbers

--Finding the Total sales
--Number of items sold
--Average Selling Price
--Total Number of Orders
--Total numbers of Products 
--Total Numbers of Customers
--Total number of Customers that placed an order

Select 'Total Sales' as Measure_Name, Sum(sales) as Measure_Value From gold.fact_sales
Union All
Select 'Total Items' as Measure_Name,Sum(quantity) as Measure_Value From gold.fact_sales
Union All
Select 'Average Selling Price' as Measure_Name,Avg(price) as Measure_Value From gold.fact_sales
Union All
Select 'Total Orders' as Measure_Name,Count(Distinct order_number) as Measure_Value From gold.fact_sales
Union All
Select 'Total Products' as Measure_name,Count(Distinct product_key) as Measure_Value From gold.dim_products
Union All 
Select 'Total_Customers' as Measure_Name,Count(Distinct customer_key) as Measure_Value From gold.dim_customers
Union All
Select 'Total_Customers_Ordered' as Measure_Name,Count(Distinct customer_key) as Measure_value From gold.fact_sales



------Magnitude of Mesaures by Dimension
---Total Customer by Country

Select 
	Country,
	Count(Distinct Customer_key) As Total_Customers
From gold.dim_customers
Group by Country
Order By Total_Customers Desc

---Total Cutomers by Gender

Select 
	Gender,
	Count(Distinct Gender) As Total_Customers
From gold.dim_customers
Group by Gender
Order By Total_Customers Desc

--Total Prodcusts by Category

Select 
	Category,
	Count(Product_key) as Total_Products
From gold.dim_products
Group by Category
Order By Total_Products Desc

---Average cost for each Category

Select 
	Category,
	Avg(Cost) as Average_Cost
From gold.dim_products
Group by Category
Order By Avg(Cost) Desc

--Total Revenue Generated for Each Category

Select
	p.Category,
	Sum(f.Sales) as Total_Revenue
From gold.Fact_sales f
Left Join gold.dim_products p
	on p.product_key = f.product_key
Group by p.category
Order by Sum(f.sales) Desc


--Total Revenue genenrated by each Customer

Select
	c.Customer_key,
	c.First_Name+' '+C.Last_name as Customer_Name,
	Sum(f.sales) as Total_Revenue
From gold.fact_sales f
Left Join gold.dim_customers c
	on c.Customer_key = f.Customer_key	
Group by 
	c.Customer_key,
	c.First_Name,
	C.Last_name
Order by Sum(f.sales) Desc
Go
--Distribution fo Sold items across Countries

Select
	c.Country,
	Sum(f.sales) as Total_Revenue
From gold.fact_sales f
Left Join gold.dim_customers c
	on c.Customer_key = f.Customer_key	
Group by c.Country
Order by Sum(f.sales) Desc

---Ranking Analytics

--Top 5 Revenue Generating Products

Select *
From
(
	Select 
		p.Product_name,
		Sum(f.Sales) as Total_Revenue,
		Row_Number() over(Order by Sum(f.Sales) Desc) as Rankings
	From gold.Fact_sales f
	Left Join gold.dim_products p
		on p.product_key = f.product_key
	Group by p.Product_name
)t
Where Rankings <=5


---Advanced Analytics
--Change over Time Analysis

--Sales Performance over Time

Select
	Format(Order_date,'yyyy-MM') As Order_Date,
	Sum(Sales) as Total_Sales,
	Count(Distinct Customer_key) as Total_Customers,
	Sum(Quantity) as Total_Quantity
From gold.fact_sales
Where Order_date is Not Null
Group by Format(Order_date,'yyyy-MM')
Order by Format(Order_date,'yyyy-MM') Desc

--Cumulative Analytics

--Total Sales for Each month and runnung Total of Sales over time


Select
	Order_date,
	Total_sales,
	Sum(Total_sales) over(Partition By Order_date Order by Order_date) as Running_Total_Year,
	Avg(Average_price) over(Partition By Order_date Order by Order_date) as Average_Running_Price
From
(
	Select
		Datetrunc(month,Order_date) as Order_date,
		Sum(Sales) as Total_sales,
		Avg(Price) as Average_price
	From
		gold.fact_sales
	Where
		Order_date is not NULL
	Group by
		Datetrunc(month,Order_date)
)t

--Performance Analytics Current Measure Vs Targeted Measure

---Yearly performance of products by comparing their sales to both the average sales and also previous years sales

WITH YearlySales AS (
    SELECT 
        YEAR(s.Order_date) AS Year,
        pr.Product_name,
        SUM(s.Sales) AS Total_Yearly_Sales
    FROM gold.fact_sales s
    LEFT JOIN gold.dim_products pr
        ON pr.product_key = s.product_key
    WHERE s.Order_date IS NOT NULL
    GROUP BY YEAR(s.Order_date), pr.Product_name
)

SELECT *,
    AVG(Total_Yearly_Sales) OVER (PARTITION BY Product_name) AS Average_Sales_Throughout,
	Total_Yearly_Sales -  AVG(Total_Yearly_Sales) OVER (PARTITION BY Product_name) as Diff_Average,
	Case When Total_Yearly_Sales -  AVG(Total_Yearly_Sales) OVER (PARTITION BY Product_name) < 0 then 'Below Average'
		 When Total_Yearly_Sales -  AVG(Total_Yearly_Sales) OVER (PARTITION BY Product_name) > 0 then 'Above Average'
		 Else 'Average'
	End As Sales_Rating,
	LAG(Total_Yearly_Sales,1) over(partition by product_name order by Year) as Previous_Year_Sales,
	Total_Yearly_Sales-LAG(Total_Yearly_Sales,1,0) over(partition by product_name order by Year) as Diff_Previous,
	Case When Total_Yearly_Sales-LAG(Total_Yearly_Sales,1,0) over(partition by product_name order by Year) < 0 then 'Below Previous'
		 When Total_Yearly_Sales-LAG(Total_Yearly_Sales,1,0) over(partition by product_name order by Year) > 0 then 'Above Previous'
		 Else 'Same'
	End As Sales_Rating2
FROM YearlySales
ORDER BY Product_name;


---Part To Whole Analytics
---What Categories cntribute the most to total sales

With Category_Sales as 
(
	Select
		p.Category,
		Sum(f.Sales) as Total_Sales
	From
		gold.fact_sales f
	Left Join 
		gold.dim_products p
	On 
		p.product_key=f.product_key
	Group by 
		p.Category
)

Select 
	Category,
	Total_sales,
	Sum(Total_Sales) over() as Overall_Sales,
	Concat(Round(Cast(Total_Sales as Float) /Sum(Total_Sales) over()*100,2),'%') as Percent_of_Total
From
	Category_Sales
Order by 
	Total_Sales Desc


------Data Segmentation
---Segmenting Products into Cost Ranges and Number of Products in each Cost Range
	
With Segments As
(
	Select 
		Product_key,
		Product_name,
		Cost,
		Case When Cost < 100 Then 'Below 100'
			 When Cost Between 100 And 500 Then '100-500'
			 When Cost Between 500 And 1000 Then '500-1000'
			 Else 'Above 1000'
		End Cost_range
	From 
		gold.dim_products
)
Select 
	Cost_range,
	Count(Product_key) as Total_products
From
	Segments
Group by Cost_range
Order by Total_products Desc

--Segmenting Customers based on their history and also their spending habits

With Customer_Behaviour As
(
	Select 
		c.Customer_key,
		Sum(f.Sales) as Total_Spending,
		Min(Order_date) as First_Order,
		Max(Order_date) as Last_Order,
		DateDiff(Month,Min(Order_date),Max(Order_date)) as Customer_Since_Months
	From 
		gold.dim_customers c
	Left Join
		gold.fact_sales f
	On c.Customer_key=f.Customer_key
	Group by c.Customer_key
)

Select
	Customer_Segment,
	Count(Customer_key) as Number_Customers
From
(
	Select 
		Customer_key,
		Total_Spending,
		Customer_Since_Months,
		Case When Customer_Since_Months >= 12 And Total_Spending > 5000 Then 'VIP'
			 When Customer_Since_Months >= 12 And Total_Spending <= 5000 Then 'Regular'
			 Else 'New'
		End As Customer_Segment
	From
		Customer_Behaviour
)t
Group by Customer_Segment
Order by Count(Customer_key) Desc

Go
 ------Generating a report 
 Create View gold.customer_report As
 
 With Base_query As
 (
	Select
		f.Order_number,
		f.Product_key,
		f.Order_date,
		f.quantity,
		f.Sales,
		c.Customer_key,
		c.Customer_number,
		Concat(c.first_name,' ',c.last_name) as Customer_name,
		Datediff(Year,c.Birthdate,Getdate()) as Age
	From 
		gold.fact_sales f
	Left Join
		gold.dim_customers c
	On
		C.Customer_key = f.Customer_key
	Where 
		Order_date is Not NULL
),
Customer_aggregation As
(
	Select
		Customer_key,
		Customer_name,
		Customer_number,
		Age,
		Count(Distinct order_number) As Total_orders,
		Sum(Sales) as Total_Sales,
		Sum(Quantity) As Total_Quantity,
		Count(Distinct Product_key) As Total_Products,
		Max(Order_date) As Last_Order_date,
		DateDiff(Month,Min(Order_date),Max(Order_date)) as Customer_Since_Months
	From 
		Base_query
	Group by
	
		Customer_name,
		Customer_number,
		Customer_key,
		Age
	
)

Select
	Customer_name,
	Customer_number,
	Customer_key,
	Age,
	Case When Age < 20 Then 'Under 20'
		 When Age Between 20 And 29 Then '20-29'
		 When Age Between 30 and 39 Then '30-39'
		 Else '40 and Above'
	End As Age_Bracket,
	Case When Customer_Since_Months >= 12 And Total_Sales > 5000 Then 'VIP'
			 When Customer_Since_Months >= 12 And Total_Sales <= 5000 Then 'Regular'
			 Else 'New'
	End As Customer_Segment,
	Total_orders,
	Total_Sales,
	Total_Quantity,
	Total_Products,
	Last_Order_date,
	Customer_Since_Months,
	DateDiff(month,Last_Order_date,Getdate()) as Order_recency,
	Case When Total_Sales = 0 Then 0
		 Else Total_Sales/Total_orders 
	End as Average_Order 
From 
	Customer_aggregation


select * from gold.customer_report
Go