CREATE TABLE [dbo].[fact_sales] (

	[date_key] int NULL, 
	[customer_key] bigint NULL, 
	[store_key] bigint NULL, 
	[product_key] bigint NULL, 
	[order_id] varchar(8000) NULL, 
	[line_number] varchar(8000) NULL, 
	[transaction_ts] datetime2(6) NULL, 
	[payment_method] varchar(8000) NULL, 
	[order_status] varchar(8000) NULL, 
	[source_system] varchar(8000) NULL, 
	[quantity] int NULL, 
	[unit_price] decimal(10,2) NULL, 
	[discount_amount] decimal(10,2) NULL, 
	[net_amount] decimal(12,2) NULL, 
	[margin] decimal(12,2) NULL
);