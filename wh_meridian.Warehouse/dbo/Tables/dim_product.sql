CREATE TABLE [dbo].[dim_product] (

	[product_key] bigint NOT NULL, 
	[product_id] bigint NULL, 
	[sku] varchar(8000) NULL, 
	[product_name] varchar(8000) NULL, 
	[category] varchar(8000) NULL, 
	[subcategory] varchar(8000) NULL, 
	[brand] varchar(8000) NULL, 
	[unit_cost] decimal(10,2) NULL, 
	[list_price] decimal(10,2) NULL, 
	[is_active] bit NULL
);


GO
ALTER TABLE [dbo].[dim_product] ADD CONSTRAINT pk_dim_product primary key NONCLUSTERED ([product_key]);