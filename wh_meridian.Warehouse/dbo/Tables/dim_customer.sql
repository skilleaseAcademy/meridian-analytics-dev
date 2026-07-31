CREATE TABLE [dbo].[dim_customer] (

	[customer_key] bigint NOT NULL, 
	[customer_id] int NULL, 
	[customer_code] varchar(8000) NULL, 
	[first_name] varchar(8000) NULL, 
	[last_name] varchar(8000) NULL, 
	[segment] varchar(8000) NULL, 
	[preferred_channel] varchar(8000) NULL, 
	[home_region] varchar(8000) NULL, 
	[age_band] varchar(8000) NULL, 
	[is_churned] bit NULL
);


GO
ALTER TABLE [dbo].[dim_customer] ADD CONSTRAINT pk_dim_customer primary key NONCLUSTERED ([customer_key]);