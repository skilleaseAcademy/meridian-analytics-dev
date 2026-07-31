CREATE TABLE [dbo].[dim_store] (

	[store_key] bigint NOT NULL, 
	[store_id] int NULL, 
	[store_code] varchar(8000) NULL, 
	[store_name] varchar(8000) NULL, 
	[city] varchar(8000) NULL, 
	[region] varchar(8000) NULL, 
	[store_type] varchar(8000) NULL
);


GO
ALTER TABLE [dbo].[dim_store] ADD CONSTRAINT pk_dim_store primary key NONCLUSTERED ([store_key]);