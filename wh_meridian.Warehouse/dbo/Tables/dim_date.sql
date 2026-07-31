CREATE TABLE [dbo].[dim_date] (

	[date_key] int NOT NULL, 
	[date] date NULL, 
	[year] int NULL, 
	[quarter] int NULL, 
	[quarter_label] varchar(8) NULL, 
	[month_number] int NULL, 
	[month_name] varchar(15) NULL, 
	[month_short] varchar(3) NULL, 
	[iso_week] int NULL, 
	[day_of_month] int NULL, 
	[day_of_week] int NULL, 
	[day_name] varchar(10) NULL, 
	[is_weekend] int NOT NULL, 
	[fiscal_year] varchar(6) NULL
);


GO
ALTER TABLE [dbo].[dim_date] ADD CONSTRAINT pk_dim_date primary key NONCLUSTERED ([date_key]);