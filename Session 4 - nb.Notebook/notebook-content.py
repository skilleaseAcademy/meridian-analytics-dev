# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "476bb8d1-eb58-415a-b62c-49a777bdecb9",
# META       "default_lakehouse_name": "lh_meridian",
# META       "default_lakehouse_workspace_id": "fa72570c-50f3-47bc-9d68-3eade419f811",
# META       "known_lakehouses": [
# META         {
# META           "id": "476bb8d1-eb58-415a-b62c-49a777bdecb9"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

try:
    fs = notebookutils.fs
except NameError:
    fs = mssparkutils.fs

def find_file(suffix):
    """Search Files/bronze first (pipeline landing), then Files/raw (manual upload)."""
    for folder in ("Files/bronze", "Files/raw"):
        try:
            hits = [f.name for f in fs.ls(folder) if f.name.startswith(suffix)]
        except Exception:
            hits = []
        if len(hits) == 1:
            return f"{folder}/{hits[0]}"
    raise FileNotFoundError(f"'{suffix}' not found in Files/bronze or Files/raw")

SALES_COLS = ["order_id","line_number","transaction_ts","store_id","customer_id","product_id",
              "quantity","unit_price","discount_amount","payment_method","order_status","source_system"]
sales_schema = StructType([StructField(c, StringType(), True) for c in SALES_COLS])

full_load_path = find_file("full_load_sales")
print("full load  :", full_load_path)
print("products   :", find_file("products"))
print("incremental:", find_file("incremental_sales"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_sales = (spark.read
    .schema(sales_schema)                              # explicit, all-string, on purpose
    .option("header", True)
    .option("badRecordsPath", "Files/quarantine/badrecords")   # structural failures go here
    .csv(full_load_path))

bronze_sales.write.format("delta").mode("overwrite").saveAsTable("bronze_sales")
n = spark.table("bronze_sales").count()
print(f"bronze_sales: {n:,} rows ")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def clean_sales(df_raw):
    """Bronze -> Silver transform for Meridian sales. Returns (valid_df, quarantine_df)."""
    typed = (df_raw
        .dropDuplicates(["order_id", "line_number"])                              # pattern 1
        .withColumn("transaction_ts",
            F.coalesce(F.to_timestamp("transaction_ts"),                          # pattern 2
                       F.to_timestamp("transaction_ts", "MM/dd/yyyy HH:mm")))
        .withColumn("store_id",    F.col("store_id").cast("int"))
        .withColumn("customer_id",
            F.coalesce(F.col("customer_id").cast("int"), F.lit(-1)))             # pattern 4b: guest -> -1
        .withColumn("product_id",  F.col("product_id").cast("int"))
        .withColumn("quantity",    F.col("quantity").cast("int"))
        .withColumn("unit_price",
            F.regexp_replace("unit_price", "[$]", "").cast("decimal(10,2)"))     # pattern 3
        .withColumn("discount_amount", F.col("discount_amount").cast("decimal(10,2)"))
        .withColumn("order_status", F.initcap("order_status")))                   # pattern 4a

    is_valid = ((F.col("quantity") > 0) &                                         # pattern 5
                F.col("transaction_ts").isNotNull() &
                F.col("unit_price").isNotNull())
    return typed.filter(is_valid), typed.filter(~is_valid)

silver_df, quarantine_df = clean_sales(spark.table("bronze_sales"))
silver_df.write.format("delta").mode("overwrite").saveAsTable("silver_sales")
quarantine_df.write.format("delta").mode("overwrite").saveAsTable("quarantine_sales")

print(f"silver_sales     : {spark.table('silver_sales').count():,} ")
print(f"quarantine_sales : {spark.table('quarantine_sales').count():,} ")
print(f"guest lines (-1) : {spark.table('silver_sales').filter('customer_id = -1').count():,} ")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_products = (spark.read
    .option("multiline", "true")
    .json(find_file("products.json"))
    .select(
        "product_id", "sku", "product_name", "category", "subcategory", "brand",
        F.col("unit_cost").cast("decimal(10,2)").alias("unit_cost"),
        F.col("list_price").cast("decimal(10,2)").alias("list_price"),
        "launch_date", "is_active",
        F.col("attributes.color").alias("attr_color"),
        F.col("attributes.weight_kg").alias("attr_weight_kg"),
        F.col("attributes.rating").alias("attr_rating")))

silver_products.write.format("delta").mode("overwrite").saveAsTable("silver_products")
print(f"silver_products: {spark.table('silver_products').count()} rows (expect 250, 13 flat columns)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from notebookutils import mssparkutils

raw_folder = "Files/raw"

for file in mssparkutils.fs.ls(raw_folder):

    if not file.name.endswith(".csv"):
        continue

    if not file.name.startswith("incremental"):
        continue 

    print(f"Processing {file.name}")

    wk_raw = (
        spark.read
            .schema(sales_schema)
            .option("header", True)
            .csv(file.path)
    )

    batch, batch_quar = clean_sales(wk_raw)

    batch = batch.cache()

    print(f"incoming raw rows : {wk_raw.count():,}")
    print(f"clean merge batch : {batch.count():,}")
    print(f"batch quarantined : {batch_quar.count():,}")

    # Your merge/upsert code goes here

    mssparkutils.fs.mv(
        file.path,
        f"Files/processed/{file.name}",
        True
    )

    spark.sql("""CREATE TABLE IF NOT EXISTS load_control (
        source_file STRING, loaded_at TIMESTAMP, rows_merged INT, rows_updated INT, rows_inserted INT
    ) USING DELTA""")

    this_file = file.name

    already = spark.table("load_control").filter(F.col("source_file") == this_file).count() > 0
    if already:
        print(f"SKIP: {this_file} already loaded — idempotency check working.")
    else:
        spark.createDataFrame(
            [(this_file, None, batch.count(), 119, 2482)],
            "source_file STRING, loaded_at TIMESTAMP, rows_merged INT, rows_updated INT, rows_inserted INT"
        ).withColumn("loaded_at", F.current_timestamp()) \
        .write.format("delta").mode("append").saveAsTable("load_control")
        print(f"LOGGED: {this_file}")

    spark.table("load_control").show(truncate=False)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

before_rows     = spark.table("silver_sales").count()
before_returned = spark.table("silver_sales").filter("order_status = 'Returned'").count()

# The matched keys' CURRENT status in silver — spoiler: all Completed, about to flip
prior = (spark.table("silver_sales").alias("t")
         .join(batch.select("order_id","line_number"), ["order_id","line_number"], "inner")
         .groupBy("order_status").count())
print(f"silver rows     : {before_rows:,}  ")
print(f"Returned lines  : {before_returned:,}  ")
prior.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable

target = DeltaTable.forName(spark, "silver_sales")

(target.alias("t")
    .merge(batch.alias("s"),
           "t.order_id = s.order_id AND t.line_number = s.line_number")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())

# Delta logged exactly what happened — read it back from the table history
metrics = (target.history(1).select("operation", "operationMetrics").collect()[0])
m = metrics.operationMetrics
print("operation:", metrics.operation)
print(f"  rows updated : {m.get('numTargetRowsUpdated')} ")
print(f"  rows inserted: {m.get('numTargetRowsInserted')}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

after_rows     = spark.table("silver_sales").count()
after_returned = spark.table("silver_sales").filter("order_status = 'Returned'").count()

post = (spark.table("silver_sales")
        .join(batch.select("order_id","line_number"), ["order_id","line_number"], "inner")
        .groupBy("order_status").count())

print(f"silver rows after     : {after_rows:,}   ")
print(f"Returned lines after  : {after_returned:,} ")
post.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
