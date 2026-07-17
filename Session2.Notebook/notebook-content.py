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

# Welcome to your new notebook
# Type here in the cell editor to add code!
try:
    fs = notebookutils.fs
except NameError:
    fs = mssparkutils.fs

try:
    for f in fs.ls("Files/raw"):
        print(f"{f.name:52s} {f.size/1e6:8.2f} MB")
except Exception as e:
    print("Files/raw not found →", str(e)[:120])
    print("FIX: attach lh_meridian as the DEFAULT lakehouse, or use the full abfss:// path.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *

# customer_id,customer_code,first_name,last_name,email,
# segment,preferred_channel,home_region,join_date,age_band,is_churned

customer_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("customer_code", IntegerType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("preferred_channel", StringType(), True),
    StructField("home_region", StringType(), True),
    StructField("join_date", DateType(), True),
    StructField("age_band", StringType(), True),
    StructField("is_churned", BooleanType(), True)
])

df_customer = (spark.read
    .schema(customer_schema)
    .option("header", True)
    .csv("Files/raw/dim_customer.csv"))

df_customer.write.format("delta").mode("overwrite").saveAsTable("bronze_customer")

print("bronze_customer created under Tables/")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tbl_path = spark.sql("DESCRIBE DETAIL bronze_customer").collect()[0]["location"]
print("table location:", tbl_path)

for f in fs.ls(tbl_path):
    print(f.name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

log0 = spark.read.json(f"{tbl_path}/_delta_log/00000000000000000000.json")
log0.printSchema()
log0.select("commitInfo", "metaData", "add").show(truncate=100, vertical=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- 2c. One-stop metadata: note format=delta, numFiles, sizeInBytes, location
# MAGIC DESCRIBE DETAIL bronze_customer

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_sales_raw = (spark.read
    .option("header", True)
    .csv("Files/raw/full_load_sales_asof_2026-07-14.csv"))   # all-string on purpose: it's raw

print("rows:", df_sales_raw.count())   # expected: 361,892 (includes seeded duplicates)

df_sales_raw.write.format("delta").mode("overwrite").saveAsTable("bronze_sales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT COUNT(*) AS rows, COUNT(DISTINCT order_id) AS orders
# MAGIC FROM bronze_sales

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Legal append: SUBSET of columns (names/types match) — Delta accepts and NULL-fills the rest
subset_row = spark.createDataFrame(
    [(9999, "Test", "User")],
    "customer_id INT, first_name STRING, last_name STRING")

subset_row.write.format("delta").mode("append").saveAsTable("bronze_customer")
spark.sql("SELECT * FROM bronze_customer WHERE customer_id = 9999").show(vertical=True, truncate=40)
# ^ inserted, with NULLs in the 8 unsupplied columns — allowed by design

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

extra_col_row = spark.createDataFrame(
    [(9998, "Bad", "Column", "oops")],
    "customer_id INT, first_name STRING, last_name STRING, not_a_real_column STRING")

try:
    extra_col_row.write.format("delta").mode("append").saveAsTable("bronze_customer")
    print("!! append unexpectedly succeeded")
except Exception as e:
    print(type(e).__name__, "→ write rejected (schema mismatch: unknown column)")
    print(str(e)[:300])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

new_col = (spark.table("bronze_customer").limit(3)
           .withColumn("loyalty_tier", F.lit("Gold")))

(new_col.write.format("delta")
    .mode("append")
    .option("mergeSchema", "true")      # the explicit opt-in
    .saveAsTable("bronze_customer"))

spark.table("bronze_customer").printSchema()   # loyalty_tier exists; older rows read as NULL

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC UPDATE bronze_customer SET segment = 'VIP' WHERE customer_id <= 10

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- clean up the subset-demo row before moving on
# MAGIC DELETE FROM bronze_customer WHERE customer_id ='9999' or customer_id = '9998'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DESCRIBE HISTORY bronze_customer
# MAGIC -- columns: version, timestamp, operation (WRITE/UPDATE/DELETE/RESTORE), operationMetrics

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Query the past vs the present — nothing is copied; the log replays an older file set
# MAGIC SELECT 'v0'  AS snapshot, segment, COUNT(*) AS c FROM bronze_customer VERSION AS OF 0 GROUP BY segment
# MAGIC UNION ALL
# MAGIC SELECT 'now' AS snapshot, segment, COUNT(*) AS c FROM bronze_customer GROUP BY segment
# MAGIC ORDER BY snapshot, segment
# MAGIC -- Timestamp form also works:  SELECT ... FROM bronze_customer TIMESTAMP AS OF '2026-07-16 19:30:00'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Roll the whole table back (bad-job recovery without restoring backups)
# MAGIC RESTORE TABLE bronze_customer TO VERSION AS OF 0

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.table("product_shared")
print(df.count())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def probe(key):
    try:
        return spark.conf.get(key)
    except Exception:
        return "<not registered on this runtime>"

for key in ("spark.sql.parquet.vorder.enabled",      # naming on many current runtimes
            "spark.sql.parquet.vorder.default"):     # newer naming on some runtimes
    print(f"{key:42s} = {probe(key)}")

# Turn V-Order ON for this session using whichever key the runtime accepts
for key in ("spark.sql.parquet.vorder.enabled", "spark.sql.parquet.vorder.default"):
    try:
        spark.conf.set(key, "true")
        print("session V-Order enabled via", key)
        break
    except Exception:
        pass

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SHOW TBLPROPERTIES bronze_sales

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC OPTIMIZE bronze_sales ZORDER BY (store_id, product_id)

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import time
t0 = time.time()
spark.sql("""SELECT SUM(CAST(quantity AS INT)) AS units
             FROM bronze_sales WHERE store_id = '7'""").show()
print(f"selective query: {time.time()-t0:.2f}s")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC OPTIMIZE bronze_sales 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC VACUUM bronze_sales RETAIN 168 HOURS

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DESCRIBE HISTORY bronze_sales

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
