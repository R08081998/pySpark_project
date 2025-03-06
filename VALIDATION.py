# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.adlssalesprojectrushii.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.adlssalesprojectrushii.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.adlssalesprojectrushii.dfs.core.windows.net", "a8447339-298e-4df1-a413-122ad1739f7c")
spark.conf.set("fs.azure.account.oauth2.client.secret.adlssalesprojectrushii.dfs.core.windows.net", "Rbe8Q~TjfoiKJTZ0oBhaeNvSUw2EeTyD0rn_icgG")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.adlssalesprojectrushii.dfs.core.windows.net", "https://login.microsoftonline.com/80ceb1a5-902a-4df4-a03d-04826ae7ec7d/oauth2/token")

# COMMAND ----------

COUNTRY = spark.read.format("csv").option("inferschema","true").option("header","true").load("abfss://bronze@adlssalesprojectrushii.dfs.core.windows.net/Target_Folder_Path/COUNTRY")

# COMMAND ----------

COUNTRY = spark.read.format("csv").option("inferschema","true").option("header","true").load("abfss://bronze@adlssalesprojectrushii.dfs.core.windows.net/Target_Folder_Path/COUNTRY/COUNTRY_20240311181949.csv")

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType ,DateType

spark = SparkSession.builder \
    .appName("Create DataFrame from CSV") \
    .getOrCreate()

schema = StructType([
    StructField("COUNTRY_CODE", StringType(), True),
    StructField("COUNTRY_ID", StringType(), True),
    StructField("COUNTRY_NAME", StringType(), True)
])

data = [
    """1003|""USA""|""United States""",
    """1004|""CAN""|""Canada""",
    """1020|""MEX""|""Mexico""",
    """1021|""BRA""|""Brazil""",
    """4011|""JPN""|""Japan""",
    """4012|""SGP""|""Singapore""",
    """4013|""KOR""|""Korea""",
    """4014|""CHN""|""China""",
    """4016|""AUS""|""Australia""",
    """5007|""NLD""|""Netherlands""",
    """5010|""SWE""|""Sweden""",
    """5018|""DNK""|""Denmark""",
    """5022|""FIN""|""Finland""",
    """6001|""FRA""|""France""",
    """6002|""DEU""|""Germany""",
    """6008|""CHE""|""Switzerland""",
    """6009|""GBR""|""United Kingdom""",
    """6017|""BEL""|""Belgium""",
    """7005|""AUT""|""Austria""",
    """7006|""ITA""|""Italy""",
    """7019|""ESP""|""Spain"""
]

rdd = spark.sparkContext.parallelize(data)
country = spark.createDataFrame(rdd.map(lambda x: x.split('|')), schema)

country.show()

from pyspark.sql.functions import *

country = country.withColumn("COUNTRY_ID", regexp_replace(country["COUNTRY_ID"], '"', "")) \
  .withColumn("COUNTRY_NAME", regexp_replace(country["COUNTRY_NAME"], '"', ""))

country.show()

country = country.withColumn("SOURCE_ID", lit(None).cast(StringType())) \
         .withColumn("DataDate", current_timestamp()) \
         .withColumn("UpdateDate", lit(None).cast(DateType()))
country = country.withColumn("SOURCE_ID",lit("LKP File"))

# write data in gen2
country = country.repartition(1)
country.write \
    .format("csv") \
    .mode("overwrite") \
    .option("header", "true") \
    .option("inferschema","true") \
    .save("abfss://bronze@adlssalesprojectrushii.dfs.core.windows.net/VALID_DATA/COUNTRY")


display(country)

# COMMAND ----------

INVLS = spark.read.format("csv").option("inferschema","true").option("header","true").load("abfss://bronze@adlssalesprojectrushii.dfs.core.windows.net/Target_Folder_Path/INVENTORY_LEVELS")


# COMMAND ----------

from pyspark.sql.functions import col ,isnull
NULL_RECORD = INVLS.filter(col("INVENTORY_YEAR").isNull())
display(NULL_RECORD)
/*NULL_RECORD.write.format("csv").option("inferschema","true").option("header","true").save("path or mount_point")*/
# COMMAND ----------

PRODUCT = spark.read.format("csv").option("inferschema","true").option("header","true").load("abfss://bronze@adlssalesprojectrushii.dfs.core.windows.net/Target_Folder_Path/PRODUCT")

# COMMAND ----------

from pyspark.sql.functions import col 
not_present_in_product = INVLS.join(PRODUCT,INVLS["PRODUCT_NUMBER"] == PRODUCT["PRODUCT_NUMBER"],"left_anti" )
display(not_present_in_product)

=  onlay record match
==  record match and data type match
=== record match and data type match as well col name is also match

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp
from pyspark.sql.types import StringType, DateType
INVLS_add = INVLS.withColumn("SOURCE_ID", lit("INTERNAL_DB").cast(StringType())) \
         .withColumn("DataDate", current_timestamp()) \
         .withColumn("UpdateDate", lit(None).cast(DateType()))
display(INVLS_add)

# COMMAND ----------

INVLS_add = INVLS_add.repartition(1)
INVLS_add.write \
    .format("csv") \
    .mode("overwrite") \
    .option("header", "true") \
    .option("inferschema","true") \
    .save("abfss://bronze@adlssalesprojectrushii.dfs.core.windows.net/VALID_DATA/INVENTORY_LEVELS")

# COMMAND ----------

ORDER_DETAILS = spark.read.format("csv").option("inferschema","true").option("header","true").load("abfss://bronze@adlssalesprojectrushii.dfs.core.windows.net/Target_Folder_Path/ORDER_DETAILS")
display(ORDER_DETAILS)

# COMMAND ----------

from pyspark.sql.functions import col
order_deatils_null = ORDER_DETAILS.filter(col("ORDER_DETAIL_CODE").isNull())
display(order_deatils_null)

# COMMAND ----------

ORH = spark.read.format("csv").option("inferschema","true").option("header","true").load("abfss://bronze@adlssalesprojectrushii.dfs.core.windows.net/Target_Folder_Path/ORDER_HEADER")
display(ORH)

# COMMAND ----------

from pyspark.sql.functions import col

# Find records in ORDER_DETAILS that are not in ORH
rejected_records = ORDER_DETAILS.join(ORH, ORDER_DETAILS["ORDER_NUMBER"] == ORH["ORDER_NUMBER"], "left_anti")

# Filter ORDER_DETAILS DataFrame
filtered_order_details = ORDER_DETAILS.filter(
    ((col("QUANTITY").isNotNull()) & (col("QUANTITY") > 0)) &
    ((col("UNIT_COST").isNotNull()) & (col("UNIT_COST") > 0)) &
    ((col("UNIT_PRICE").isNotNull()) & (col("UNIT_PRICE") > 0)) &
    ((col("UNIT_SALE_PRICE").isNotNull()) & (col("UNIT_SALE_PRICE") > 0))
)

rejected_records = filtered_order_details.join(ORH, ORDER_DETAILS["ORDER_NUMBER"] == ORH["ORDER_NUMBER"], "left_anti")


# Perform an inner join with ORH
combined_result = filtered_order_details.join(ORH, filtered_order_details["ORDER_NUMBER"] == ORH["ORDER_NUMBER"])

# Select only the columns from ORDER_DETAILS DataFrame
combined_result_selected = combined_result \
    .select(ORDER_DETAILS.ORDER_DETAIL_CODE,ORDER_DETAILS.ORDER_NUMBER,ORDER_DETAILS.SHIP_DATE,ORDER_DETAILS.PRODUCT_NUMBER,ORDER_DETAILS.PROMOTION_CODE,ORDER_DETAILS.QUANTITY,ORDER_DETAILS.UNIT_COST,ORDER_DETAILS.UNIT_PRICE,ORDER_DETAILS.UNIT_SALE_PRICE,ORDER_DETAILS.modifydate)

# Display the resulting DataFrame
display(combined_result_selected)


# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp
from pyspark.sql.types import StringType, DateType
combined_result_selected = combined_result_selected.withColumn("SOURCE_ID", lit("INTERNAL_DB").cast(StringType())) \
         .withColumn("DataDate", current_timestamp()) \
         .withColumn("UpdateDate", lit(None).cast(DateType()))
display(combined_result_selected)

# COMMAND ----------

combined_result_selected = combined_result_selected.repartition(1)
combined_result_selected.write \
    .format("csv") \
    .mode("overwrite") \
    .option("header", "true") \
    .option("inferschema","true") \
    .save("abfss://bronze@adlssalesprojectrushii.dfs.core.windows.net/VALID_DATA/ORDER_DETAILS")

# COMMAND ----------

ORDER_HEADER = spark.read.format("csv").option("inferschema","true").option("header","true").load("abfss://bronze@adlssalesprojectrushii.dfs.core.windows.net/Target_Folder_Path/ORDER_HEADER")
display(ORDER_HEADER)

# COMMAND ----------

from pyspark.sql.functions import col
ORH = ORDER_HEADER.filter(col("ORDER_NUMBER").isNull())
display(ORH)

# COMMAND ----------

from pyspark.sql.functions import col
ORH = ORDER_HEADER.filter(col("ORDER_NUMBER").isNull() & col("RETAILER_NAME").isNull() & col("ORDER_DATE").isNull())
display(ORH)

# COMMAND ----------

from pyspark.sql.functions import col
ORH = ORDER_HEADER.groupBy("ORDER_NUMBER").count().filter(col("count") > 1)
display(ORH)

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp, *
from pyspark.sql.types import StringType, DateType
ADD_COL_NEW_REC = ORDER_HEADER.withColumn("SOURCE_ID", lit("INTERNAL_DB").cast(StringType())) \
         .withColumn("DataDate", current_timestamp()) \
         .withColumn("UpdateDate", lit(None).cast(DateType()))
display(ADD_COL_NEW_REC)

# COMMAND ----------

ADD_COL_NEW_REC = ADD_COL_NEW_REC.repartition(1)
ADD_COL_NEW_REC.write \
    .format("csv") \
    .mode("overwrite") \
    .option("header", "true") \
    .option("inferschema","true") \
    .save("abfss://bronze@adlssalesprojectrushii.dfs.core.windows.net/VALID_DATA/ORDER_HEADER")

# COMMAND ----------

ORM = spark.read.format("csv").option("inferschema","true").option("header","true").load("abfss://bronze@adlssalesprojectrushii.dfs.core.windows.net/Target_Folder_Path/ORDER_METHOD")
display(ORM)

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp
from pyspark.sql.types import StringType, DateType
ORM_add = ORM.withColumn("SOURCE_ID", lit("LKP_File").cast(StringType())) \
         .withColumn("DataDate", current_timestamp()) \
         .withColumn("UpdateDate", lit(None).cast(DateType()))
display(ORM_add)

# COMMAND ----------

ORM_add = ORM_add.repartition(1)
ORM_add.write \
    .format("csv") \
    .mode("overwrite") \
    .option("header", "true") \
    .option("inferschema","true") \
    .save("abfss://bronze@adlssalesprojectrushii.dfs.core.windows.net/VALID_DATA/ORDER_METHOD")

# COMMAND ----------

PRODUCT = spark.read.format("csv").option("inferschema","true").option("header","true").load("abfss://bronze@adlssalesprojectrushii.dfs.core.windows.net/Target_Folder_Path/PRODUCT")
display(PRODUCT)

# COMMAND ----------

from pyspark.sql.functions import col
PRODUCT_null = PRODUCT.filter(col("PRODUCT_NUMBER").isNull())
display(PRODUCT_null)

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp
from pyspark.sql.types import StringType, DateType
PRODUCT_add = PRODUCT.withColumn("SOURCE_ID", lit("INTERNAL_DB").cast(StringType())) \
         .withColumn("DataDate", current_timestamp()) \
         .withColumn("UpdateDate", lit(None).cast(DateType()))
display(PRODUCT_add)

# COMMAND ----------

PRODUCT_add = PRODUCT_add.repartition(1)
PRODUCT_add.write \
    .format("csv") \
    .mode("overwrite") \
    .option("header", "true") \
    .option("inferschema","true") \
    .save("abfss://bronze@adlssalesprojectrushii.dfs.core.windows.net/VALID_DATA/PRODUCT")

# COMMAND ----------

PROD_NAME = spark.read.format("csv").option("inferschema","true").option("header","true").load("abfss://bronze@adlssalesprojectrushii.dfs.core.windows.net/Target_Folder_Path/PRODUCT_NAME_LOOKUP")
display(PROD_NAME)

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp
from pyspark.sql.types import StringType, DateType
PROD_NAME_add = PROD_NAME.withColumn("SOURCE_ID", lit("LKP_File").cast(StringType())) \
         .withColumn("DataDate", current_timestamp()) \
         .withColumn("UpdateDate", lit(None).cast(DateType()))

display(PROD_NAME_add)

# COMMAND ----------

PROD_NAME_add = PROD_NAME_add.repartition(1)
PROD_NAME_add.write \
    .format("csv") \
    .mode("overwrite") \
    .option("header", "true") \
    .option("inferschema","true") \
    .save("abfss://bronze@adlssalesprojectrushii.dfs.core.windows.net/VALID_DATA/PRODUCT_NAME")

# COMMAND ----------

RTLR = spark.read.format("csv").option("inferschema","true").option("header","true").load("abfss://bronze@adlssalesprojectrushii.dfs.core.windows.net/Target_Folder_Path/Retailer")
display(RTLR)

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp
from pyspark.sql.types import StringType, DateType
RTLR_add = RTLR.withColumn("SOURCE_ID", lit("LKP_File").cast(StringType())) \
         .withColumn("DataDate", current_timestamp()) \
         .withColumn("UpdateDate", lit(None).cast(DateType()))
display(RTLR_add)

# COMMAND ----------

RTLR_add = RTLR_add.repartition(1)
RTLR_add.write \
    .format("csv") \
    .mode("overwrite") \
    .option("header", "true") \
    .option("inferschema","true") \
    .save("abfss://bronze@adlssalesprojectrushii.dfs.core.windows.net/VALID_DATA/RETAILER")

# COMMAND ----------

RITEM = spark.read.format("csv").option("inferschema","true").option("header","true").load("abfss://bronze@adlssalesprojectrushii.dfs.core.windows.net/Target_Folder_Path/RETURNED_ITEM")
display(RITEM)

# COMMAND ----------

from pyspark.sql.functions import col , isnull
null_record = RITEM.filter(col("RETURN_CODE").isNull())
display(null_record)

# COMMAND ----------

ORDER_DETAILS = spark.read.format("csv").option("inferschema","true").option("header","true").load("abfss://bronze@adlssalesprojectrushii.dfs.core.windows.net/Target_Folder_Path/ORDER_DETAILS")
display(ORDER_DETAILS)

# COMMAND ----------

from pyspark.sql.functions import col
not_prsent_in_order_details = RITEM.join(ORDER_DETAILS,RITEM ["ORDER_DETAIL_CODE"] == ORDER_DETAILS["ORDER_DETAIL_CODE"],"left_anti")
#valid_record = RITEM.exceptAll(not_prsent_in_order_details)
display(not_prsent_in_order_details)

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp
from pyspark.sql.types import StringType, DateType
RITEM_add = RITEM.withColumn("SOURCE_ID", lit("INTERNAL_DB").cast(StringType())) \
         .withColumn("DataDate", current_timestamp()) \
         .withColumn("UpdateDate", lit(None).cast(DateType()))
display(RITEM_add)

# COMMAND ----------

RITEM_add = RITEM_add.repartition(1)
RITEM_add.write \
    .format("csv") \
    .mode("overwrite") \
    .option("header", "true") \
    .option("inferschema","true") \
    .save("abfss://bronze@adlssalesprojectrushii.dfs.core.windows.net/VALID_DATA/RETURN_ITEM")

# COMMAND ----------

RR = spark.read.format("csv").option("inferschema","true").option("header","true").load("abfss://bronze@adlssalesprojectrushii.dfs.core.windows.net/Target_Folder_Path/RETURN_REASON")
display(RR)

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp
from pyspark.sql.types import StringType, DateType
RR_add = RR.withColumn("SOURCE_ID", lit("LKP_File").cast(StringType())) \
         .withColumn("DataDate", current_timestamp()) \
         .withColumn("UpdateDate", lit(None).cast(DateType()))
display(RR_add)

# COMMAND ----------

RR_add = RR_add.repartition(1)
RR_add.write \
    .format("csv") \
    .mode("overwrite") \
    .option("header", "true") \
    .option("inferschema","true") \
    .save("abfss://bronze@adlssalesprojectrushii.dfs.core.windows.net/VALID_DATA/RETURN_REASON")

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, lit, current_timestamp
from pyspark.sql.types import StringType, DateType

spark = SparkSession.builder \
    .appName("Split Columns by Delimiter") \
    .getOrCreate()

data_path = "abfss://bronze@adlssalesprojectrushii.dfs.core.windows.net/Target_Folder_Path/WAREHOUSE"
df = spark.read.option("header", "true").csv(data_path, sep="\t")

WAREHOUSE = df.withColumn("BRANCH_CODE", split(df["BRANCH_CODE"], "\t").getItem(0)) \
    .withColumn("ADDRESS1", split(df["ADDRESS1"], "\t").getItem(0)) \
    .withColumn("CITY", split(df["CITY"], "\t").getItem(0)) \
    .withColumn("POSTAL_ZONE", split(df["POSTAL_ZONE"], "\t").getItem(0)) \
    .withColumn("COUNTRY_CODE", split(df["COUNTRY_CODE"], "\t").getItem(0)) \
    .withColumn("WAREHOUSE_BRANCH_CODE", split(df["WAREHOUSE_BRANCH_CODE"], "\t").getItem(0))

WAREHOUSE.show(truncate=False)

WAREHOUSE_add = WAREHOUSE.withColumn("SOURCE_ID", lit("LKP_File").cast(StringType())) \
    .withColumn("DataDate", current_timestamp()) \
    .withColumn("UpdateDate", lit(None).cast(DateType()))
display(WAREHOUSE_add)


# COMMAND ----------

WAREHOUSE_add = WAREHOUSE_add.repartition(1)
WAREHOUSE_add.write \
    .format("csv") \
    .mode("overwrite") \
    .option("header", "true") \
    .option("inferschema","true") \
    .save("abfss://bronze@adlssalesprojectrushii.dfs.core.windows.net/VALID_DATA/WAREHOUSE")
    
   
