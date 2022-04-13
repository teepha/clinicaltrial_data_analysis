# Databricks notebook source
# MAGIC %md
# MAGIC <b>LOADING THE DATA<b>

# COMMAND ----------

dataset_year = str(2019)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# DO NOT CHANGE THE CONTENT OF THIS CELL
import os
import sys

fileroot = "clinicaltrial_" + dataset_year + "_csv"
renamed_fileroot = "clinicaltrial_" + dataset_year + ".csv"
mesh_csv = "/FileStore/tables/mesh.csv"
pharma_csv = "/FileStore/tables/pharma.csv"

# if 'dbruntime.dbutils' in sys.modules.keys():
try:
    dbutils.fs.ls("/FileStore/tables/" + renamed_fileroot)
except:
    dbutils.fs.cp("/FileStore/tables/" + fileroot + ".gz", "file:/tmp/")
    os.environ['fileroot'] = fileroot

# COMMAND ----------

# MAGIC %sh
# MAGIC gunzip /tmp/ /tmp/$fileroot.gz

# COMMAND ----------

try:
    dbutils.fs.ls("file:/tmp/" + fileroot)
    dbutils.fs.mv("file:/tmp/" + fileroot, "/FileStore/tables/" + renamed_fileroot, True)
except:
    pass

# COMMAND ----------

clinical_csv = "/FileStore/tables/" + renamed_fileroot
mesh_csv = "/FileStore/tables/mesh.csv"
pharma_csv = "/FileStore/tables/pharma.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC NOT SURE YET

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/")

# COMMAND ----------

clinicalDF = spark.read.option("header","true").option("inferSchema", "true").option("delimiter", '|').csv(clinical_csv)
meshDF = spark.read.option("header","true").option("inferSchema", "true").csv(mesh_csv)
pharmaDF = spark.read.option("header","true").option("inferSchema", "true").csv(pharma_csv)

# COMMAND ----------

clinicalDF.createOrReplaceTempView("clinicaltrial_view")
meshDF.createOrReplaceTempView("mesh_view")
pharmaDF.createOrReplaceTempView("pharma_view")
