# Databricks notebook source
# MAGIC %md
# MAGIC <b>LOADING THE DATA<b>

# COMMAND ----------

# MAGIC %md
# MAGIC CLINICAL TRIAL DATA

# COMMAND ----------

# CLINICAL TRIAL YEAR
clinicaltrial_year = '2021'

# COMMAND ----------

# DO NOT CHANGE THE CONTENT OF THIS CELL
import sys
if not 'dbruntime.dbutils' in sys.modules.keys():
    import pyspark
    sc = pyspark.SparkContext()

# COMMAND ----------

# DO NOT CHANGE THE CONTENT OF THIS CELL  
import os
import sys

fileroot = "clinicaltrial_" + clinicaltrial_year + "_csv"
clinicaltrial_csv = "/FileStore/tables/clinicaltrial_" + clinicaltrial_year + ".csv"

os.environ['fileroot'] = "false"

try:
    dbutils.fs.ls(clinicaltrial_csv)
except:
    dbutils.fs.cp("/FileStore/tables/" + fileroot + ".gz", "file:/tmp/")
    os.environ['fileroot'] = fileroot

# COMMAND ----------

# MAGIC %sh
# MAGIC gunzip /tmp/ /tmp/$fileroot.gz

# COMMAND ----------

# DO NOT CHANGE THE CONTENT OF THIS CELL
if 'dbruntime.dbutils' in sys.modules.keys():
    try:
        dbutils.fs.ls(clinicaltrial_csv)
    except:
        dbutils.fs.cp("file:/tmp/" + fileroot, clinicaltrial_csv)

# COMMAND ----------

# MAGIC %md
# MAGIC MESH AND PHARMA DATA

# COMMAND ----------

# DO NOT CHANGE THE CONTENT OF THIS CELL
# THE MESH AND PHARMA FILES GET REMOVED AFTER THE DATA IS LOADED, THIS WOULD HELP FIX THE ISSUE, BY COPYING THE FILE TO ANOTHER LOCATION
mesh_csv = "mesh.csv"
pharma_csv = "pharma.csv"

if 'dbruntime.dbutils' in sys.modules.keys():
    try:
        dbutils.fs.ls("/FileStore/" + mesh_csv)
        dbutils.fs.ls("/FileStore/" + pharma_csv)
    except:
        dbutils.fs.cp("/FileStore/tables/" + mesh_csv, "/FileStore/" + mesh_csv)
        dbutils.fs.cp("/FileStore/tables/" + pharma_csv, "/FileStore/" + pharma_csv)

# COMMAND ----------


