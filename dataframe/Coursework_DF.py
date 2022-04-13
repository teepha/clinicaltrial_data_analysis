# Databricks notebook source
# MAGIC %md
# MAGIC <b> LOADING THE DATA <b>

# COMMAND ----------

dataset_year = str(2019)

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

dbutils.fs.ls("/FileStore/tables/")

# COMMAND ----------

clinical_csv = "/FileStore/tables/" + renamed_fileroot
mesh_csv = "/FileStore/tables/mesh.csv"
pharma_csv = "/FileStore/tables/pharma.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC <b>PREPARE THE DATA<b>

# COMMAND ----------

# MAGIC %md
# MAGIC USING DATAFRAME

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

clinicalDF = spark.read.option("header","true").option("inferSchema", "true").option("delimiter", '|').csv(clinical_csv)
clinicalDF.show(10)

# COMMAND ----------

meshDF = spark.read.option("header","true").option("inferSchema", "true").csv(mesh_csv)
meshDF.show(10)

# COMMAND ----------

pharmaDF = spark.read.option("header","true").option("inferSchema", "true").csv(pharma_csv)
display(pharmaDF)

# COMMAND ----------

# MAGIC %md
# MAGIC MAYBE START FROM HERE

# COMMAND ----------

# MAGIC %md
# MAGIC <b>ANALYSE THE DATA<b>

# COMMAND ----------

# QUESTION1 - The number of studies in the dataset
clinicalDF.distinct().count()

# COMMAND ----------

# QUESTION2 - List all the Type of studies in the dataset along with the frequencies of each type
typesFromDF = clinicalDF.groupBy("Type").count().orderBy("count", ascending=False)
typesFromDF.show()

# COMMAND ----------

# QUESTION3 - The top 5 Conditions with their frequencies
splitConditionsFromDF = split(regexp_replace(col("Conditions"), "(^\[)|(\]$)", ""), ",")
explodeConditionsFromDF = clinicalDF.withColumn(
    "Conditions", explode(splitConditionsFromDF)
)

topConditionsFromDF = explodeConditionsFromDF.groupBy("Conditions").count().orderBy("count", ascending=False)
topConditionsFromDF.show(5, truncate=False)

# COMMAND ----------

# QUESTION4 - The 5 most frequent roots from the hierarchy codes
rootsFromDF = meshDF.join(explodeConditionsFromDF, meshDF.term == explodeConditionsFromDF.Conditions).\
                    select("tree", "term", "Conditions").withColumn('tree', substring('tree', 1,3)).\
                    groupBy("tree").count().orderBy("count", ascending=False)
rootsFromDF.show(5)

# COMMAND ----------

# QUESTION 5 - The 10 most common sponsors that are not pharmaceutical companies with the number of clinical trials they have sponsored
nonPharmaSponsorsDF = clinicalDF.join(pharmaDF, pharmaDF.Parent_Company == clinicalDF.Sponsor, "leftanti")
mostCommonNonPharmaSponsorsDF = nonPharmaSponsorsDF.filter(nonPharmaSponsorsDF.Status!="Active").groupBy("Sponsor").count().orderBy("count", ascending=False)

mostCommonNonPharmaSponsorsDF.show(10, truncate=False)

# COMMAND ----------

clinicalDF.printSchema()

# COMMAND ----------

# QUESTION 6 - Number of completed studies each month in a given year
completedStudiesDF = clinicalDF.filter(clinicalDF.Status=="Completed").filter(col("Completion").endswith(dataset_year)).\
                                    withColumn('Completion', substring('Completion', 1,3)).groupBy("Completion").count()
numberOfCompletedStudiesDF = completedStudiesDF.sort(unix_timestamp(col("Completion"),"MMM"))

numberOfCompletedStudiesDF.show()

# COMMAND ----------

display(numberOfCompletedStudiesDF)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>VISUALISE RESULTS<b>

# COMMAND ----------

# Values to Plot on Bar chart
months = numberOfCompletedStudiesDF.select("Completion").rdd.map(lambda row: row["Completion"]).collect()
counts = numberOfCompletedStudiesDF.select("count").rdd.map(lambda row: row["count"]).collect()

# COMMAND ----------

import matplotlib.pyplot as plt

fig = plt.figure()
ax = fig.add_axes([0,0,1.2,1.2])
ax.bar(months,counts)

plt.xlabel('Months')
plt.ylabel('Number of Completed studies')
plt.title('Completed studies each month in a given year')

plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC <b>USING RDD<b>

# COMMAND ----------



# COMMAND ----------


