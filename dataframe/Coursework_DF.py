# Databricks notebook source
# MAGIC %md
# MAGIC <b>PREPARE THE DATA<b>

# COMMAND ----------

# MAGIC %run ./Scripts

# COMMAND ----------

# FILE PATHS
clinicaltrial_csv = "/FileStore/tables/clinicaltrial_" + clinicaltrial_year + ".csv"
mesh_csv = "/FileStore/tables/mesh.csv"
pharma_csv = "/FileStore/tables/pharma.csv"

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

clinicaltrialDF = spark.read.option("header","true").option("inferSchema", "true").option("delimiter", '|').csv(clinicaltrial_csv)
clinicaltrialDF.show(10)

# COMMAND ----------

meshDF = spark.read.option("header","true").option("inferSchema", "true").csv(mesh_csv)
meshDF.show(10)

# COMMAND ----------

pharmaDF = spark.read.option("header","true").option("inferSchema", "true").csv(pharma_csv)
display(pharmaDF)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>ANALYSE THE DATA<b>

# COMMAND ----------

# The total number of clinical trial studies
clinicaltrialDF.count()

# COMMAND ----------

# QUESTION 1 - The distinct number of studies in the clinical trial dataset
clinicaltrialDF.distinct().count()

# COMMAND ----------

# QUESTION 2 - List all the Type of studies in the dataset along with the frequencies of each Type
typesFromDF = clinicaltrialDF.groupBy("Type").count().orderBy("count", ascending=False)
typesFromDF.show(truncate=False)

# COMMAND ----------

# QUESTION 3 - The top 5 Conditions with their frequencies
explodeConditionsFromDF = clinicaltrialDF.withColumn(
    "Conditions", explode(split(col("Conditions"), ","))
)

topConditionsFromDF = explodeConditionsFromDF.groupBy("Conditions").count().orderBy("count", ascending=False)
topConditionsFromDF.show(5, truncate=False)

# COMMAND ----------

# QUESTION 4 - The 5 most frequent roots from the hierarchy codes
rootsFromDF = meshDF.join(explodeConditionsFromDF, meshDF.term == explodeConditionsFromDF.Conditions).\
                    select("tree", "term", "Conditions").withColumn('tree', substring('tree', 1,3)).\
                    groupBy("tree").count().orderBy("count", ascending=False)
rootsFromDF.show(5)

# COMMAND ----------

# QUESTION 5 - The 10 most common sponsors that are not pharmaceutical companies with the number of clinical trials they have sponsored
nonPharmaSponsorsDF = clinicaltrialDF.join(pharmaDF, pharmaDF.Parent_Company == clinicaltrialDF.Sponsor, "leftanti")
mostCommonNonPharmaSponsorsDF = nonPharmaSponsorsDF.filter(nonPharmaSponsorsDF.Status!="Active").\
                                                    groupBy("Sponsor").count().orderBy("count", ascending=False)

mostCommonNonPharmaSponsorsDF.show(10, truncate=False)

# COMMAND ----------

# QUESTION 6 - Number of completed studies each month in a given year
completedStudiesDF = clinicaltrialDF.filter(clinicaltrialDF.Status=="Completed").filter(col("Completion").endswith(clinicaltrial_year)).\
                                    withColumn('Completion', substring('Completion', 1,3)).groupBy("Completion").count()
numberOfCompletedStudiesDF = completedStudiesDF.sort(unix_timestamp(col("Completion"),"MMM"))

numberOfCompletedStudiesDF.show()

# COMMAND ----------

# MAGIC %md
# MAGIC <b>VISUALISING THE RESULTS<b>

# COMMAND ----------

# QUESTION 6: Plot number of completed studies each month in a given year
import matplotlib.pyplot as plt

# Values to Plot on Bar chart
months = numberOfCompletedStudiesDF.select("Completion").rdd.map(lambda row: row["Completion"]).collect()
counts = numberOfCompletedStudiesDF.select("count").rdd.map(lambda row: row["count"]).collect()

fig = plt.figure()
ax = fig.add_axes([0,0,1.2,1.2])
ax.bar(months,counts)

plt.xlabel('Months')
plt.ylabel('Number of Completed studies')
plt.title('Completed studies each month in a given year')

plt.show()

# COMMAND ----------

# QUESTION 6: Plot number of completed studies each month in a given year
display(numberOfCompletedStudiesDF)

# COMMAND ----------


