# Databricks notebook source
# MAGIC %md
# MAGIC <b>PREPARE THE DATA<b>

# COMMAND ----------

# MAGIC %md
# MAGIC RUN THE SCRIPT NOTEBOOK

# COMMAND ----------

# MAGIC %run ./Scripts

# COMMAND ----------

# FILE PATHS
clinicaltrial_csv = "/FileStore/tables/clinicaltrial_" + clinicaltrial_year + ".csv"
mesh_csv = "/FileStore/tables/mesh.csv"
pharma_csv = "/FileStore/tables/pharma.csv"

# COMMAND ----------

# FUNCTION FOR CREATING RDDS FROM FILES
def create_rdd_from_file(filename):
    rdd = sc.textFile(filename)
    header = rdd.first() #extracts the header
    return rdd.filter(lambda row: row != header)

# COMMAND ----------

# CREATING RDDS FROM CSV FILES
clinicaltrialRDD = create_rdd_from_file(clinicaltrial_csv)
clinicaltrialRDD.take(5)

# COMMAND ----------

meshRDD = create_rdd_from_file(mesh_csv)
meshRDD.take(5)

# COMMAND ----------

pharmaRDD = spark.read.option("header","true").option("inferSchema", "true").csv(pharma_csv).rdd
pharmaRDD.take(2)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>ANALYSING THE DATA<b>

# COMMAND ----------

# QUESTION 1: The distinct studies in the clinical trial dataset
clinicaltrialRDD.distinct().count()

# COMMAND ----------

# SPLIT CLINICAL TRIAL RDD BY DELIMITER
clinicaltrialRDD = clinicaltrialRDD.map(lambda line: line.split('|'))

# COMMAND ----------

# QUESTION 2: List all the Type of studies in the dataset along with the frequencies of each Type
pairedTypesRDD = clinicaltrialRDD.map(lambda line: (line[5], 1))
reducedTypesRDD = pairedTypesRDD.reduceByKey(lambda accum,curr: accum + curr)

typesFromRDD = reducedTypesRDD.sortBy(lambda a: -a[1])
typesFromRDD.collect()

# COMMAND ----------

# QUESTION 3: The top 5 Conditions with their frequencies
explodeConditionsFromRDD = clinicaltrialRDD.flatMap(lambda line: line[7].split(','))

groupConditionsFromRDD = explodeConditionsFromRDD.map(lambda line: (line, 1)).filter(lambda x: x[0])

topConditionsRDD = groupConditionsFromRDD.reduceByKey(lambda accum,curr: accum + curr).sortBy(lambda a: -a[1])
topConditionsRDD.take(5)

# COMMAND ----------

# QUESTION 4: The 5 most frequent roots from the hierarchy codes
explodedRDD = explodeConditionsFromRDD.map(lambda line: (line, 0))
splitMeshRDD = meshRDD.map(lambda x: (x.split(',')[0], x.split(',')[1]))

joinTreeRDD = splitMeshRDD.join(explodedRDD).map(lambda line: (line[1][0], line[0]))
pairedTreeRDD = joinTreeRDD.map(lambda x: (x[0].split('.')[0], 1))

rootsRDD = pairedTreeRDD.reduceByKey(lambda accum,curr: accum + curr).sortBy(lambda a: -a[1])
rootsRDD.take(5)

# COMMAND ----------

# QUESTION 5: The 10 most common sponsors that are not pharmaceutical companies with the number of clinical trials they have sponsored
selectClinicaltrialRDD = clinicaltrialRDD.map(lambda line: (line[1], line[2]))
splitPharmaRDD = pharmaRDD.flatMap(lambda x: x.Parent_Company.split(',')).map(lambda line: (line, 0))

joinedPharmaRDD = selectClinicaltrialRDD.leftOuterJoin(splitPharmaRDD)

nonActivePharmaRDD = joinedPharmaRDD.filter(lambda x: x[1][1]==None).filter(lambda x: x[1][0]!='Active').map(lambda x: (x[0], 1))

mostCommonNonPharmaRDD = nonActivePharmaRDD.reduceByKey(lambda accum,curr: accum + curr).sortBy(lambda a: -a[1])
mostCommonNonPharmaRDD.take(10)

# COMMAND ----------

# QUESTION 6: Number of completed studies each month in a given year
filteredCompletedStudiesRDD = clinicaltrialRDD.filter(lambda line: line[2]=="Completed").\
                                                filter(lambda x: clinicaltrial_year in x[4]).\
                                                map(lambda x: (x[4].split(' ')[0], 1))
unsortedCompletedStudiesRDD = filteredCompletedStudiesRDD.reduceByKey(lambda accum,curr: accum + curr)

months = {'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6, 'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12}

completedStudies = unsortedCompletedStudiesRDD.sortBy(lambda x: months.get(x[0]))
completedStudies.collect()

# COMMAND ----------


