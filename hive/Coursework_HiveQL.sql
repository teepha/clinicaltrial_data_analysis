-- Databricks notebook source
-- MAGIC %md
-- MAGIC <b>LOADING THE DATA<b>

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dataset_year = str(2019)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import os
-- MAGIC import sys
-- MAGIC 
-- MAGIC fileroot = "clinicaltrial_" + dataset_year + "_csv"
-- MAGIC renamed_fileroot = "clinicaltrial_" + dataset_year + ".csv"
-- MAGIC mesh_csv = "/FileStore/tables/mesh.csv"
-- MAGIC pharma_csv = "/FileStore/tables/pharma.csv"
-- MAGIC 
-- MAGIC # if 'dbruntime.dbutils' in sys.modules.keys():
-- MAGIC try:
-- MAGIC     dbutils.fs.ls("/FileStore/tables/" + renamed_fileroot)
-- MAGIC except:
-- MAGIC     dbutils.fs.cp("/FileStore/tables/" + fileroot + ".gz", "file:/tmp/")
-- MAGIC     os.environ['fileroot'] = fileroot

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC gunzip /tmp/ /tmp/$fileroot.gz

-- COMMAND ----------

-- MAGIC %python
-- MAGIC try:
-- MAGIC     dbutils.fs.ls("file:/tmp/" + fileroot)
-- MAGIC     dbutils.fs.mv("file:/tmp/" + fileroot, "/FileStore/tables/" + renamed_fileroot, True)
-- MAGIC except:
-- MAGIC     pass

-- COMMAND ----------

-- MAGIC %python
-- MAGIC clinical_csv = "/FileStore/tables/" + renamed_fileroot
-- MAGIC mesh_csv = "/FileStore/tables/mesh.csv"
-- MAGIC pharma_csv = "/FileStore/tables/pharma.csv"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import *
-- MAGIC clinicalDF = spark.read.option("header","true").option("inferSchema", "true").option("delimiter", '|').csv(clinical_csv)
-- MAGIC meshDF = spark.read.option("header","true").option("inferSchema", "true").csv(mesh_csv)
-- MAGIC pharmaDF = spark.read.option("header","true").option("inferSchema", "true").csv(pharma_csv)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC clinicalDF.createOrReplaceTempView("clinicaltrial_view")
-- MAGIC meshDF.createOrReplaceTempView("mesh_view")
-- MAGIC pharmaDF.createOrReplaceTempView("pharma_view")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC MAYBE START FROM HERE

-- COMMAND ----------

SET hivevar:year='2019';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <b>PREPARE THE DATA<b>

-- COMMAND ----------

SELECT * FROM clinicaltrial_view;

-- COMMAND ----------

-- QUESTION1: The number of studies in the dataset
SELECT DISTINCT COUNT(*) AS Count FROM clinicaltrial_view;

-- COMMAND ----------

-- QUESTION2: List all the Type of studies in the dataset along with the frequencies of each type
SELECT DISTINCT Type,COUNT(*) AS Count FROM clinicaltrial_view GROUP BY Type ORDER BY Count DESC;

-- COMMAND ----------

-- QUESTION3: The top 5 Conditions with their frequencies
SELECT Conditions,COUNT(Conditions) AS Counts
FROM (SELECT explode(split(Conditions, ',')) AS Conditions FROM clinicaltrial_view)
GROUP BY Conditions ORDER BY Counts DESC LIMIT 5;

-- COMMAND ----------

-- Create a temporary View from exploded clinical trial data
CREATE TEMPORARY VIEW explodedclinical_view
AS SELECT *,explode(split(Conditions, ',')) AS ExplodedConditions FROM clinicaltrial_view;

-- COMMAND ----------

-- QUESTION 4: The 5 most frequent roots from the hierarchy codes
SELECT tree,COUNT(tree) counts FROM
(
SELECT SUBSTRING(m.tree,1,3) AS tree FROM mesh_view AS m
JOIN explodedclinical_view AS e
ON (m.term=e.ExplodedConditions)
)
GROUP BY tree ORDER BY counts DESC LIMIT 5;

-- COMMAND ----------

-- QUESTION 5: The 10 most common sponsors that are not pharmaceutical companies with the number of clinical trials they have sponsored
SELECT Sponsor,COUNT(Sponsor) AS counts FROM
(
SELECT * FROM clinicaltrial_view c
LEFT OUTER JOIN pharma_view p
ON c.Sponsor=p.Parent_Company
WHERE p.Parent_Company IS NULL AND c.Status <> "Active"
)
GROUP BY Sponsor ORDER BY counts DESC LIMIT 10;

-- COMMAND ----------

-- QUESTION 6: Number of completed studies each month in a given year
-- SET hivevar:year='2019';

SELECT SUBSTRING(Completion,1,3) AS Completion,
       COUNT(Completion) AS counts
FROM clinicaltrial_view
WHERE Status=="Completed" AND Completion LIKE concat("%",${hivevar:year})
GROUP BY Completion
ORDER BY (unix_timestamp(Completion,'MMM'),'MM');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <b>VISUALISE RESULTS<b>

-- COMMAND ----------

-- Create a temporary View from exploded clinical trial data
CREATE TEMP VIEW completedstudies_view
AS

SELECT SUBSTRING(Completion,1,3) AS Completion,
       COUNT(Completion) AS counts
FROM clinicaltrial_view
WHERE Status=="Completed" AND Completion LIKE concat("%",${hivevar:year})
GROUP BY Completion
ORDER BY (unix_timestamp(Completion,'MMM'),'MM');

-- COMMAND ----------

-- MAGIC %python
-- MAGIC %pip install bokeh

-- COMMAND ----------

-- MAGIC %python
-- MAGIC months = spark.sql("SELECT * FROM completedstudies_view").select("Completion").rdd.map(lambda row: row["Completion"]).collect()
-- MAGIC counts = spark.sql("SELECT * FROM completedstudies_view").select("counts").rdd.map(lambda row: row["counts"]).collect()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from bokeh.io import output_file, show
-- MAGIC from bokeh.plotting import figure
-- MAGIC from bokeh.embed import file_html
-- MAGIC from bokeh.resources import CDN
-- MAGIC 
-- MAGIC # months = spark.sql("SELECT * FROM completedstudies_view").select("Completion").rdd.map(lambda row: row["Completion"]).collect()
-- MAGIC # counts = spark.sql("SELECT * FROM completedstudies_view").select("counts").rdd.map(lambda row: row["counts"]).collect()
-- MAGIC 
-- MAGIC p = figure(x_range=months, height=300, title="Completed studies each month in a given year")
-- MAGIC 
-- MAGIC p.vbar(x=months, top=counts, width=0.7)
-- MAGIC 
-- MAGIC p.xgrid.grid_line_color = None
-- MAGIC p.y_range.start = 0
-- MAGIC 
-- MAGIC html = file_html(p, CDN, "plot")
-- MAGIC displayHTML(html)

-- COMMAND ----------

DROP TABLE IF EXISTS explodedclinical_view;

-- COMMAND ----------

DROP TABLE IF EXISTS completedstudies_view;

-- COMMAND ----------


