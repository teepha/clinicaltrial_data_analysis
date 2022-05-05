-- Databricks notebook source
-- MAGIC %md
-- MAGIC <b>DATA CLEANING AND PREPARATION<b>

-- COMMAND ----------

-- MAGIC %run ./Scripts

-- COMMAND ----------

-- CHANGE CLINICALTRIAL YEAR  
-- PLEASE ENSURE THAT THE YEAR MATCHES THE YEAR SPECIFIED IN THE SCRIPTS NOTEBOOK
SET hivevar:year=2021;

-- COMMAND ----------

-- VARIABLE FOR CLINICAL TRIAL FILE
SET hivevar:clinicaltrialdata='/FileStore/tables/clinicaltrial_${hivevar:year}.csv';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC REMOVING THE EXISTING TABLES AND VIEWS

-- COMMAND ----------

-- MAGIC %python
-- MAGIC clinicaltrial_tables = "/FileStore/clinicaltrials/"
-- MAGIC 
-- MAGIC try:
-- MAGIC     if len(dbutils.fs.ls(clinicaltrial_tables)) > 0:
-- MAGIC         dbutils.fs.rm(clinicaltrial_tables, True)
-- MAGIC     dbutils.fs.rm('dbfs:/user/hive/warehouse/pharma_formatted_table/', True)
-- MAGIC     dbutils.fs.rm('dbfs:/user/hive/warehouse/mesh/', True)
-- MAGIC except:
-- MAGIC     print("There's nothing to see here!")

-- COMMAND ----------

DROP TABLE IF EXISTS clinicaltrial_${hivevar:year}_table;
DROP VIEW IF EXISTS clinicaltrial_${hivevar:year}_view;
DROP VIEW IF EXISTS explodedclinical_view;
DROP VIEW IF EXISTS completedstudies_view;

-- COMMAND ----------

DROP TABLE IF EXISTS mesh_table;
DROP VIEW IF EXISTS mesh_view;

-- COMMAND ----------

DROP TABLE IF EXISTS pharma_table;
DROP TABLE IF EXISTS pharma_formatted_table;
DROP VIEW IF EXISTS pharma_view;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC CREATING TABLES AND LOADING DATA INTO EACH ONE

-- COMMAND ----------

-- CREATE CLINICALTRIAL TABLE
CREATE TABLE IF NOT EXISTS clinicaltrial_${hivevar:year}_table(
  Id STRING,
  Sponsor STRING,
  Status STRING,
  Start STRING,
  Completion STRING,
  Type STRING,
  Submission STRING,
  Conditions STRING,
  Interventions STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LOCATION '/FileStore/clinicaltrials';

-- COMMAND ----------

-- LOAD DATA INTO CLINICALTRIAL TABLE
LOAD DATA INPATH ${hivevar:clinicaltrialdata} OVERWRITE INTO TABLE clinicaltrial_${hivevar:year}_table;

-- COMMAND ----------

-- CREATE MESH TABLE
CREATE TABLE IF NOT EXISTS mesh_table(
  term STRING,
  tree STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/FileStore/mesh';

-- COMMAND ----------

-- LOAD DATA INTO MESH TABLE
LOAD DATA INPATH '/FileStore/mesh.csv' OVERWRITE INTO TABLE mesh_table;

-- COMMAND ----------

-- CREATE PHARMA TABLE
CREATE TABLE IF NOT EXISTS pharma_table(
  Company STRING, Parent_Company STRING, Penalty_Amount STRING,
  Subtraction_From_Penalty STRING, Penalty_Amount_Adjusted_For_Eliminating_Multiple_Counting STRING,
  Penalty_Year STRING, Penalty_Date STRING, Offense_Group STRING, Primary_Offense STRING,
  Secondary_Offense STRING, Description STRING, Level_of_Government STRING, Action_Type STRING,
  Agency STRING, `Civil/Criminal` STRING, Prosecution_Agreement STRING, Court STRING,
  Case_ID STRING, Private_Litigation_Case_Title STRING, Lawsuit_Resolution STRING,
  Facility_State STRING, City STRING, Address STRING, Zip STRING, NAICS_Code STRING,
  NAICS_Translation STRING, HQ_Country_of_Parent STRING, HQ_State_of_Parent STRING,
  Ownership_Structure STRING, Parent_Company_Stock_Ticker STRING, Major_Industry_of_Parent STRING, 
  Specific_Industry_of_Parent STRING, Info_Source STRING, Notes STRING
  )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/FileStore/pharma';

-- COMMAND ----------

-- LOAD DATA INTO PHARMA TABLE
LOAD DATA INPATH '/FileStore/pharma.csv' OVERWRITE INTO TABLE pharma_table;

-- COMMAND ----------

-- FORMAT PHARMA TABLE TO REMOVE DOUBLE QUOTATIONS AND FIELDS THAT ARE NOT NEEDED
CREATE TABLE IF NOT EXISTS pharma_formatted_table
AS SELECT REGEXP_REPLACE(Company, '["]','') AS Company, REGEXP_REPLACE(Parent_Company, '["]','') AS Parent_Company
FROM pharma_table;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC CREATING VIEWS FROM TABLES

-- COMMAND ----------

-- CREATE A TEMPORARY VIEW FROM CLINICALTRIAL TABLE
CREATE TEMPORARY VIEW clinicaltrial_${hivevar:year}_view
AS SELECT * FROM clinicaltrial_${hivevar:year}_table WHERE Id!='Id';

-- COMMAND ----------

-- CREATE A TEMPORARY VIEW FROM MESH TABLE
CREATE TEMPORARY VIEW mesh_view
AS SELECT * FROM mesh_table WHERE term!='term';

-- COMMAND ----------

-- CREATE A TEMPORARY VIEW FROM PHARMA FORMATTED TABLE
CREATE TEMPORARY VIEW pharma_view
AS SELECT * FROM pharma_formatted_table WHERE Company!='Company';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <b>ANALYSING THE DATA<b>

-- COMMAND ----------

-- Total number of studies in the clinical trial dataset
SELECT COUNT(*) AS Count FROM clinicaltrial_${hivevar:year}_view;

-- COMMAND ----------

-- QUESTION1: The number of distinct studies in the clinical trial dataset
SELECT DISTINCT COUNT(*) AS Count FROM clinicaltrial_${hivevar:year}_view;

-- COMMAND ----------

-- QUESTION2: All the Type of studies in the dataset along with the frequencies of each one
SELECT Type,COUNT(*) AS Count FROM clinicaltrial_${hivevar:year}_view GROUP BY Type ORDER BY Count DESC;

-- COMMAND ----------

-- QUESTION3: The top 5 Conditions with their frequencies
SELECT Conditions,COUNT(Conditions) AS Counts
FROM (
  SELECT explode(split(Conditions, ',')) 
  AS Conditions 
  FROM clinicaltrial_${hivevar:year}_view 
  WHERE Conditions!=''
  )
GROUP BY Conditions ORDER BY Counts DESC LIMIT 5;

-- COMMAND ----------

-- CREATE A TEMPORARY VIEW FROM THE EXPLODED CLINICALTRIAL DATA
CREATE TEMPORARY VIEW explodedclinical_view
AS 
SELECT *,explode(split(Conditions, ',')) AS ExplodedConditions 
FROM clinicaltrial_${hivevar:year}_view WHERE Conditions!='';

-- COMMAND ----------

-- QUESTION 4: The 5 most frequent roots from the hierarchy codes
SELECT tree,COUNT(tree) counts FROM
(
SELECT LEFT(m.tree,3) AS tree FROM mesh_view AS m
JOIN explodedclinical_view AS e
ON (m.term=e.ExplodedConditions)
)
GROUP BY tree ORDER BY counts DESC LIMIT 5;

-- COMMAND ----------

-- QUESTION 5: The 10 most common sponsors that are not pharmaceutical companies with the number of clinical trials they have sponsored
SELECT Sponsor,COUNT(Sponsor) AS counts FROM
(
SELECT * FROM clinicaltrial_${hivevar:year}_view c
LEFT OUTER JOIN pharma_view p
ON c.Sponsor=p.Parent_Company
WHERE p.Parent_Company IS NULL AND c.Status <> "Active"
)
GROUP BY Sponsor ORDER BY counts DESC LIMIT 10;

-- COMMAND ----------

-- QUESTION 6: Number of completed studies each month in a given year
-- LEFT(m.tree,3)
SELECT LEFT(Completion,3) AS Completion,
       COUNT(Completion) AS counts
FROM clinicaltrial_${hivevar:year}_view
WHERE Status=="Completed" AND Completion LIKE '%${hivevar:year}'
GROUP BY Completion
ORDER BY (unix_timestamp(Completion,'MMM'),'MM');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <b>VISUALISING THE RESULT<b>

-- COMMAND ----------

-- Create a temporary view from exploded clinical trial data
CREATE TEMP VIEW completedstudies_view
AS

SELECT SUBSTRING(Completion,1,3) AS Completion,
       COUNT(Completion) AS counts
FROM clinicaltrial_${hivevar:year}_view
WHERE Status=="Completed" AND Completion LIKE concat("%",${hivevar:year})
GROUP BY Completion
ORDER BY (unix_timestamp(Completion,'MMM'),'MM');

-- COMMAND ----------

-- MAGIC %python
-- MAGIC %pip install bokeh

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # QUESTION 6: Plot number of completed studies each month in a given year
-- MAGIC 
-- MAGIC from bokeh.io import output_file, show
-- MAGIC from bokeh.plotting import figure
-- MAGIC from bokeh.embed import file_html
-- MAGIC from bokeh.resources import CDN
-- MAGIC 
-- MAGIC months = spark.sql("SELECT * FROM completedstudies_view").select("Completion").rdd.map(lambda row: row["Completion"]).collect()
-- MAGIC counts = spark.sql("SELECT * FROM completedstudies_view").select("counts").rdd.map(lambda row: row["counts"]).collect()
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


