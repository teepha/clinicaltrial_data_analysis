-- Databricks notebook source
-- MAGIC %md
-- MAGIC <b>LOADING THE DATA<b>

-- COMMAND ----------

-- CHANGE CLINICALTRIAL YEAR
SET hivevar:year='2021';

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # CHANGE CLINICAL TRIAL FILE NAME HERE!!
-- MAGIC import os
-- MAGIC import sys
-- MAGIC 
-- MAGIC fileroot = "clinicaltrial_2021_csv"
-- MAGIC renamed_fileroot = "clinicaltrial_2021.csv"
-- MAGIC mesh_csv = "/FileStore/tables/mesh.csv"
-- MAGIC pharma_csv = "/FileStore/tables/pharma.csv"
-- MAGIC 
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
-- MAGIC dbutils.fs.ls("/FileStore/tables/")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <b>CREATE TABLES AND LOAD DATA INTO EACH ONE<b>

-- COMMAND ----------

-- CREATE CLINICALTRIAL TABLE
-- CHANGE TABLE NAME TO INCLLUDE YEAR OF CLINICALTRIAL
CREATE TABLE IF NOT EXISTS clinicaltrial_20_21_table(
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
LOCATION '/FileStore/clinicaltrial';

-- COMMAND ----------

-- CHANGE CLINICALTRIAL FILE NAME AND TABLE NAME HERE!!
LOAD DATA INPATH '/FileStore/tables/clinicaltrial_2021.csv' INTO TABLE clinicaltrial_20_21_table;

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
LOAD DATA INPATH '/FileStore/tables/mesh.csv' INTO TABLE mesh_table;

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
LOAD DATA INPATH '/FileStore/tables/pharma.csv' INTO TABLE pharma_table;

-- COMMAND ----------

-- FORMAT PHARMA TABLE TO REMOVE DOUBLE QUOTATIONS AND FIELDS THAT ARE NOT NEEDED
CREATE TABLE IF NOT EXISTS pharma_formatted_table
AS SELECT REGEXP_REPLACE(Company, '["]','') AS Company, REGEXP_REPLACE(Parent_Company, '["]','') AS Parent_Company
FROM pharma_table;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <b>PREPARE THE DATA<b>

-- COMMAND ----------

-- CREATE A TEMPORARY VIEW FROM CLINICALTRIAL TABLE
-- CHANGE CLINICALTRIAL TABLE NAME HERE!!
CREATE TEMPORARY VIEW clinicaltrial_20_21_view
AS SELECT * FROM clinicaltrial_20_21_table WHERE Id!='Id';

-- COMMAND ----------

DROP VIEW clinicaltrial_view

-- COMMAND ----------

SELECT * FROM clinicaltrial_20_21_view

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

-- QUESTION1: The number of studies in the dataset
SELECT DISTINCT COUNT(*) AS Count FROM clinicaltrial_20_21_view;

-- COMMAND ----------

-- QUESTION2: List all the Type of studies in the dataset along with the frequencies of each type
SELECT DISTINCT Type,COUNT(*) AS Count FROM clinicaltrial_20_21_view GROUP BY Type ORDER BY Count DESC;

-- COMMAND ----------

-- QUESTION3: The top 5 Conditions with their frequencies
SELECT Conditions,COUNT(Conditions) AS Counts
FROM (SELECT explode(split(Conditions, ',')) AS Conditions FROM clinicaltrial_20_21_view WHERE Conditions!='')
GROUP BY Conditions ORDER BY Counts DESC LIMIT 5;

-- COMMAND ----------

-- Create a temporary View from exploded clinical trial data
CREATE TEMPORARY VIEW explodedclinical_view
AS SELECT *,explode(split(Conditions, ',')) AS ExplodedConditions FROM clinicaltrial_20_21_view WHERE Conditions!='';

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
SELECT * FROM clinicaltrial_20_21_view c
LEFT OUTER JOIN pharma_view p
ON c.Sponsor=p.Parent_Company
WHERE p.Parent_Company IS NULL AND c.Status <> "Active"
)
GROUP BY Sponsor ORDER BY counts DESC LIMIT 10;

-- COMMAND ----------

-- QUESTION 6: Number of completed studies each month in a given year
SELECT SUBSTRING(Completion,1,3) AS Completion,
       COUNT(Completion) AS counts
FROM clinicaltrial_20_21_view
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
FROM clinicaltrial_20_21_view
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

DROP VIEW explodedclinical_view;

-- COMMAND ----------

DROP VIEW completedstudies_view;

-- COMMAND ----------

DROP VIEW clinicaltrial_20_21_view;


-- COMMAND ----------

DROP TABLE IF EXISTS clinicaltrial_20_21_table;

-- COMMAND ----------

DROP VIEW mesh_view;


-- COMMAND ----------

DROP TABLE IF EXISTS mesh_table;

-- COMMAND ----------

DROP VIEW pharma_view;

-- COMMAND ----------

DROP TABLE IF EXISTS pharma_formatted_table;

-- COMMAND ----------

DROP TABLE IF EXISTS pharma_table;

-- COMMAND ----------


