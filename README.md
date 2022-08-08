# Clinical Trial Data Analysis

## Introduction
Clinical trials are research studies performed on people, aimed at evaluating a medical, surgical, or behavioral intervention. They are the primary way that researchers find out if a new treatment, like a new drug or diet or medical device (for example, a pacemaker) is safe and effective in people.


## Table of Contents
1. <a href="#technologies-and-platforms-used">Technologies and Platforms Used</a>
2. <a href="#datasets-and-their-sources">Datasets and Their Sources</a>
3. <a href="#implementation-steps">Implementation Steps</a>
3. <a href="#analysis-performed">Analysis Performed</a>
4. <a href="#how-to-run-this-project">How To Run This Project</a>
4. <a href="#recommended-improvements">Recommended Improvements</a>
5. <a href="#author">Author</a>
6. <a href="#license">License</a>


## Technologies and Platforms Used

- [Python](https://www.python.org/)
- [Spark](https://spark.apache.org/documentation.html)
- [HiveQL](https://cwiki.apache.org/confluence/display/Hive/LanguageManual)
- [Matplotlib](https://matplotlib.org/2.0.2/users/intro.html)
- [Bokeh](https://docs.bokeh.org/en/latest/)
- [Linux terminal](https://ubuntu.com/tutorials/command-line-for-beginners)
- [Databricks](https://community.cloud.databricks.com/)


## Datasets and Their Sources

- Clinicaltrial_year
<br><br>
Each row represents an individual clinical trial, identified by an Id, listing the sponsor (Sponsor), the status of the study at time of the file’s download (Status), the start and completion dates (Start and Completion respectively), the type of study (Type), when the trial was first submitted (Submission), and the lists of conditions the trial concerns (Conditions) and the interventions explored (Interventions). Individual conditions and interventions are separated by commas. (Source: [ClinicalTrials.gov](https://clinicaltrials.gov/))

- Mesh
<br><br>
The conditions from the clinical trial list may also appear in a number of hierarchies. The hierarchy identifiers have the format [A-Z][0-9]+(.[0-9]+)* (such as, e.g., D03.633.100.221.173) where the initial letter and number combination designates the root of this particular hierarchy (in the example, this is D03) and each “.” descends a level down the hierarchy. The rows of this file contain condition (term), hierarchy identifier (tree) pairs. (Source: [U.S. National Library of Medicine](https://www.nlm.nih.gov/))

- Pharma
<br><br>
The file contains a small number of a publicly available list of pharmaceutical violations. For the puposes of this work, we are interested in the second column, Parent Company, which con- tains the name of the pharmaceutical company in question. (Source: [Pharma data](https://violationtracker.goodjobsfirst.org/industry/pharmaceuticals))


## Implementation Steps

* Loading the data to be analysed
* Cleaning the data
* Analysis
* Visualisation / Report generation


## Analysis Performed
#### <b>Problem statements</b>
You are a data analyst / data scientist whose client wishes to gain further insight into clinical trials. You are tasked with answering these questions, using visualisations where these would support your conclusions.

- The number of studies in the dataset (distinct studies).
- List all the types (as contained in the Type column) of studies in the dataset along with the frequencies of each type. These was ordered from most frequent to least frequent.
- List the top 5 Conditions with their frequencies.
- Each Condition can be mapped to one or more hierarchy codes. The client wishes to know the 5 most frequent roots (i.e. the sequence of letters and numbers before the first full stop) after this is done.
  ```bash
  To clarify, suppose the clinical trial data was:
      NCT01, ... ,"Disease_A,Disease_B",
      NCT02, ... ,Disease_B,
  And the mesh file contained:
      Disease_A A01.01 C23.02
      Disease_B B01.34.56
  The result would be
      B01 2
      A01 1
      C23 1
  ```
- Find the 10 most common Sponsors that are not pharmaceutical companies, along with the number of clinical trials they have sponsored. For a basic implementation, we can assume that the Parent Company column contains all possible pharmaceutical companies.
- Plot number of completed studies each month in a given year. 


## How To Run This Project

For this project, the platform used is Databricks. 
<br><br>
All the processing was done via executable Notebooks, Scripts and Code. Terminal commands were stored in shell scripts, language specific code was stored in separate files (for example, HiveQL code were stored in .sql scripts).
The solution was implemented using HiveQL and PySpark's RDD(Resilient Distributed Dataset) and DataFrames. 
<br><br>
Note that the PySpark implementation did not use SQL queries directly.

```bash
# Clone or Download this repository
$ git clone https://github.com/teepha/clinicaltrial_data_analysis.git

# Download the datasets as .csv files
$ https://clinicaltrials.gov/
$ https://www.nlm.nih.gov/
$ https://violationtracker.goodjobsfirst.org/industry/pharmaceuticals

# Sign up on Databricks
$ https://community.cloud.databricks.com/

# Upload the datasets(csv files) on the Databricks platform. The uploaded datasets, if used, must exist (and be named) in the following locations: 
• /FileStore/tables/clinicaltrial 2021.csv (similarly for 2019 and 2020 datasets)
• /FileStore/tables/mesh.csv
• /FileStore/tables/pharma.csv

#On Databricks, import the `.ipynb` files from the cloned repository as Notebooks. Open each of the Notebook and click on <Run All>. This will run all the cells in the Notebook and give the result for the analysis.
$ Run All
```

## Recommended Improvements

*  Implementation using an AWS cluster. In this case, scripts and screenshots need to be supplied to ensure reproducibility.
* Further analysis of the data, motivated by the questions asked.
* Creation of additional visualizations presenting useful information based exploration which is not covered by the problem statements.

## Author

Lateefat Amuda

## License

--

---
