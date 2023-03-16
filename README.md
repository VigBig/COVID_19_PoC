# COVID-19 Data Engineering PoC Repository

This repository contains data engineering projects related to COVID-19, including data cleaning, transformation, and analysis.

## Data Sources

We use the following data sources for our analysis:

 **World Health Organization (WHO):** We obtained a COVID-19 dataset from the World Health Organization (WHO) as a primary data source. The dataset includes information on COVID-19 cases, deaths, and recoveries at the global, regional, and country levels. We downloaded the dataset from the WHO's official website in CSV format and used it for our analysis.
 
## Project Structure
The repository is structured as follows:

- **Data:** Contains the raw data and cleaned/transformed data for each data source.
- **Scripts/Notebooks:** Contains the Python scripts used for data extraction, cleaning and transformation.

## Data Flow
- **Extract:** COVID-19 data obtained from the WHO website is stored in an S3 bucket using a Python script.
- **Transform:** Jupyter Notebook on EMR is used to transform the data by following the steps below:
  - Create 5 dimension tables and a fact table.
  - Handle null/missing values and remove duplicate records from the dimension and fact tables.
  - Upload these tables as .csv files into an S3 bucket.
- **Load:** The transformed files are loaded from the S3 bucket into AWS Redshift. Primary key constraints are set for the respective tables.

Steps
1. **Extraction**
- Python script in Jupyter notebook '1_upload_data_to_s3.ipynb' is used to upload COVID-19 data in an S3 bucket.

2. **Transformation**
- Jupyter Notebook '2_covid19_transform_EMR.ipynb' on EMR is used to transform the extracted data. The steps involved in the transformation are:

  2a. **Create Dimension and Fact Tables**
    - Create a Date dimension table with columns 'Date_id', 'Year', 'Month', 'Day', 'Date'.
    - Create a dimpatients dimension table with columns 'country_date_id',‘date’, ‘hosp_patients’, ‘weely_icu_admissions’, ‘weekly_hosp_admissions’, ‘diabetes_prevalence’, ‘cardiovasc_death_rate’.
    - Create a Testing dimension table with columns 'country_date_id', ‘positive_rate’, ‘tests_per_case’, ‘tests_units’.
    - Create a Country dimension table with columns 'iso_code', 'continent', 'location', and 'population'.
    - Create a covidfact fact table with columns 'iso_code','new_cases', 'total_cases', 'total_deaths', 'new_deaths', 'total_tests', 'new_tests', 'total_vaccinations','new_vaccinations', 'icu_patients'.
    
  2b. **Handle Null/Missing Values and Remove Duplicates**
    - Handle null/missing values in the dimension and fact tables by filling them with appropriate values.
    - Remove duplicate records from the dimension and fact tables.
    
  2c. **Upload Tables to S3**
    - Upload the dimension and fact tables as .csv files to an S3 bucket.

## 3. Loading
Load the transformed data from the S3 bucket into AWS Redshift using the following steps:

- Create tables in AWS Redshift for each of the dimension and fact tables.
- Set primary key constraints for the respective tables.
- Load the data from the S3 bucket into the Redshift tables.
