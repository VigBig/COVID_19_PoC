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
- **Extract:** : The raw COVID-19 dataset is automatically extracted from the link https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv via AWS Lambda scheduled by Amazon EventBridge. The extracted data is then staged in an S3 bucket.
- **Transform:** Jupyter Notebook on AWS Glue is used to transform the data by following the steps below:
  - Create 5 dimension tables and a fact table.
  - Handle null/missing values and remove duplicate records from the dimension and fact tables.
  - Upload these tables as .csv files into an S3 bucket.
- **Load:** Glue Crawlers are used to discover and populate the AWS Glue Data Catalog with metadata tables. Then, data from the Glue database is loaded into an AWS Redshift cluster.
- **Visualization:** The transformed data is connected to Power BI for data visualization. Interactive visuals and reports are generated to provide insights into the COVID-19 data.

Steps
1. **Extraction**
- Write a Python script in AWS Lambda to extract COVID-19 data from 'https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv' in an S3 bucket. Use Amazon EventBridge to invoke the lambda function on a daily basis.

2. **Transformation**
- Using Pyspark in Jupyter Notebook on AWS Glue, transform the extracted data. The steps involved in the transformation are:

  2a. **Create Dimension and Fact Tables**
    - Create a Date dimension table with columns 'Date_id', 'Year', 'Month', 'Day', 'Date'.
    - Create a dimpatients dimension table with columns 'country_date_id',‘date’, ‘hosp_patients’, ‘weely_icu_admissions’, ‘weekly_hosp_admissions’, ‘diabetes_prevalence’, ‘cardiovasc_death_rate’.
    - Create a Testing dimension table with columns 'country_date_id', ‘positive_rate’, ‘tests_per_case’, ‘tests_units’.
    - Create a Country dimension table with columns 'iso_code', 'continent', 'country', and 'population'.
    - Create a covidfact fact table with columns 'iso_code','new_cases', 'total_cases', 'total_deaths', 'new_deaths', 'total_tests', 'new_tests', 'total_vaccinations','new_vaccinations', 'icu_patients'.
    
  2b. **Handle Null/Missing Values and Remove Duplicates**
    - Handle null/missing values in the dimension and fact tables by filling them with appropriate values.
    - Remove duplicate records from the dimension and fact tables.
    
  2c. **Upload Tables to S3**
    - Upload the dimension and fact tables as .csv files to an S3 bucket.

3. **Loading**
- Create tables in AWS Redshift for each of the dimension and fact tables.
- Set primary key and foreign key constraints for the respective tables.
- Execute the Glue crawler on the designated dimension and fact tables to persist them within the AWS Glue Catalog. Subsequently, proceed to load the tables from the Glue database into AWS Redshift.

4. **Visualization**
- After establishing the connection between COVID-19 data stored in AWS Redshift and Power BI, proceeded to design and build reports that incorporated interactive visuals to provide valuable insights , relevant and timely information to decision-makers, enabling them to make informed decisions based on the trends and patterns uncovered from the COVID-19 data.

## Conclusion
This data engineering project demonstrates how to build an end-to-end pipeline to extract, transform, load, and visualize COVID-19 data using various AWS services. The pipeline can be customized and extended to work with other datasets and use cases.
