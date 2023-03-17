import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1678970371277 = glueContext.create_dynamic_frame.from_catalog(
    database="covid19_transformation_db",
    table_name="dim_date",
    transformation_ctx="AWSGlueDataCatalog_node1678970371277",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1678970435126 = glueContext.create_dynamic_frame.from_catalog(
    database="covid19_transformation_db",
    table_name="dim_patients",
    transformation_ctx="AWSGlueDataCatalog_node1678970435126",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1678969983909 = glueContext.create_dynamic_frame.from_catalog(
    database="covid19_transformation_db",
    table_name="dim_country",
    transformation_ctx="AWSGlueDataCatalog_node1678969983909",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1678970630666 = glueContext.create_dynamic_frame.from_catalog(
    database="covid19_transformation_db",
    table_name="dim_vaccination",
    transformation_ctx="AWSGlueDataCatalog_node1678970630666",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1678970286195 = glueContext.create_dynamic_frame.from_catalog(
    database="covid19_transformation_db",
    table_name="covid_fact",
    transformation_ctx="AWSGlueDataCatalog_node1678970286195",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1678970512212 = glueContext.create_dynamic_frame.from_catalog(
    database="covid19_transformation_db",
    table_name="dim_testing",
    transformation_ctx="AWSGlueDataCatalog_node1678970512212",
)

# Script generated for node Change Schema
ChangeSchema_node1679028696274 = ApplyMapping.apply(
    frame=AWSGlueDataCatalog_node1678970371277,
    mappings=[
        ("date_id", "long", "date_id", "long"),
        ("year", "long", "year", "long"),
        ("month", "long", "month", "long"),
        ("day", "long", "day", "long"),
        ("date", "string", "date", "string"),
    ],
    transformation_ctx="ChangeSchema_node1679028696274",
)

# Script generated for node Change Schema
ChangeSchema_node1679028461781 = ApplyMapping.apply(
    frame=AWSGlueDataCatalog_node1678970435126,
    mappings=[
        ("country_date_id", "string", "country_date_id", "string"),
        ("hosp_patients", "double", "hosp_patients", "double"),
        ("weekly_icu_admissions", "double", "weekly_icu_admissions", "double"),
        ("weekly_hosp_admissions", "double", "weekly_hosp_admissions", "double"),
        ("diabetes_prevalence", "double", "diabetes_prevalence", "double"),
        ("cardiovasc_death_rate", "double", "cardiovasc_death_rate", "double"),
    ],
    transformation_ctx="ChangeSchema_node1679028461781",
)

# Script generated for node Change Schema
ChangeSchema_node1678969986809 = ApplyMapping.apply(
    frame=AWSGlueDataCatalog_node1678969983909,
    mappings=[
        ("col0", "string", "iso_code", "string"),
        ("col1", "string", "continent", "string"),
        ("col2", "string", "country", "string"),
        ("col3", "string", "population", "double"),
    ],
    transformation_ctx="ChangeSchema_node1678969986809",
)

# Script generated for node Change Schema
ChangeSchema_node1678970802794 = ApplyMapping.apply(
    frame=AWSGlueDataCatalog_node1678970630666,
    mappings=[
        ("col0", "string", "country_date_id", "string"),
        ("col1", "string", "people_vaccinated", "double"),
        ("col2", "string", "people_fully_vaccinated", "double"),
        ("col3", "string", "total_boosters", "double"),
    ],
    transformation_ctx="ChangeSchema_node1678970802794",
)

# Script generated for node Change Schema
ChangeSchema_node1679029176158 = ApplyMapping.apply(
    frame=AWSGlueDataCatalog_node1678970286195,
    mappings=[
        ("fact_id", "string", "fact_id", "string"),
        ("country_date_id", "string", "country_date_id", "string"),
        ("date_id", "long", "date_id", "long"),
        ("iso_code", "string", "iso_code", "string"),
        ("new_cases", "double", "new_cases", "double"),
        ("total_cases", "double", "total_cases", "double"),
        ("total_deaths", "double", "total_deaths", "double"),
        ("new_deaths", "double", "new_deaths", "double"),
        ("total_tests", "double", "total_tests", "double"),
        ("new_tests", "double", "new_tests", "double"),
        ("total_vaccinations", "string", "total_vaccinations", "string"),
        ("new_vaccinations", "double", "new_vaccinations", "double"),
        ("icu_patients", "double", "icu_patients", "double"),
    ],
    transformation_ctx="ChangeSchema_node1679029176158",
)

# Script generated for node Change Schema
ChangeSchema_node1679029321096 = ApplyMapping.apply(
    frame=AWSGlueDataCatalog_node1678970512212,
    mappings=[
        ("country_date_id", "string", "country_date_id", "string"),
        ("positive_rate", "string", "positive_rate", "double"),
        ("tests_per_case", "double", "tests_per_case", "double"),
        ("tests_units", "string", "tests_units", "string"),
    ],
    transformation_ctx="ChangeSchema_node1679029321096",
)

# Script generated for node Amazon Redshift
pre_query = "drop table if exists public.stage_table_db10d07172a44d729cd56dd97ea57e0f;create table public.stage_table_db10d07172a44d729cd56dd97ea57e0f as select * from public.dimdate where 1=2;"
post_query = "begin;delete from public.dimdate using public.stage_table_db10d07172a44d729cd56dd97ea57e0f where public.stage_table_db10d07172a44d729cd56dd97ea57e0f.date = public.dimdate.date and public.stage_table_db10d07172a44d729cd56dd97ea57e0f.month = public.dimdate.month and public.stage_table_db10d07172a44d729cd56dd97ea57e0f.year = public.dimdate.year and public.stage_table_db10d07172a44d729cd56dd97ea57e0f.date_id = public.dimdate.date_id and public.stage_table_db10d07172a44d729cd56dd97ea57e0f.day = public.dimdate.day; insert into public.dimdate select * from public.stage_table_db10d07172a44d729cd56dd97ea57e0f; drop table public.stage_table_db10d07172a44d729cd56dd97ea57e0f; end;"
AmazonRedshift_node1679028710506 = glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=ChangeSchema_node1679028696274,
    catalog_connection="redshift_connection",
    connection_options={
        "database": "covid19data",
        "dbtable": "public.stage_table_db10d07172a44d729cd56dd97ea57e0f",
        "preactions": pre_query,
        "postactions": post_query,
    },
    redshift_tmp_dir=args["TempDir"],
    transformation_ctx="AmazonRedshift_node1679028710506",
)

# Script generated for node Amazon Redshift
pre_query = "drop table if exists public.stage_table_244522da352c4023b4d63c0bb828d0ed;create table public.stage_table_244522da352c4023b4d63c0bb828d0ed as select * from public.dimpatients where 1=2;"
post_query = "begin;delete from public.dimpatients using public.stage_table_244522da352c4023b4d63c0bb828d0ed where public.stage_table_244522da352c4023b4d63c0bb828d0ed.country_date_id = public.dimpatients.country_date_id and public.stage_table_244522da352c4023b4d63c0bb828d0ed.hosp_patients = public.dimpatients.hosp_patients and public.stage_table_244522da352c4023b4d63c0bb828d0ed.diabetes_prevalence = public.dimpatients.diabetes_prevalence and public.stage_table_244522da352c4023b4d63c0bb828d0ed.weekly_icu_admissions = public.dimpatients.weekly_icu_admissions and public.stage_table_244522da352c4023b4d63c0bb828d0ed.weekly_hosp_admissions = public.dimpatients.weekly_hosp_admissions and public.stage_table_244522da352c4023b4d63c0bb828d0ed.cardiovasc_death_rate = public.dimpatients.cardiovasc_death_rate; insert into public.dimpatients select * from public.stage_table_244522da352c4023b4d63c0bb828d0ed; drop table public.stage_table_244522da352c4023b4d63c0bb828d0ed; end;"
AmazonRedshift_node1679028624689 = glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=ChangeSchema_node1679028461781,
    catalog_connection="redshift_connection",
    connection_options={
        "database": "covid19data",
        "dbtable": "public.stage_table_244522da352c4023b4d63c0bb828d0ed",
        "preactions": pre_query,
        "postactions": post_query,
    },
    redshift_tmp_dir=args["TempDir"],
    transformation_ctx="AmazonRedshift_node1679028624689",
)

# Script generated for node Amazon Redshift
pre_query = "drop table if exists public.stage_table_5c7ad2781d9f4f52ba444018d547c6d1;create table public.stage_table_5c7ad2781d9f4f52ba444018d547c6d1 as select * from public.dimcountry where 1=2;"
post_query = "begin;delete from public.dimcountry using public.stage_table_5c7ad2781d9f4f52ba444018d547c6d1 where public.stage_table_5c7ad2781d9f4f52ba444018d547c6d1.continent = public.dimcountry.continent and public.stage_table_5c7ad2781d9f4f52ba444018d547c6d1.location = public.dimcountry.location and public.stage_table_5c7ad2781d9f4f52ba444018d547c6d1.iso_code = public.dimcountry.iso_code and public.stage_table_5c7ad2781d9f4f52ba444018d547c6d1.population = public.dimcountry.population; insert into public.dimcountry select * from public.stage_table_5c7ad2781d9f4f52ba444018d547c6d1; drop table public.stage_table_5c7ad2781d9f4f52ba444018d547c6d1; end;"
AmazonRedshift_node1678969991593 = glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=ChangeSchema_node1678969986809,
    catalog_connection="redshift_connection",
    connection_options={
        "database": "covid19data",
        "dbtable": "public.stage_table_5c7ad2781d9f4f52ba444018d547c6d1",
        "preactions": pre_query,
        "postactions": post_query,
    },
    redshift_tmp_dir=args["TempDir"],
    transformation_ctx="AmazonRedshift_node1678969991593",
)

# Script generated for node Amazon Redshift
pre_query = "drop table if exists public.stage_table_8c2a7bdd590d4f11811ab5d3486e9a60;create table public.stage_table_8c2a7bdd590d4f11811ab5d3486e9a60 as select * from public.dimvaccination where 1=2;"
post_query = "begin;delete from public.dimvaccination using public.stage_table_8c2a7bdd590d4f11811ab5d3486e9a60 where public.stage_table_8c2a7bdd590d4f11811ab5d3486e9a60.people_vaccinated = public.dimvaccination.people_vaccinated and public.stage_table_8c2a7bdd590d4f11811ab5d3486e9a60.country_date_id = public.dimvaccination.country_date_id and public.stage_table_8c2a7bdd590d4f11811ab5d3486e9a60.people_fully_vaccinated = public.dimvaccination.people_fully_vaccinated and public.stage_table_8c2a7bdd590d4f11811ab5d3486e9a60.total_boosters = public.dimvaccination.total_boosters; insert into public.dimvaccination select * from public.stage_table_8c2a7bdd590d4f11811ab5d3486e9a60; drop table public.stage_table_8c2a7bdd590d4f11811ab5d3486e9a60; end;"
AmazonRedshift_node1678970941947 = glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=ChangeSchema_node1678970802794,
    catalog_connection="redshift_connection",
    connection_options={
        "database": "covid19data",
        "dbtable": "public.stage_table_8c2a7bdd590d4f11811ab5d3486e9a60",
        "preactions": pre_query,
        "postactions": post_query,
    },
    redshift_tmp_dir=args["TempDir"],
    transformation_ctx="AmazonRedshift_node1678970941947",
)

# Script generated for node Amazon Redshift
pre_query = "drop table if exists public.stage_table_52b3ce30d02d46ef80a2b194d9d1e41e;create table public.stage_table_52b3ce30d02d46ef80a2b194d9d1e41e as select * from public.covidfact where 1=2;"
post_query = "begin;delete from public.covidfact using public.stage_table_52b3ce30d02d46ef80a2b194d9d1e41e where public.stage_table_52b3ce30d02d46ef80a2b194d9d1e41e.country_date_id = public.covidfact.country_date_id and public.stage_table_52b3ce30d02d46ef80a2b194d9d1e41e.total_cases = public.covidfact.total_cases and public.stage_table_52b3ce30d02d46ef80a2b194d9d1e41e.fact_id = public.covidfact.fact_id and public.stage_table_52b3ce30d02d46ef80a2b194d9d1e41e.new_cases = public.covidfact.new_cases and public.stage_table_52b3ce30d02d46ef80a2b194d9d1e41e.date_id = public.covidfact.date_id and public.stage_table_52b3ce30d02d46ef80a2b194d9d1e41e.new_vaccinations = public.covidfact.new_vaccinations and public.stage_table_52b3ce30d02d46ef80a2b194d9d1e41e.new_tests = public.covidfact.new_tests and public.stage_table_52b3ce30d02d46ef80a2b194d9d1e41e.total_vaccinations = public.covidfact.total_vaccinations and public.stage_table_52b3ce30d02d46ef80a2b194d9d1e41e.total_deaths = public.covidfact.total_deaths and public.stage_table_52b3ce30d02d46ef80a2b194d9d1e41e.new_deaths = public.covidfact.new_deaths and public.stage_table_52b3ce30d02d46ef80a2b194d9d1e41e.total_tests = public.covidfact.total_tests and public.stage_table_52b3ce30d02d46ef80a2b194d9d1e41e.iso_code = public.covidfact.iso_code; insert into public.covidfact select * from public.stage_table_52b3ce30d02d46ef80a2b194d9d1e41e; drop table public.stage_table_52b3ce30d02d46ef80a2b194d9d1e41e; end;"
AmazonRedshift_node1679029261180 = glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=ChangeSchema_node1679029176158,
    catalog_connection="redshift_connection",
    connection_options={
        "database": "covid19data",
        "dbtable": "public.stage_table_52b3ce30d02d46ef80a2b194d9d1e41e",
        "preactions": pre_query,
        "postactions": post_query,
    },
    redshift_tmp_dir=args["TempDir"],
    transformation_ctx="AmazonRedshift_node1679029261180",
)

# Script generated for node Amazon Redshift
pre_query = "drop table if exists public.stage_table_c7f3e2c7b07645069a33f79df1be623b;create table public.stage_table_c7f3e2c7b07645069a33f79df1be623b as select * from public.dimtesting where 1=2;"
post_query = "begin;delete from public.dimtesting using public.stage_table_c7f3e2c7b07645069a33f79df1be623b where public.stage_table_c7f3e2c7b07645069a33f79df1be623b.country_date_id = public.dimtesting.country_date_id and public.stage_table_c7f3e2c7b07645069a33f79df1be623b.tests_per_case = public.dimtesting.tests_per_case and public.stage_table_c7f3e2c7b07645069a33f79df1be623b.positive_rate = public.dimtesting.positive_rate and public.stage_table_c7f3e2c7b07645069a33f79df1be623b.tests_units = public.dimtesting.tests_units; insert into public.dimtesting select * from public.stage_table_c7f3e2c7b07645069a33f79df1be623b; drop table public.stage_table_c7f3e2c7b07645069a33f79df1be623b; end;"
AmazonRedshift_node1679029380577 = glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=ChangeSchema_node1679029321096,
    catalog_connection="redshift_connection",
    connection_options={
        "database": "covid19data",
        "dbtable": "public.stage_table_c7f3e2c7b07645069a33f79df1be623b",
        "preactions": pre_query,
        "postactions": post_query,
    },
    redshift_tmp_dir=args["TempDir"],
    transformation_ctx="AmazonRedshift_node1679029380577",
)

job.commit()
