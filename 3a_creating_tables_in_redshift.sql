-- Creating Table 'dimcountry'
CREATE TABLE public.dimcountry (
    iso_code character varying(256) NOT NULL ENCODE lzo,
    continent character varying(256) ENCODE lzo,
    location character varying(256) ENCODE lzo,
    population real ENCODE raw,
    PRIMARY KEY (iso_code)
) DISTSTYLE AUTO;

-- Creating Table 'dimdate'
CREATE TABLE public.dimdate (
    date_id integer NOT NULL ENCODE az64,
    year integer ENCODE az64,
    month integer ENCODE az64,
    day integer ENCODE az64,
    date character varying(256) ENCODE lzo,
    PRIMARY KEY (date_id)
) DISTSTYLE AUTO;

-- Creating Table 'dimvaccination'
CREATE TABLE "public"."dimvaccination"(
    "country_date_id" VARCHAR NULL,
    "people_vaccinated" REAL NULL,
    "people_fully_vaccinated" REAL NULL,
    "total_boosters" REAL NULL,
    PRIMARY KEY("country_date_id")
) ENCODE AUTO;

-- Creating Table 'dimpatients'
CREATE TABLE "public"."dimpatients"(
    "country_date_id" VARCHAR NULL,
    "hosp_patients" REAL NULL,
    "weekly_icu_admissions" REAL NULL,
    "weekly_hosp_admissions" REAL NULL,
    "diabetes_prevalence" REAL NULL,
    "cardiovasc_death_rate" REAL NULL,
    PRIMARY KEY("country_date_id")
) ENCODE AUTO;

-- Creating Table 'dimtesting'
CREATE TABLE "public"."dimtesting"(
    "country_date_id" VARCHAR NULL,
    "positive_rate" REAL NULL,
    "tests_per_case" REAL NULL,
    "tests_units" VARCHAR NULL,
    PRIMARY KEY("country_date_id")
) ENCODE AUTO;

-- Creating Table 'covidfact'
CREATE TABLE "public"."covidfact"(
    "fact_id" VARCHAR NULL,
    "country_date_id" VARCHAR NULL,
    "date_id" INTEGER NULL,
    "iso_code" VARCHAR NULL,
    "new_cases" REAL NULL,
    "total_cases" REAL NULL,
    "total_deaths" REAL NULL,
    "new_deaths" REAL NULL,
    "total_tests" REAL NULL,
    "new_tests" REAL NULL,
    "total_vaccinations" REAL NULL,
    "new_vaccinations" REAL NULL,
    "icu_patients" REAL NULL,
    PRIMARY KEY("fact_id")
) ENCODE AUTO;

-- Adding Foreign key constraints on fields 'country_date_id', 'date_id' and 'iso_code' of table 'covid_fact'
ALTER TABLE covidfact
ADD FOREIGN KEY (iso_code) 
REFERENCES dimcountry(iso_code);

ALTER TABLE covidfact
ADD FOREIGN KEY (date_id) 
REFERENCES dimdate(date_id);

ALTER TABLE covidfact
ADD FOREIGN KEY (country_date_id) 
REFERENCES dimpatients(country_date_id);

ALTER TABLE covidfact
ADD FOREIGN KEY (country_date_id)
REFERENCES dimtesting(country_date_id);

ALTER TABLE covidfact
ADD FOREIGN KEY (country_date_id) 
REFERENCES dimvaccination(country_date_id);