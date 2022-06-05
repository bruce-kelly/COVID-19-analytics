--------------------------------------------------------------------------
-- COVID 19 DATA ANALYSIS ------------------------------------------------
-- BRUCE KELLY -----------------------------------------------------------
--------------------------------------------------------------------------

-- Create Country General Info Table --------------------------------------
CREATE TABLE country_general_info (
    date DATE,
    location VARCHAR(100),
    iso_code VARCHAR(25),
    continent VARCHAR(25),
    population BIGINT,
    population_density FLOAT,
    median_age FLOAT,
    aged_65_older FLOAT,
    aged_70_older FLOAT,
    gdp_per_capita FLOAT,
    extreme_poverty FLOAT,
    human_development_index FLOAT,
    stringency_index FLOAT,
    cardiovasc_death_rate FLOAT,
    diabetes_prevalence FLOAT,
    female_smokers FLOAT,
    male_smokers FLOAT,
    handwashing_facilities FLOAT,
    life_expectancy FLOAT,
    reproduction_rate FLOAT
);

-- Populate Country General Info Table--------------
COPY country_general_info (
    date,
    location,
    iso_code,
    continent,
    population,
    population_density,
    median_age,
    aged_65_older,
    aged_70_older,
    gdp_per_capita,
    extreme_poverty,
    human_development_index,
    stringency_index,
    cardiovasc_death_rate,
    diabetes_prevalence,
    female_smokers,
    male_smokers,
    handwashing_facilities,
    life_expectancy,
    reproduction_rate
)
FROM 'C:\Users\Public\datasets\covid\set_2\country_general_info.csv'
DELIMITER ','
CSV HEADER;

-- Test
SELECT * FROM country_general_info;

------------------------------------------------------------------------------------------------------
-- Create Covid Cases, Deaths, Hospitalizations Table ------------------------------------------------
CREATE TABLE covid_cases_deaths_hosp (
    date DATE,
    location VARCHAR(100),
    total_cases BIGINT,
    new_cases BIGINT,
    new_cases_smoothed FLOAT,
    total_deaths BIGINT,
    new_deaths BIGINT,
    new_deaths_smoothed FLOAT,
    total_cases_per_million FLOAT,
    new_cases_per_million FLOAT,
    new_cases_smoothed_per_million FLOAT,
    total_deaths_per_million FLOAT,
    new_deaths_per_million FLOAT,
    new_deaths_smoothed_per_million FLOAT,
    icu_patients BIGINT,
    icu_patients_per_million FLOAT,
    hosp_patients BIGINT,
    hosp_patients_per_million FLOAT,
    weekly_icu_admissions FLOAT,
    weekly_icu_admissions_per_million FLOAT,
    weekly_hosp_admissions FLOAT,
    weekly_hosp_admissions_per_million FLOAT,
    hospital_beds_per_thousand FLOAT,
    excess_mortality_cumulative_absolute FLOAT,
    excess_mortality_cumulative FLOAT,
    excess_mortality FLOAT,
    excess_mortality_cumulative_per_million FLOAT
);

-- Populate Covid Cases, Deaths, Hospitalizations Table
COPY covid_cases_deaths_hosp (
    date,
    location,
    total_cases,
    new_cases,
    new_cases_smoothed,
    total_deaths,
    new_deaths,
    new_deaths_smoothed,
    total_cases_per_million,
    new_cases_per_million,
    new_cases_smoothed_per_million,
    total_deaths_per_million,
    new_deaths_per_million,
    new_deaths_smoothed_per_million,
    icu_patients,
    icu_patients_per_million,
    hosp_patients,
    hosp_patients_per_million,
    weekly_icu_admissions,
    weekly_icu_admissions_per_million,
    weekly_hosp_admissions,
    weekly_hosp_admissions_per_million,
    hospital_beds_per_thousand,
    excess_mortality_cumulative_absolute,
    excess_mortality_cumulative,
    excess_mortality,
    excess_mortality_cumulative_per_million
)
FROM 'C:\Users\Public\datasets\covid\set_2\covid_cases_deaths_hosp.csv'
DELIMITER ','
CSV HEADER;

-- Test
SELECT * FROM covid_cases_deaths_hosp;

-- Create Covid Tests & Vaccinations Table ------------------------------------------------------
CREATE TABLE covid_tests_vaccinations (
    date DATE,
    location VARCHAR(100),
    total_tests BIGINT,
    new_tests BIGINT,
    total_tests_per_thousand FLOAT,
    new_tests_per_thousand FLOAT,
    new_tests_smoothed BIGINT,
    new_tests_smoothed_per_thousand FLOAT,
    positive_rate FLOAT,
    tests_per_case FLOAT,
    tests_units VARCHAR(75),
    total_vaccinations BIGINT,
    people_vaccinated BIGINT,
    people_fully_vaccinated BIGINT,
    total_boosters BIGINT,
    new_vaccinations BIGINT,
    new_vaccinations_smoothed BIGINT,
    total_vaccinations_per_hundred FLOAT,
    people_vaccinated_per_hundred FLOAT,
    people_fully_vaccinated_per_hundred FLOAT,
    total_boosters_per_hundred FLOAT,
    new_vaccinations_smoothed_per_million BIGINT,
    new_people_vaccinated_smoothed BIGINT,
    new_people_vaccinated_smoothed_per_hundred FLOAT
);

-- Populate Covid Tests & Vaccinations Table
COPY covid_tests_vaccinations (
    date,
    location,
    total_tests,
    new_tests,
    total_tests_per_thousand,
    new_tests_per_thousand,
    new_tests_smoothed,
    new_tests_smoothed_per_thousand,
    positive_rate,
    tests_per_case,
    tests_units,
    total_vaccinations,
    people_vaccinated,
    people_fully_vaccinated,
    total_boosters,
    new_vaccinations,
    new_vaccinations_smoothed,
    total_vaccinations_per_hundred,
    people_vaccinated_per_hundred,
    people_fully_vaccinated_per_hundred,
    total_boosters_per_hundred,
    new_vaccinations_smoothed_per_million,
    new_people_vaccinated_smoothed,
    new_people_vaccinated_smoothed_per_hundred
)
FROM 'C:\Users\Public\datasets\covid\set_2\covid_tests_vaccinations.csv'
DELIMITER ','
CSV HEADER;

-- Test
SELECT * FROM covid_tests_vaccinations;
----------------------------------------------------------------------------------
-- SETUP MAIN VIEW FRAMEWORK------------------------------------------------------
----------------------------------------------------------------------------------
-- Full Join Display Template
SELECT *

FROM country_general_info AS c
JOIN covid_cases_deaths_hosp AS d
ON c.location = d.location
AND c.date = d.date
JOIN covid_tests_vaccinations AS v
ON c.location = v.location
AND c.date = v.date
WHERE c.continent IS NOT NULL;

--------------------------------------------------------------------------------------------------
-- MAIN QUERY SETUP
-- Goal here is to get as much relevant information as I need in one query, then convert it to a view
--------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- CREATE VIEW TO STORE DATA FOR VISUALIZATIONS
-------------------------------------------------------------------------------
CREATE VIEW deaths_cases_vaccines as
    WITH deaths_cases_vaccines
    (continent, location, date, population, new_cases, total_cases, new_deaths, total_deaths, new_vaccinations, people_fully_vaccinated, total_people_vaccinated,
    stringency_index, median_age, weekly_hosp_admissions_per_million, positive_rate, new_tests, total_tests, total_tests_per_thousand)
            AS (SELECT c.continent, c.location, c.date, c.population, d.new_cases, d.total_cases,
                d.new_deaths, d.total_deaths, v.new_vaccinations, v.people_fully_vaccinated,
                    SUM(v.new_vaccinations)
                    OVER (PARTITION BY c.location ORDER BY c.location, c.date)
                    AS total_people_vaccinated,
                c.stringency_index, c.median_age, d.weekly_hosp_admissions_per_million, v.positive_rate, new_tests,
                v.total_tests, v.total_tests_per_thousand

            FROM country_general_info AS c
            JOIN covid_tests_vaccinations AS v
            ON c.location = v.location
            AND c.date = v.date
            JOIN covid_cases_deaths_hosp AS d
            ON c.location = d.location
            AND c.date = d.date
            WHERE c.continent IS NOT NULL
            )
SELECT *,  (CAST(people_fully_vaccinated AS FLOAT) / population) * 100 AS percent_pop_vaccinated,
           (CAST(total_deaths AS FLOAT) / total_cases) * 100 AS death_percent_from_cases,
           (CAST(total_cases AS FLOAT) / population) * 100 AS percent_pop_infected
FROM deaths_cases_vaccines;

----------------------------------------------------------------------------------
-- QUERIES -----------------------------------------------------------------------
----------------------------------------------------------------------------------
-- All World Info from View
SELECT * FROM deaths_cases_vaccines;
--
-- Grouped by Location: World General Numbers ----------------------------------
WITH grouped_world (location, population, total_deaths, people_fully_vaccinated, total_tests, total_cases,
    percent_pop_infected, stringency_index, median_age, percent_pop_vaccinated)
    AS (SELECT location,
               population,
               MAX(total_deaths)            AS total_deaths,
               MAX(people_fully_vaccinated) AS people_fully_vaccinated,
               MAX(total_tests)             AS total_tests,
               MAX(total_cases)             AS total_cases,
               MAX(percent_pop_infected)    AS percent_pop_infected,
               MAX(stringency_index)        AS stringency_index,
               MAX(median_age)              as median_age,
               MAX(percent_pop_vaccinated)  as percent_pop_vaccinated
        FROM deaths_cases_vaccines
        GROUP BY location, population
)
SELECT *, (CAST(total_deaths AS FLOAT) / total_cases) * 100 AS death_percent_from_cases
FROM grouped_world;

-- US general numbers
SELECT date, location, population, new_cases, total_cases, percent_pop_infected, positive_rate, new_tests, total_tests,
       people_fully_vaccinated, new_deaths, total_deaths
FROM deaths_cases_vaccines
WHERE location = 'United States'
ORDER BY date ASC;

-- World general numbers
SELECT date, location, population, new_cases, total_cases, percent_pop_infected, positive_rate, new_tests, total_tests,
       people_fully_vaccinated, new_deaths, total_deaths
FROM deaths_cases_vaccines
ORDER BY date ASC;