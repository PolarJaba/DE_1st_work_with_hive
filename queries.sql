-- Создание временных таблиц

CREATE TABLE IF NOT EXISTS customers_tmp(
    row_index INT,
    customer_id STRING,
    first_name STRING,
    last_name STRING,
    company STRING,
    city STRING,
    country STRING,
    phone_1 STRING,
    phone_2 STRING,
    email STRING,
    sub_date DATE,
    website STRING,
    sub_year STRING,
    cust_group STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE TABLE IF NOT EXISTS org_tmp(
    index INT,
    organization_id STRING,
    name STRING,
    website STRING,
    country STRING,
    description STRING,
    founded INT,
    industry STRING,
    nomber_of_employees INT,
    cust_group STRING
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ';'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE TABLE IF NOT EXISTS people_tmp(
    index INT,
    user_id STRING,
    first_name STRING,
    last_name STRING,
    sex STRING,
    email STRING,
    phone STRING,
    date_of_birth DATE,
    job_title STRING,
    cust_group STRING
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ';'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Загрузка данных во временные таблицы

LOAD DATA INPATH '/user/polar_jabka/hive_practice/customers_groups.csv' INTO TABLE customers_tmp;
LOAD DATA INPATH '/user/polar_jabka/hive_practice/orgs_groups.csv' INTO TABLE org_tmp;
LOAD DATA INPATH '/user/polar_jabka/hive_practice/people_groups.csv' INTO TABLE people_tmp;

-- Создание вспомогательной таблицы и загрузка данных

CREATE TABLE IF NOT EXISTS ages(
    min_age INT,
    max_age INT,
    age_group STRING
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ';'
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH '/user/polar_jabka/hive_practice/age_groups.csv' INTO TABLE ages;

-- Создание постоянных таблиц

CREATE TABLE IF NOT EXISTS customers(
    row_index INT,
    customer_id STRING,
    first_name STRING,
    last_name STRING,
    company STRING,
    city STRING,
    country STRING,
    phone_1 STRING,
    phone_2 STRING,
    email STRING,
    sub_date DATE,
    website STRING,
    cust_group STRING
)
PARTITIONED BY (sub_year STRING)
CLUSTERED BY (cust_group) INTO 10 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS PARQUET;


CREATE TABLE IF NOT EXISTS people(
    index INT,
    user_id STRING,
    first_name STRING,
    last_name STRING,
    sex STRING,
    email STRING,
    phone STRING,
    date_of_birth DATE,
    job_title STRING,
    cust_group STRING
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ';'
CLUSTERED BY cust_group INTO 10 BUCKETS
STORED AS PARQUET;


CREATE TABLE IF NOT EXISTS orgs(
    index INT,
    organization_id STRING,
    name STRING,
    website STRING,
    country STRING,
    description STRING,
    founded INT,
    industry STRING,
    nomber_of_employees INT,
    cust_group STRING
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ';'
CLUSTERED BY (cust_group) INTO 10 BUCKETS
STORED AS PARQUET;


INSERT INTO TABLE customers PARTITION (sub_year = '2020')
SELECT row_index, customer_id, first_name,
    last_name, company, city, country, phone_1,
    phone_2, email, sub_date, website, cust_group
FROM customers_tmp WHERE sub_year = '2020';

INSERT INTO TABLE customers PARTITION (sub_year = '2021')
SELECT row_index, customer_id, first_name,
    last_name, company, city, country, phone_1,
    phone_2, email, sub_date, website, cust_group
FROM customers_tmp WHERE sub_year = '2021';

INSERT INTO TABLE customers PARTITION (sub_year = '2022')
SELECT row_index, customer_id, first_name,
    last_name, company, city, country, phone_1,
    phone_2, email, sub_date, website, cust_group
FROM customers_tmp WHERE sub_year = '2022';


DROP TABLE customers_tmp;
DROP TABLE people_tmp;
DROP TABLE org_tmp;

-- Создание datamart

WITH 
customers_union AS (
    SELECT customer_id, first_name, last_name, email, company, sub_year, sub_date
    FROM customers
    WHERE sub_year = '2020'
    UNION ALL 
    SELECT customer_id, company, sub_year, sub_date
    FROM customers
    WHERE sub_year = '2021'
    UNION ALL
    SELECT customer_id, company, sub_year, sub_date
    FROM customers
    WHERE sub_year = '2022'),
count_age AS (
    SELECT c.customer_id, c.company, c.sub_year AS sub_year, CAST(months_between(c.sub_date, p.date_of_birth) AS INTEGER)/12 AS c_years_old 
    FROM customers_union c
    LEFT JOIN people p ON c.first_name = p.first_name AND c.last_name = p.last_name AND c.email = p.email), 
def_age_group AS (
    SELECT ca.company, ca.sub_year, ag.age_group, COUNT(ag.age_group) AS amt
    FROM count_age ca, ages ag
    WHERE ca.c_years_old BETWEEN ag.min_age AND ag.max_age
    GROUP BY ca.company, ca.sub_year, ag.age_group),
def_main_group AS (    
    SELECT company, sub_year, age_group, (MAX(amt) OVER (PARTITION BY company, sub_year)) AS max_amt
    FROM def_age_group)
SELECT company AS Company, sub_year AS Year, age_group AS Age_group
FROM def_main_group
GROUP BY Company, Year;
