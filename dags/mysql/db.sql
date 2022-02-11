CREATE DATABASE IF NOT EXISTS airflow_db_test;

USE airflow_db_test;

CREATE TABLE `airflow_db_test`.`forex_rates`(
    base VARCHAR(500),
    last_update DATE,
    eur DOUBLE,
    usd DOUBLE,
    nzd DOUBLE,
    gbp DOUBLE,
    jpy DOUBLE,
    cad DOUBLE
);