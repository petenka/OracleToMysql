# OracleToMysql
Simple script to convert tables from Oracle to MySQL. Migrates one table structure (including PRIMARY KEYS, UNIQUE KEYS) and data

## Installation

You will need to install the following python packages:

```
mysql.connector (https://dev.mysql.com/downloads/connector/python/) 
cx_Oracle (https://sourceforge.net/projects/cx-oracle/)
```
Edit the config.txt and specify the hostnames, accounts, and passwords for your two databases.

## Running the script

```
python OracleToMysql.py
```

# TODO:
 - cycle for all tables
 - foreign keys
