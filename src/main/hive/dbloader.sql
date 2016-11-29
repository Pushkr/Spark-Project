
-- create database
CREATE DATABASE IF NOT EXISTS CRAIGSLIST
LOCATION '/user/cloudera/CRAIGSLIST' ;

set LOCATION=${hiveconf:LCTN};
set LOCATION;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions = 2000;
set hive.exec.max.dynamic.partitions.pernode = 2000;


--drop table CRAIGSLIST.CANON2470;

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:TABLE}
(TITLE STRING,
URL STRING,
PRICE STRING,
LOCATION STRING,
POSTED_ON STRING,
POSTED_AT STRING,
UPDATED_ON STRING,
UPDATED_AT STRING,
DESCRIPTION STRING)
PARTITIONED BY (FROM_MONTH STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
LOCATION '${hiveconf:LCTN}';

CREATE TEMPORARY EXTERNAL TABLE TEST_DATA(r1 string,r2 string,r3 string,r4 string,
r5 string,r6 string,r7 string,r8 string,r9 string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LOCATION '/user/cloudera/craigslist/rawdata';


-- Load data
from TEST_DATA
insert into table ${hiveconf:TABLE} partition(from_month)
select r1 as TITLE,
r2 AS URL,
R3 AS PRICE,
r4 AS LOCATION,
r5 AS POSTED_ON,
r6 AS POSTED_AT,
r7 AS UPDATED_ON,
r8 AS UPDATED_AT,
r9 AS DESCRIPTION,
SUBSTR(r5,1,7) AS FROM_MONTH;

--perform tests

describe formatted ${hiveconf:TABLE} ;

select * from ${hiveconf:TABLE} LIMIT 2;


