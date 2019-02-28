-- **Overall remember that credentials are cloudera/cloudera

-- Start ZooKeeper, HDFS, YARN, Hive and Hue services in that order (in Cloudera manager)

-- Download students data set from: https://s3.amazonaws.com/hadoop-dataset/student.csv into ~/Data/Students

-- Create folder ~/Data/Students/HiveOutput

-- Open Hue (make sure you are in Hive and not Impala -> Click on Query, Editor, Hive)

-- 1) Create Database, table and load data into it.

--Show HDFS directory inside metastore
CREATE DATABASE STUDENT_DB;

USE STUDENT_DB;

CREATE TABLE STUDENT
(SSN STRING, FIRST_NAME STRING, LAST_NAME STRING, RACE STRING, AGE INT, STATE STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
TBLPROPERTIES("skip.header.line.count"="1");

DESCRIBE STUDENT;

--Show file in HDFS now
LOAD DATA LOCAL INPATH '/home/cloudera/Data/Students/student.csv' INTO TABLE STUDENT;

SELECT * FROM STUDENT;

---Go to the command line and run hive command
---Show how it says its deprecated, however show that it works and DROP the table
---Reason is that hive command can only access metastore existent in that machine
---You can connect remotely with beeline

--Before droping, copy the csv file into HData/ in HDFS
--See how data disappears, since this was a managed table
DROP TABLE STUDENT;

---Connect with beeline
--beeline -u jdbc:hive2://localhost:10000/default -n cloudera

---Create the table as external
---Create a folder in HData -> HiveTables
CREATE EXTERNAL TABLE STUDENT
(SSN STRING, FIRST_NAME STRING, LAST_NAME STRING, RACE STRING, AGE INT, STATE STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/cloudera/HData/HiveTables/'
TBLPROPERTIES("skip.header.line.count"="1");

---Load data directly from HDFS
---It actually move the file!!
LOAD DATA INPATH '/user/cloudera/HData/student.csv' INTO TABLE STUDENT;

---However, if we drop the table, let's see what happens.
--->>>File is kept, that is the big difference between MANAGED and EXTERNAL
DROP TABLE STUDENT;

-- 2) List the first 50 students with last names sorted in descending order.

SELECT * FROM STUDENT
ORDER BY LAST_NAME DESC
LIMIT 50;

-- 3) Display Race and count per Race and store result on a local directory named racedata.csv.

-- Make sure your ~/HData/hive exists
--chown hive:hive hive/

INSERT OVERWRITE LOCAL DIRECTORY '/home/cloudera/HData/hive/output/' 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
SELECT RACE, COUNT(RACE) FROM STUDENT
GROUP BY RACE;

--** Rename file from output content and change ownerships

--chown -R cloudera:cloudera ~/HData/hive/output/
--cat ~/HData/hive/output/* > ~/HData/racedata.csv

--4) Display all students who reside in California or Virginia.

SELECT FIRST_NAME, LAST_NAME, STATE
FROM STUDENT
WHERE UPPER(STATE)='CALIFORNIA' OR STATE='VIRGINIA'
ORDER BY STATE;

--5) Display SSN of all students who are Cuban and their first name starts with ‘C’.

SELECT SSN FROM STUDENT
WHERE RACE='Cuban' AND FIRST_NAME like 'C%';

--6) Display all students younger than 25 and older than 18 years.

SELECT * FROM STUDENT
WHERE AGE<25 AND AGE>18;

--7) Display all Asian students older than 30 years of age.

SELECT * FROM STUTENT
WHERE (RACE='Laotian' OR RACE='Chinese' OR RACE='Vietnamese' OR RACE='Cambodian' OR RACE='Korean'
OR RACE='Filipino') AND AGE > 30;

--8) Display the average age of all students.

SELECT AVG(AGE) FROM STUDENT
GROUP BY AGE;

--9) Display the count of all students whose first name starts with ‘A or a’.

SELECT COUNT(FIRST_NAME) FROM STUDENT
WHERE (FIRST_NAME LIKE 'A%') OR (FIRST_NAME like 'a%');

--10) Display the SSN of all students who live in Pennsylvania and also show the last name.

SELECT SSN, LAST_NAME FROM STUDENT
WHERE STATE='Pennsylvania';

--11) Calculate the count of students based on the states (show name of state as well) and store result in another table on HDFS.

--We cannot create an EXTERNAL table with SELECT AS, data is by default going into HDFS
--(SemanticException: CREATE-TABLE-AS-SELECT cannot create external table)

CREATE TABLE STUDENTS_STATE
AS SELECT COUNT(*) AS N_STUDENTS, STATE FROM STUDENT
GROUP BY STATE;

SELECT * FROM STUDENTS_STATE;

--Check data in HDFS
--hdfs dfs -cat /user/hive/warehouse/student_db.db/students_state/*

--To create a table (can only be managed if directly created) in a separate directory, we need to create the directories first under /user/cloudera and then
--it is still not our own file format
--hdfs dfs -mkdir hive
--hdfs dfs -mkdir hive/tables

CREATE TABLE STUDENTS_STATE
LOCATION '/user/cloudera/hive/tables/'
AS SELECT COUNT(*) AS N_STUDENTS, STATE FROM STUDENT
GROUP BY STATE;

--The actual "tables" directory disappears (it's managed)
DROP TABLE STUDENTS_STATE;