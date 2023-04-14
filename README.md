# spark_hive_transportation_sales
Using Hive and Spark and SQL to analyze the data regarding transportation  Using three datasets Sales_data.csv, Company_data.csv and company __Emp_data1.csv in Hadoop environment and SQL to create different reports.
Schema of columns for the external table:

CREATE TABLE IF NOT EXISTS sales 
(
ORDERNUMBER INT,
QUANTITYORDERED INT,
PRICEEACH INT,
ORDERLINENUMBER INT,
SALES INT,
REVENUE INT,
ORDERDATE INT,
STATUS STRING,
QTR_ID INT,
MONTH_ID INT,
YEAR_ID INT,
PRODUCTLINE STRING,
MSRP INT,
PRODUCTCODE STRING,
CUSTOMERNAME STRING,
PHONE INT,
ADDRESSLINE1 STRING,
ADDRESSLINE2 STRING,
CITY STRING,
STATE STRING,
POSTALCODE STRING,
COUNTRY STRING,
TERRITORY STRING,
CONTACTLASTNAME STRING,
CONTACTFIRSTNAME STRING,
DEALSIZE STRING,
CompanyID INT
)
COMMENT 'public database'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE ;

LOAD DATA INPATH '/user/maria_dev/drivers.csv' OVERWRITE INTO TABLE drivers
PROCESS DATA IN ZEPPELIN
Go to Zeppelin Notebook

Create a Company_Emp_data1 DataFrame from CSV file

val Company_Emp_data1 = (spark.read
.option("header", "true") // Use first line as header
.option("inferSchema", "true") // Infer schema
.csv("/tmp/Company_Emp_data1.csv"))
Create a company_data DataFrame from CSV file

val company_data = (spark.read
.option("header", "true") // Use first line as header
.option("inferSchema", "true") // Infer schema
.csv("/tmp/company_data.csv"))
Print Schema Company_Emp_data1

Company_Emp_data1.printSchema()
PrintSchema company_data

company_data.printSchema()
Create Tempview Company_Emp_data1

Company_Emp_data1.createOrReplaceTempView(“EmpView”)
Create Tempview Compview

Company_data.createOrReplaceTempView(“CompView”)
ANALYSIS
I. Produce a pie chart with the Cumulation of revenue by country from Sales for the following countries : USA, UK, France, Austria, Canada and Denmark.
%spark2.sql
SELECT sum(revenue) AS Cumulative_Revenue, country AS Country FROM sales
WHERE country=’USA’ GROUP BY Country
UNION
SELECT sum(revenue) AS Cumulative_Revenue, country AS Country FROM sales
WHERE country=’UK’ GROUP BY Country
UNION
SELECT sum(revenue) AS Cumulative_Revenue, country AS Country FROM sales
WHERE country=’France’ GROUP BY Country
UNION
SELECT sum(revenue) AS Cumulative_Revenue, country AS Country FROM sales
WHERE country=’Austria’ GROUP BY Country
UNION
SELECT sum(revenue) AS Cumulative_Revenue, country AS Country FROM sales
WHERE country='Canada' GROUP BY Country
UNION
SELECT sum(revenue) AS Cumulative_Revenue, country AS Country FROM sales
WHERE country='Denmark' GROUP BY Country
Or

%spark2.sql
SELECT sum(Revenue), country
FROM Salesview
WHERE country in ("USA","UK","France","Austria","Canada","Denmark")
GROUP BY country
