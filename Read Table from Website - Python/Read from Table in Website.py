# Databricks notebook source
# DBTITLE 1,Install Modules
# MAGIC %sh
# MAGIC pip install beautifulsoup4

# COMMAND ----------

# DBTITLE 1,Import Libraries
#Import Libraries and Functions
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType,IntegerType
from pyspark.sql import Row
import requests
import pandas as pd 
from bs4 import BeautifulSoup

# COMMAND ----------

# DBTITLE 1,URL Request
#GET Request from website
URL = "https://price.www.fairtrade.net/frame.php"
htmlresult = requests.get(URL)

# COMMAND ----------

# DBTITLE 1,Data Parsing and Transformation
#Parse HTML result and convert to Dataframe
soup = BeautifulSoup(htmlresult.text, 'html.parser')

tabularData = soup.find('table', id="dt-container")

listColumns = [rowiterate.text for rowiterate in tabularData.find("thead").find_all("th")][1:]

listRows = [[rowiterate.text for rowiterate in coliterate.find_all("td")][1:] for coliterate in tabularData.find("tbody").find_all("tr")]
gridDF = pd.DataFrame(listRows, columns=listColumns)

# COMMAND ----------

# DBTITLE 1,Display Output
#Print Dataframe
display(gridDF)

# COMMAND ----------

# DBTITLE 1,Persist Output
#Generate csv file in Data Lake
df.coalesce(1).write.option("header", "true").mode("overwrite").csv("dbfs:/FileStore/df/ExtractedTable.csv")
