# This notebook connects to an Azure Data Lake Storage to pick up the JSON file sources as obtained from the online source.
# The notebook uses Python to transform JSON based files into a Tabular format dataset ready to be related within a data model.
# It generates a Fact and several dimensions to allow a Star Schema data model.
# Databricks notebook source
# DBTITLE 1,Define Widgets
dbutils.widgets.text("Container Name","sources")
dbutils.widgets.text("Storage Account Name","tempstoragejb")
dbutils.widgets.text("SAS","?sv...")
dbutils.widgets.text("FileName","yelp_academic_dataset_review.json,yelp_academic_dataset_business.json,yelp_academic_dataset_checkin.json,yelp_academic_dataset_user.json"

# COMMAND ----------

# DBTITLE 1,Read values from Widgets
containerName = dbutils.widgets.get("Container Name")
storageAccountName = dbutils.widgets.get("Storage Account Name")
sasToken = dbutils.widgets.get("SAS")
fileNameAllOptions = dbutils.widgets.get("FileName")
sasURL = "https://"+storageAccountName+".blob.core.windows.net/"+sasToken

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions
import pyspark.sql.types

# COMMAND ----------

# DBTITLE 1,Define Functions
def cleanText(uncleantext):
	  return regexp_replace(uncleantext, "[{}_\"\'():;,.!?\\-]", "")
    
def removebadchars (column):
    return initcap(regexp_replace(regexp_replace(column, "u'",""), "'",""))
  
def extractValsFromArray (column, key, itemNo):
    return trim(cleanText(regexp_replace(array_sort(split(col(column),",")).getItem(itemNo),key,"")))

def mountSettings(mountPoint):
    if any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
            dbutils.fs.unmount(mountPoint)
    
    dbutils.fs.mount(
      source = f"wasbs://{containerName}@{storageAccountName}.blob.core.windows.net/sourceFiles/",
      mount_point = mountPoint,
      extra_configs = {f"fs.azure.sas.{containerName}.{storageAccountName}.blob.core.windows.net": sasToken}
    )

# COMMAND ----------

# DBTITLE 1,Read from Source Mount
mountSettings("/mnt/sourceFiles")

filenamearr = fileNameAllOptions.split(',')
df = {}
for element in filenamearr:
    dfname = element.replace("yelp_academic_dataset_","").replace(".json","")
    df[dfname] = spark.read.option("header","false").json("/mnt/sourceFiles/"+str(element))

# COMMAND ----------

# DBTITLE 1,Segregate Dataframes
reviewDf = df['review']
businessDf = df['business']
checkinDf = df['checkin']
userDf = df['user']

# COMMAND ----------

# DBTITLE 1,Convert Dimension User JSON Information
userDf = userDf.select(
                            userDf["user_id"],
                            userDf["friends"],
                            userDf["average_stars"],
                            userDf["compliment_cool"],
                            userDf["compliment_cute"],
                            userDf["compliment_funny"],
                            userDf["compliment_hot"],
                            userDf["compliment_list"],
                            userDf["compliment_more"],
                            userDf["compliment_note"],
                            userDf["compliment_photos"],
                            userDf["compliment_plain"],
                            userDf["compliment_profile"],
                            userDf["compliment_writer"],
                            userDf["cool"].alias("userDf_cool"),
                            userDf["elite"],
                            userDf["fans"],
                            userDf["funny"].alias("userDf_funny"),
                            userDf["name"].alias("userDf_name"),
                            userDf["review_count"].alias("userDf_review_count"),
                            userDf["useful"].alias("userDf_useful"),
                            userDf["yelping_since"]
                        ).distinct()

display(userDf)

# COMMAND ----------

# DBTITLE 1,Convert Dimension Business JSON Information
#Clean bad characters to be able to derive an array
businesstmpDf = businessDf.withColumn("attributes_BusinessParking", concat_ws(',',array_sort(split(regexp_replace(col("attributes.BusinessParking"),"[ '\{}]",""),","))))\
                   .withColumn("attributes_Music", concat_ws(',',array_sort(split(regexp_replace(col("attributes.Music"),"[ '\{}]",""),","))))\
                   .withColumn("attributes_Ambience", concat_ws(',',array_sort(split(regexp_replace(col("attributes.Ambience"),"[ '\{}]",""),","))))\
                   .withColumn("attributes_GoodForMeal", concat_ws(',', array_sort(split(regexp_replace(col("attributes.GoodForMeal"),"[ '\{}]",""),","))))\
                   .withColumn("attributes_BestNights", concat_ws(',', array_sort(split(regexp_replace(col("attributes.BestNights"),"[ '\{}]",""),","))))\
                   .withColumn("business_review_count", col("review_count"))\
                   .withColumn("business_stars", col("stars"))\
                   .withColumn("business_name", col("name"))\
                   .select(
                            businessDf["business_id"],
                            "business_name",
                            businessDf["address"],
                            businessDf["attributes"],
                            businessDf["categories"],
                            businessDf["city"],
                            businessDf["hours"],
                            businessDf["is_open"],
                            businessDf["latitude"],
                            businessDf["longitude"],
                            businessDf["postal_code"],
                            businessDf["state"],
                            "business_stars",
                            "business_review_count",
                            removebadchars("attributes.AcceptsInsurance").alias("AcceptsInsurance"),
                            removebadchars("attributes.AgesAllowed").alias("AgesAllowed"),
                            removebadchars("attributes.Alcohol").alias("Alcohol"),
                            "attributes_Ambience",
                            removebadchars("attributes.BYOB").alias("BYOB"),
                            removebadchars("attributes.BYOBCorkage").alias("BYOBCorkage"),
                            "attributes_BestNights",
                            removebadchars("attributes.BikeParking").alias("BikeParking"),
                            removebadchars("attributes.BusinessAcceptsBitcoin").alias("BusinessAcceptsBitcoin"),
                            removebadchars("attributes.BusinessAcceptsCreditCards").alias("BusinessAcceptsCreditCards"),
                            "attributes_BusinessParking",
                            removebadchars("attributes.ByAppointmentOnly").alias("ByAppointmentOnly"),
                            removebadchars("attributes.Caters").alias("Caters"),
                            removebadchars("attributes.CoatCheck").alias("CoatCheck"),
                            removebadchars("attributes.Corkage").alias("Corkage"),
                            removebadchars("attributes.DietaryRestrictions").alias("DietaryRestrictions"),
                            removebadchars("attributes.DogsAllowed").alias("DogsAllowed"),
                            removebadchars("attributes.DriveThru").alias("DriveThru"),
                            removebadchars("attributes.GoodForDancing").alias("GoodForDancing"),
                            removebadchars("attributes.GoodForKids").alias("GoodForKids"),
                            "attributes_GoodForMeal",
                            removebadchars("attributes.HairSpecializesIn").alias("HairSpecializesIn"),
                            removebadchars("attributes.HappyHour").alias("HappyHour"),
                            removebadchars("attributes.HasTV").alias("HasTV"),
                            "attributes_Music",
                            removebadchars("attributes.NoiseLevel").alias("NoiseLevel"),
                            removebadchars("attributes.Open24Hours").alias("Open24Hours"),
                            removebadchars("attributes.OutdoorSeating").alias("OutdoorSeating"),
                            removebadchars("attributes.RestaurantsAttire").alias("RestaurantsAttire"),
                            removebadchars("attributes.RestaurantsCounterService").alias("RestaurantsCounterService"),
                            removebadchars("attributes.RestaurantsDelivery").alias("RestaurantsDelivery"),
                            removebadchars("attributes.RestaurantsGoodForGroups").alias("RestaurantsGoodForGroups"),
                            removebadchars("attributes.RestaurantsPriceRange2").alias("RestaurantsPriceRange2"),
                            removebadchars("attributes.RestaurantsReservations").alias("RestaurantsReservations"),
                            removebadchars("attributes.RestaurantsTableService").alias("RestaurantsTableService"),
                            removebadchars("attributes.RestaurantsTakeOut").alias("RestaurantsTakeOut"),
                            removebadchars("attributes.Smoking").alias("Smoking"),
                            removebadchars("attributes.WheelchairAccessible").alias("WheelchairAccessible"),
                            removebadchars("attributes.WiFi").alias("WiFi")
                                       )

#Extract values from each attribute array
businesstmpDf = businesstmpDf.withColumn('HasGoodForMeal_breakfast',extractValsFromArray("attributes_GoodForMeal","breakfast",0))\
                    .withColumn('HasGoodForMeal_brunch',extractValsFromArray("attributes_GoodForMeal","brunch",1))\
                    .withColumn('HasGoodForMeal_dessert',extractValsFromArray("attributes_GoodForMeal","dessert",2))\
                    .withColumn('HasGoodForMeal_dinner',extractValsFromArray("attributes_GoodForMeal","dinner",3))\
                    .withColumn('HasGoodForMeal_latenight',extractValsFromArray("attributes_GoodForMeal","latenight",4))\
                    .withColumn('HasGoodForMeal_lunch',extractValsFromArray("attributes_GoodForMeal","lunch",5))\
                    .withColumn('HasBusinessParking_garage',extractValsFromArray("attributes_BusinessParking","garage",0))\
                    .withColumn('HasBusinessParking_lot',extractValsFromArray("attributes_BusinessParking","lot",1))\
                    .withColumn('HasBusinessParking_street',extractValsFromArray("attributes_BusinessParking","street",2))\
                    .withColumn('HasBusinessParking_valet',extractValsFromArray("attributes_BusinessParking","valet",3))\
                    .withColumn('HasBusinessParking_validated',extractValsFromArray("attributes_BusinessParking","validated",4))\
                    .withColumn('IncludesMusic_backgroundMusic',extractValsFromArray("attributes_Music","background_music",0))\
                    .withColumn('IncludesMusic_dj',extractValsFromArray("attributes_Music","dj",1))\
                    .withColumn('IncludesMusic_jukebox',extractValsFromArray("attributes_Music","jukebox",2))\
                    .withColumn('IncludesMusic_karaoke',extractValsFromArray("attributes_Music","karaoke",3))\
                    .withColumn('IncludesMusic_live',extractValsFromArray("attributes_Music","live",4))\
                    .withColumn('IncludesMusic_noMusic',extractValsFromArray("attributes_Music","no_music",5))\
                    .withColumn('IncludesMusic_video',extractValsFromArray("attributes_Music","video",6))\
                    .withColumn('IsBestNight_Friday',extractValsFromArray("attributes_BestNights","friday",0))\
                    .withColumn('IsBestNight_Monday',extractValsFromArray("attributes_BestNights","monday",1))\
                    .withColumn('IsBestNight_Saturday',extractValsFromArray("attributes_BestNights","saturday",2))\
                    .withColumn('IsBestNight_Sunday',extractValsFromArray("attributes_BestNights","sunday",3))\
                    .withColumn('IsBestNight_Thursday',extractValsFromArray("attributes_BestNights","thursday",4))\
                    .withColumn('IsBestNight_Tuesday',extractValsFromArray("attributes_BestNights","tuesday",5))\
                    .withColumn('IsBestNight_Wednesday',extractValsFromArray("attributes_BestNights","wednesday",6))\
                    .withColumn('IsAmbience_Casual',extractValsFromArray("attributes_Ambience","casual",0))\
                    .withColumn('IsAmbience_Classy',extractValsFromArray("attributes_Ambience","classy",1))\
                    .withColumn('IsAmbience_Divey',extractValsFromArray("attributes_Ambience","divey",2))\
                    .withColumn('IsAmbience_Hipster',extractValsFromArray("attributes_Ambience","hipster",3))\
                    .withColumn('IsAmbience_Intimate',extractValsFromArray("attributes_Ambience","intimate",4))\
                    .withColumn('IsAmbience_Romantic',extractValsFromArray("attributes_Ambience","romantic",5))\
                    .withColumn('IsAmbience_Touristy',extractValsFromArray("attributes_Ambience","touristy",6))\
                    .withColumn('IsAmbience_Trendy',extractValsFromArray("attributes_Ambience","trendy",7))\
                    .withColumn('IsAmbience_Upscale',extractValsFromArray("attributes_Ambience","upscale",8))\
                    .withColumn('HasGoodForMeal_breakfast', when(trim(col("HasGoodForMeal_breakfast"))=="",lit(None)).otherwise(col("HasGoodForMeal_breakfast")))\
                    .withColumn('HasGoodForMeal_brunch', when(trim(col("HasGoodForMeal_brunch"))=="",lit(None)).otherwise(col("HasGoodForMeal_brunch")))\
                    .withColumn('HasGoodForMeal_dessert', when(trim(col("HasGoodForMeal_dessert"))=="",lit(None)).otherwise(col("HasGoodForMeal_dessert")))\
                    .withColumn('HasGoodForMeal_dinner', when(trim(col("HasGoodForMeal_dinner"))=="",lit(None)).otherwise(col("HasGoodForMeal_dinner")))\
                    .withColumn('HasGoodForMeal_latenight', when(trim(col("HasGoodForMeal_latenight"))=="",lit(None)).otherwise(col("HasGoodForMeal_latenight")))\
                    .withColumn('HasGoodForMeal_lunch', when(trim(col("HasGoodForMeal_lunch"))=="",lit(None)).otherwise(col("HasGoodForMeal_lunch")))\
                    .withColumn('HasBusinessParking_garage', when(trim(col("HasBusinessParking_garage"))=="",lit(None)).otherwise(col("HasBusinessParking_garage")))\
                    .withColumn('HasBusinessParking_lot', when(trim(col("HasBusinessParking_lot"))=="",lit(None)).otherwise(col("HasBusinessParking_lot")))\
                    .withColumn('HasBusinessParking_street', when(trim(col("HasBusinessParking_street"))=="",lit(None)).otherwise(col("HasBusinessParking_street")))\
                    .withColumn('HasBusinessParking_valet', when(trim(col("HasBusinessParking_valet"))=="",lit(None)).otherwise(col("HasBusinessParking_valet")))\
                    .withColumn('HasBusinessParking_validated', when(trim(col("HasBusinessParking_validated"))=="",lit(None)).otherwise(col("HasBusinessParking_validated")))\
                    .withColumn('IncludesMusic_backgroundMusic', when(trim(col("IncludesMusic_backgroundMusic"))=="",lit(None)).otherwise(col("IncludesMusic_backgroundMusic")))\
                    .withColumn('IncludesMusic_dj', when(trim(col("IncludesMusic_dj"))=="",lit(None)).otherwise(col("IncludesMusic_dj")))\
                    .withColumn('IsAmbience_Classy', when(trim(col("IsAmbience_Classy"))=="",lit(None)).otherwise(col("IsAmbience_Classy")))\
                    .withColumn('IsAmbience_Hipster', when(trim(col("IsAmbience_Hipster"))=="",lit(None)).otherwise(col("IsAmbience_Hipster")))\
                    .withColumn('IsAmbience_Intimate', when(trim(col("IsAmbience_Intimate"))=="",lit(None)).otherwise(col("IsAmbience_Intimate")))\
                    .withColumn('IsAmbience_Romantic', when(trim(col("IsAmbience_Romantic"))=="",lit(None)).otherwise(col("IsAmbience_Romantic")))\
                    .withColumn('IsAmbience_Touristy', when(trim(col("IsAmbience_Touristy"))=="",lit(None)).otherwise(col("IsAmbience_Touristy")))\
                    .withColumn('IsAmbience_Trendy', when(trim(col("IsAmbience_Trendy"))=="",lit(None)).otherwise(col("IsAmbience_Trendy")))
              
#Select final column list from exploded, cleaned columns
businessfinalDf = businesstmpDf.select(
                            "business_id",
                            "address",
                            "latitude",
                            "longitude",
                            "business_name",
                            "postal_code",
                            "business_review_count",
                            "business_stars",
                            "AcceptsInsurance",
                            "AgesAllowed",
                            "Alcohol",
                            "BYOB",
                            "BYOBCorkage",
                            "BikeParking",
                            "BusinessAcceptsBitcoin",
                            "BusinessAcceptsCreditCards",
                            "ByAppointmentOnly",
                            "Caters",
                            "CoatCheck",
                            "Corkage",
                            "DietaryRestrictions",
                            "DogsAllowed",
                            "DriveThru",
                            "GoodForDancing",
                            "GoodForKids",
                            "HairSpecializesIn",
                            "HappyHour",
                            "HasTV",
                            "NoiseLevel",
                            "Open24Hours",
                            "OutdoorSeating",
                            "RestaurantsAttire",
                            "RestaurantsCounterService",
                            "RestaurantsDelivery",
                            "RestaurantsGoodForGroups",
                            "RestaurantsPriceRange2",
                            "RestaurantsReservations",
                            "RestaurantsTableService",
                            "RestaurantsTakeOut",
                            "Smoking",
                            "WheelchairAccessible",
                            "WiFi",
                            "HasGoodForMeal_dessert",
                            "HasGoodForMeal_latenight",
                            "HasGoodForMeal_lunch",
                            "HasGoodForMeal_dinner",
                            "HasGoodForMeal_brunch",
                            "HasGoodForMeal_breakfast",
                            "HasBusinessParking_garage",
                            "HasBusinessParking_street",
                            "HasBusinessParking_validated",
                            "HasBusinessParking_lot",
                            "HasBusinessParking_valet",
                            "IncludesMusic_dj",
                            "IncludesMusic_backgroundMusic",
                            "IncludesMusic_noMusic",
                            "IncludesMusic_jukebox",
                            "IncludesMusic_live",
                            "IncludesMusic_video",
                            "IncludesMusic_karaoke",
                            "IsBestNight_Monday",
                            "IsBestNight_Tuesday",
                            "IsBestNight_Wednesday",
                            "IsBestNight_Thursday",
                            "IsBestNight_Friday",
                            "IsBestNight_Saturday",
                            "IsBestNight_Sunday",
                            "IsAmbience_Touristy",
                            "IsAmbience_Hipster",
                            "IsAmbience_Romantic",
                            "IsAmbience_Divey",
                            "IsAmbience_Intimate",
                            "IsAmbience_Trendy",
                            "IsAmbience_Upscale",
                            "IsAmbience_Classy",
                            "IsAmbience_Casual")

#Fill any blank fields for each attribute with "Unknown" for string values
businessfinalDf = businessfinalDf.na.fill(value='Unknown',subset=["AcceptsInsurance","AgesAllowed","Alcohol","BYOB","BYOBCorkage","BikeParking","BusinessAcceptsBitcoin","BusinessAcceptsCreditCards","ByAppointmentOnly","Caters","CoatCheck","Corkage","DietaryRestrictions","DogsAllowed","DriveThru","GoodForDancing","GoodForKids","HairSpecializesIn","HappyHour","HasTV","NoiseLevel","Open24Hours","OutdoorSeating","RestaurantsAttire","RestaurantsCounterService","RestaurantsDelivery","RestaurantsGoodForGroups","RestaurantsReservations","RestaurantsTableService","RestaurantsTakeOut","Smoking","WheelchairAccessible","WiFi",
"HasGoodForMeal_breakfast","HasGoodForMeal_brunch","HasGoodForMeal_dessert","HasGoodForMeal_dinner","HasGoodForMeal_latenight","HasGoodForMeal_lunch","HasBusinessParking_garage","HasBusinessParking_lot","HasBusinessParking_street","HasBusinessParking_validated","HasBusinessParking_valet","IncludesMusic_backgroundMusic","IncludesMusic_dj","IncludesMusic_jukebox","IncludesMusic_karaoke","IncludesMusic_live","IncludesMusic_noMusic","IncludesMusic_video","IsBestNight_Friday","IsBestNight_Monday","IsBestNight_Saturday","IsBestNight_Sunday","IsBestNight_Thursday","IsBestNight_Tuesday","IsBestNight_Wednesday","IsAmbience_Casual","IsAmbience_Classy","IsAmbience_Divey","IsAmbience_Hipster","IsAmbience_Intimate","IsAmbience_Romantic","IsAmbience_Touristy","IsAmbience_Trendy","IsAmbience_Upscale"])
#Fill any blank fields for each attribute with 0 for integer values
businessfinalDf =businessfinalDf.na.fill(value=0,subset=["RestaurantsPriceRange2"])

#Leave distinct rows
businessfinalDf = businessfinalDf.distinct()

display(businessfinalDf)

# COMMAND ----------

# DBTITLE 1,Convert Dimension Review Details JSON Information
reviewDetailsDf = reviewDf.select( 
                            reviewDf["review_id"],
                            reviewDf["cool"].alias("review_cool"),
                            reviewDf["date"],
                            reviewDf["funny"].alias("review_funny"),
                            reviewDf["stars"].alias("review_stars"),
                            reviewDf["text"],
                            reviewDf["useful"].alias("review_useful")
                                 ).distinct()

display(reviewDetailsDf)

# COMMAND ----------

# DBTITLE 1,Convert Dimension Friends Details JSON Information
#Derive a dimension with all friends for each user
userFriendDf = userDf.withColumn("friends",explode(split(col("friends"),",")))\
                     .select(
                            "user_id",
                            "friends"
                            ).distinct()

display(userFriendDf)

# COMMAND ----------

# DBTITLE 1,Convert Fact Review JSON Information
#Derive a Fact by utilising date and ID combinations
reviewDf = reviewDf.join(businessDf,businessDf['business_id']==reviewDf['business_id'],'left')\
                   .join(userDf,userDf['user_id']==reviewDf['user_id'],'left')\
                   .withColumn('year',year(col("date")))\
                   .withColumn('month',month(col("date")))\
                   .withColumn('day',dayofmonth(col("date")))\
                   .select(
                           col("year"),
                           col("month"),
                           col("day"),
                           col("date"),
                           reviewDf['review_id'],
                           businessDf['business_id'],
                           userDf['user_id']
                          ).distinct()

display(reviewDf)

# COMMAND ----------

# DBTITLE 1,Save Output to Data Lake
mountSettings("/mnt/outputFile")
businessfinalDf.write.mode("overwrite").parquet(mountPoint+"/ParquetOutput2/Business/")
userDf.write.mode("overwrite").parquet(mountPoint+"/ParquetOutput2/User/")
userFriendDf.write.mode("overwrite").parquet(mountPoint+"/ParquetOutput2/UserFriend/")
reviewDf.write.mode("overwrite").parquet(mountPoint+"/ParquetOutput2/Review/")
reviewDetailsDf.write.mode("overwrite").parquet(mountPoint+"/ParquetOutput2/ReviewDetail/")
