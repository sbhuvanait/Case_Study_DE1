from sys import argv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import year, month, to_date, regexp_replace, sum

# salesFile = './dataset/Global Superstore Sales - Global Superstore Sales.csv'
# returnFile = './dataset/Global Superstore Sales - Global Superstore Returns.csv'

def main(salesFile, returnFile):
    spark = SparkSession.builder.appName("case_study_1").master("local[*]").getOrCreate()
    spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    dfSales :DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(salesFile)
    #dfSales.show()

    dfReturn :DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(returnFile)
    #dfReturn.show()

    # TODO: For the total profit and total quantity calculations the returns data should be used to exclude all returns
    # Filter out the the data matching in retuns for Sale file
    dfCombinedSales :DataFrame = dfSales.join(dfReturn, ["Order ID"], "leftanti")

    # Casting fields
    dfFormattedSales = dfCombinedSales\
                        .withColumn("Order Date", to_date(dfCombinedSales["Order Date"], "MM/dd/yyyy"))\
                        .withColumn("Profit", regexp_replace("Profit", "\\$", ""))\
                        .withColumn("Profit", regexp_replace("Profit", ",", "").cast("float"))

    # Creating year and month fields                   
    dfFormattedSales = dfFormattedSales.withColumn("Year", year(dfFormattedSales["Order Date"]))\
                        .withColumn("Month", month(dfFormattedSales["Order Date"]))
    
    # TODO: That contains the aggregate (sum) Profit and Quantity, based on Year, Month, Category, Sub Category
    dfFormattedSales = dfFormattedSales.groupBy(["Year", "Month", "Category", "Sub-Category"])\
                                        .agg(sum("Profit").alias("Total Profit"),
                                            sum("Quantity").alias("Total Quantity Sold"))
    
    # TODO: This data has to be stored in a partition based on year month. like year=2014/month=11
    dfFormattedSales.coalesce(1)\
        .write.partitionBy(["Year", "Month"]).mode("overwrite").format("csv")\
        .option("header", "true")\
        .save("./dataset/case_study_report.csv")\


# TODO: UNIT TEST
if __name__ == '__main__':
    # TODO: Inputs should be sales and return files
    _, salesFile, returnFile = argv
    main(salesFile, returnFile)  