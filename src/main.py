from sys import argv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import year, month, to_date, regexp_replace, sum

# salesFile = './dataset/Global Superstore Sales - Global Superstore Sales.csv'
# returnFile = './dataset/Global Superstore Sales - Global Superstore Returns.csv'

def main(salesFile, returnFile):
    spark = SparkSession.builder.appName("spark_pgm").master("local[*]").getOrCreate()
    dfSales :DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(salesFile)
    #dfSales.show()

    dfReturn :DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(returnFile)
    #dfReturn.show()
    dfCombinedSales :DataFrame = dfSales.join(dfReturn, ["Order ID"], "leftanti")

    dfFormattedSales = dfCombinedSales.withColumn("Order Date", to_date(dfCombinedSales["Order Date"], "MM/dd/yyyy")).withColumn("Profit", regexp_replace("Profit", "[\$,]", "")).cast("float"))
    # Create year and month                   
    dfFormattedSales = dfFormattedSales.withColumn("Year", year(dfFormattedSales["Order Date"])).withColumn("Month", month(dfFormattedSales["Order Date"]))
    # aggregate (sum) Profit and Quantity, based on Year, Month, Category, Sub Category
    dfFormattedSales = dfFormattedSales.groupBy(["Year", "Month", "Category", "Sub-Category"]).agg(sum("Profit").alias("Total Profit"),sum("Quantity").alias("Total Quantity Sold"))
    # Store the data in a partition based on year month
    dfFormattedSales.write.partitionBy(["Year", "Month"]).mode("overwrite").format("csv").option("header", "true").save("./dataset/case_study_report.csv")

# unit test case
def check_join_count (df1: int, df2: int):
    """ Testing the join operation """
    assert df1==df2
check_join_count(dfSales.count(), dfFormattedSales.count())


if __name__ == '__main__':
    # Sales and retun as input
    _, salesFile, returnFile = argv
    main(salesFile, returnFile)  
