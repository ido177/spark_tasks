from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T   # let us cast into understandable for spark type 

def main():
    spark = SparkSession.builder.getOrCreate()
    
    df = spark.read.format('csv').option('header', 'true').load('data/cars.csv')
    
    output = (
        df
        .groupBy('manufacturer_name')
        .agg(
            F.count("manufacturer_name").alias("count"), # alias allows to rename columns
            F.round(F.avg("year_produced")).cast(T.IntegerType()).alias("avg_year"), 
            F.min(F.col("price_usd").cast(T.FloatType())).alias("min_price"),
            F.max(F.col("price_usd").cast(T.FloatType())).alias("max_price")
        )
    )
    
    output.show(5)

    output.write.mode('overwrite').format('json').save("result.json")

                                          
main()