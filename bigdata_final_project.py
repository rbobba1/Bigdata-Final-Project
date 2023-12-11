from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofmonth

#input/output locations
data_path = "s3://hakky/NYC_parking_data_2017.csv"
output_path = "s3://hakky/"

def main():
    spark = SparkSession.builder.appName('bigdatafinal').getOrCreate()  
    
    #reading the data and storing the data frame

    data = spark.read.csv(data_path, header=True)
    
    #getting the total number of entries and printing them...

    Total_Entries = data.count()
    
    print(f"The total number of entries are {Total_Entries}")

    # Total number of violations by registration state
    violations_by_state = data.groupBy("Registration State").count().orderBy(col("count").desc())
    violations_by_state.show()

    #tolal number of Issuer Code for with respective to cars..
    code_count = data.groupBy("Issuer Code").count().orderBy(col("count").desc())
    code_count.show()

    #count based on date
    count_by_issue_date = data.groupBy("Issue Date").count().orderBy(col ("count").desc())
    count_by_issue_date.show()

    # Total number of violations by description
    violations_by_description = data.groupBy("Violation Description").count().orderBy(col("count").desc())
    violations_by_description.show(truncate=False)

    #vehicle body count.
    count = data.groupBy("Vehicle Body Type").count().orderBy(col("count").desc())
    count.show()

    #voilation time..
    time = data.groupBy("Violation Time").count().orderBy(col("count").desc())
    time.show()





main()
