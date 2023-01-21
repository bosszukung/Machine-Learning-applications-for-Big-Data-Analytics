Initiate and Configure Spark Streaming

# Load Spark engine
#!pip3 install pyspark
import findspark
findspark.init()

# Import libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Defining a function that adds an 'index' to the dataframe
def dfIndexed(df):
    w = Window.partitionBy(lit(1)).orderBy(lit(1))
    # Enumerates the rows from the dataframe to create an index
    dfIndex = df.withColumn("index", row_number().over(w))

    return dfIndex
    

# Defining the process that each batch of the input goes through
def inputProcess(df, epochId):
    # Checking if the input dataframe is empty
    if df.count() != 0: 
        # Calling a pre-defined function (dfIndexed) that adds an 'index' to the dataframe 'df' and stores it into a variable called 'dfIndex'
        dfIndex = dfIndexed(df)
        # Splitting the dataframe into Odd or Even, by checking if they are devisible by two or have a remainder 
        Even = dfIndex.where(col("index") % 2 == 0)
        Odd = dfIndex.where(col("index") % 2 != 0)
        # Checks if the value in the 'Even' dataframe has non-digits and replaces them with an empty string
        Even = Even.withColumn('value', regexp_replace('value', '\D', ''))
        # The seperate 'Odd' and 'Even' dataframes are merged together to form a 'combinedDF'
        combinedDF = Odd.union(Even).sort("index")
        # To process digits within words, this checks for a digit and adds a space before and after it for better aggregation
        combinedDF = combinedDF.withColumn('value', regexp_replace('value', r'(\d)', r' $1 '))

        # Splits the input sentences into seperate inputs
        inputString = combinedDF.select(explode(split(col("value"), " ")).alias("Input"))
        # Creates a column that identifies the input length
        inputString = inputString.withColumn("InputLength", length(col("Input")))
        # Classifing the inputs based on the word length (>= 5) and check for digits
        inputString = inputString.withColumn("FinalInput",
                                             when((col("Input").cast("int").isNotNull()) == True, col("input"))
                                             .when((col("InputLength") < 5), "null")
                                             .when((col("InputLength") >= 5), "Word Length >= 5")
                                             .otherwise(col("InputLength")))
        # Outputs the valid elements of the final output
        inputString.where(~col('FinalInput').contains('null')).show(100)
        # Aggregation of the selected inputs
        wordCounts = inputString.groupBy("FinalInput").count().filter(col("FinalInput") != "null").orderBy("FinalInput",
                                                                                                           ascending=True)
        # Outputs the final processed data
        wordCounts.show()
        return wordCounts
    else: pass
    
    
# Defining the function for the main program 
def main():
    # Initiate a local spark session that uses two working threads
    spark = SparkSession.builder.appName("StructuredStreaming_Group11").master("local[2]").getOrCreate()  # Initate Spark Session
    # 'lines' stores the text data from the socket connection based on the local server
    # cmd -> nc -lk 7777
    lines = spark.readStream.format("socket").option("host", "localhost").option("port", 7777).load()
    
    # Start the streaming computation based on the 'foreachBatch' sink and 'update' based on the current micro-batch
    query = lines.writeStream.outputMode("update").format("console").foreachBatch(inputProcess).queryName("wordOut").start()
    query.awaitTermination()
    
    
    
    
# Runs the main program
if __name__ == "__main__":
    try:
        print("Application started")
        main()
    finally:
        print("Application stopped.")
        
        
#Test input:
    #"Welcom3 2 th3 structured stream1ng t35t"
    #"words that g3ts inputted here 15 stored" 
    #"we used Pyspark to d0 data str3aming"
    #"but th1s input is irrelavant to the w0rd count"
