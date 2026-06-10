from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType
from pyspark.sql.functions import from_json, col
import os



# directory where Spark will store its checkpoint data. crucial in streaming to enable fault tolerance
checkpoint_dir = "/tmp/checkpoint/kafka_to_postgres"
if not os.path.exists(checkpoint_dir):
    os.makedirs(checkpoint_dir)
    

# --- Configuration matching the Producer ---
postgres_config = {
    "url": "jdbc:postgresql://postgres:5432/stock_data",        # JDBC URL for connecting to the PostgreSQL database, specifying the host, port, and database name
    "user": "admin",                    # Username for authenticating with the PostgreSQL database
    "password": "admin",            # Password for authenticating with the PostgreSQL database  
    "dbtable": "stocks",            # Name of the database table where the data will be written
    "driver": "org.postgresql.Driver"   # JDBC driver for connecting to the PostgreSQL database
}
    
    
    
# The schema/structure matching the new data coming from kafka
kafka_data_schema = StructType([
    StructField("date", StringType()),      # Define a field named 'date' of type StringType to represent the date of the stock data# 
    StructField("high", StringType()),      # Define a field named 'high' of type StringType to represent the high price of the stock data
    StructField("low", StringType()),
    StructField("open", StringType()),
    StructField("close", StringType()),
    StructField("symbol", StringType())
])


# Initialize the SparkSession with the application name 'KafkaSparkStreaming' and create a SparkSession object that will be used to read from Kafka and process the streaming data
spark = (SparkSession.builder
        .appName('KafkaSparkStreaming')
        .getOrCreate()
)



# Read the streaming data from the Kafka topic 'stock_analysis' using the specified Kafka broker address and configuration options, and load it into a DataFrame for further processing
df = ( spark.readStream.format('kafka')
    .option('kafka.bootstrap.servers', 'kafka:9092')
    .option('subscribe', 'stock_analysis')
    .option('startingOffsets', 'latest') # Read only new incoming messages (ignore old messages in the topic)
    .option('failOnDataLoss', 'false')  # If kafka deletes old messages (retention), Spark wont crash.
    .load()   # start reading the kafka topic as a stream
)


# Convert the 'value' column (which is a json string) into structure columns 
# AKA - Extract the value from the Kafka message and parse it as JSON using the defined schema
parsed_df = df.selectExpr('CAST(value AS STRING)') \
            .select(from_json(col("value"), kafka_data_schema).alias('data')) \
            .select("data.*")  # Select the individual fields from the parsed JSON data for further processing or analysis



# Process the parsed data by selecting and transforming the relevant columns, such as converting the 'date' field to a TimestampType
processed_df = parsed_df.select(
        col("date").cast(TimestampType()).alias("date"),  # Convert the 'date' field to a TimestampType for proper handling of date and time data
        col("high").alias("high"),
        col("low").alias("low"),  # Convert the 'low' field to a FloatType for numerical analysis and calculations
        col("open").alias("open"),
        col("close").alias("close"),
        col("symbol").alias("symbol")
)

##### -----USE THE BELOW TO DEBUG AND TEST THE STREAMING DATA FROM KAFKA BEFORE WRITING TO POSTGRESQL----- #####
# Display the results to the terminal (conmsole output mode) for testing and verification purposes
#query = processed_df.writeStream \
#   .outputMode("append") \
#   .format("console") \
#   .option("truncate", "false") \
#   .option("checkpointLocation", checkpoint_dir) \
#   .start()  # Start the streaming query to continuously process incoming data from the Kafka topic and output the results to the console




# Define a function to write each microbatch of processed data to PostgreSQL using JDBC in 'append' mode, allowing for incremental updates to the database as new data arrives from the Kafka stream
def write_to_postgres(batch_df, batch_id):
    """
    Writes a microbatch DataFrame to PostgreSQL using JDBC in 'append' mode.
    """
    batch_df.write \
        .format("jdbc") \
        .mode("append") \
        .options(**postgres_config) \
        .save()

# --- stream to PostgreSQL using foreachBatch ---
query = (
    processed_df.writeStream
    .foreachBatch(write_to_postgres)  # Use foreachBatch for JDBC sinks
    .option('checkpointLocation', checkpoint_dir)  # directory where Spark will store its checkpoint data. crucial in streaming to enable fault tolerance
    .outputMode('append') # Or 'append', depending on your use case and table schema 
    .start()
)


# Wait for the termination of the query
query.awaitTermination()  # Wait for the streaming query to terminate, allowing the application to run indefinitely until manually stopped or an error occurs
