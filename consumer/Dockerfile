FROM spark:3.5.1-python3
# Use the official Spark image as the base image

USER root
# Use the root user to perform operations that require elevated privileges, such as creating directories and setting permissions

ENV SPARK_JARS_IVY=/opt/spark/work-dir/.ivy2 \
    ALPHA_TZ_DEFAULT=US/Eastern
# Set environment variables for Spark to specify the location of the Ivy cache for managing dependencies and to set the default timezone for the application to US/Eastern

RUN mkdir -p /opt/spark/work-dir/.ivy2 /opt/spark/work-dir/apps \
    && chown -R spark:spark /opt/spark/work-dir
# Create necessary directories for the Spark application and set ownership to the spark user to ensure that the application can read and write files in these directories without permission issues

COPY --chown=spark:spark consumer.py /opt/spark/work-dir/apps/consumer.py
# Copy the consumer.py file from the local directory to the specified location in the Docker image, while setting the ownership to the spark user to ensure that the application can access and execute the file without permission issues

USER spark
# Switch to the spark user to run the application with appropriate permissions and to avoid running the application as the root user, which is a security best practice

# Define the command to run the Spark application using spark-submit, specifying the master URL for the Spark cluster, the deploy mode, and the necessary configurations for managing dependencies and setting timezone options. The command also includes the path to the consumer.py file that contains the Spark application code.
CMD ["/opt/spark/bin/spark-submit", \
    "--master", "spark://spark-master:7077", \
    "--deploy-mode", "client", \
    "--conf", "spark.jars.ivy=/opt/spark/work-dir/.ivy2", \
    "--conf", "spark.executor.extraJavaOptions=-Duser.timezone=UTC", \
    "--conf","spark.driver.extraJavaOptions=-Duser.timezone=UTC", \
    "--packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3", \
    "/opt/spark/work-dir/apps/consumer.py"\
    ]