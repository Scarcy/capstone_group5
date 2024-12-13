# Base image with Spark pre-installed
FROM apache/spark:latest

# Set working directory
WORKDIR /capstone

# Install additional tools if needed (e.g., fzf)
# RUN apt-get update && apt-get install -y fzf

# Copy necessary files into the container
COPY pums /capstone/pums
COPY db_cass.zip /capstone/

# Set environment variables for Spark
ENV SPARK_CONF_DIR=/capstone/spark-conf
ENV SPARK_HOME=/opt/spark

# Copy custom configuration (if required)
RUN mkdir -p $SPARK_CONF_DIR

# Install dependencies using sbt
WORKDIR /capstone/pums
# RUN apt-get update && apt-get install -y openjdk-11-jdk sbt && sbt package

# Entry point for the application
ENTRYPOINT ["/opt/spark/bin/spark-submit"]
