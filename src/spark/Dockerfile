# Use an appropriate base image
FROM openjdk:8-jre-slim

# Install necessary dependencies
RUN apt-get update
RUN apt-get install -y python3 python3-pip wget
RUN rm -rf /var/lib/apt/lists/*

# Install Python packages
RUN pip3 install google-cloud-bigquery websocket-client apscheduler

# Set environment variables
ENV SPARK_VERSION=3.2.0
ENV HADOOP_VERSION=3.2
ENV SPARK_HOME=/opt/spark
ENV PYTHONUNBUFFERED=1
ENV PATH=$SPARK_HOME/bin:$PATH

# Download and install Apache Spark
RUN wget -qO- "https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz" | tar xvz -C /opt
RUN mv /opt/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION $SPARK_HOME
RUN rm -rf $SPARK_HOME/examples $SPARK_HOME/kubernetes $SPARK_HOME/data

# Set the working directory and copy application files
WORKDIR /opt/spark_app
COPY load.py ./load.py
COPY tubes-big-data-422412-f473452a086a.json ./tubes-big-data-422412-f473452a086a.json

# Command to run your Spark application
CMD ["spark-submit", "--master", "local[*]", "load.py"]