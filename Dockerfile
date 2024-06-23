FROM python:3.11-slim

RUN apt-get -y update && apt-get -y install curl gpg zip

# Install gsutil
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | \
    tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | \
    gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg && \
    apt-get update -y && apt-get install google-cloud-sdk -y

# Install Spark
ENV SPARK_VERSION=3.2.2
ENV HADOOP_VERSION=2.7
RUN wget -qO- "http://www.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz" | tar -xz -C /usr/local/
ENV SPARK_HOME=/usr/local/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION
ENV PATH=$PATH:$SPARK_HOME/bin

COPY . /app
WORKDIR /app

RUN pip install pyspark pytest