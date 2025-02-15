FROM python:3.9-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jdk \
    wget \
    && rm -rf /var/lib/apt/lists/*

ENV SPARK_VERSION=3.5.4
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

RUN mkdir -p ${SPARK_HOME}

RUN wget -q https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    && tar xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    && mv spark-${SPARK_VERSION}-bin-hadoop3/* ${SPARK_HOME}/ \
    && rm spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    && rm -rf spark-${SPARK_VERSION}-bin-hadoop3

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /app
COPY . .

ENTRYPOINT ["python"]
CMD ["main.py"]