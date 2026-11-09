FROM python:3.11-slim-bookworm

ARG LAB_UID=1000
ARG LAB_GID=1000
ARG SPARK_VERSION=3.5.8
ARG HADOOP_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262
ENV JAVA_HOME=/opt/java SPARK_HOME=/opt/spark
ENV PATH="/opt/spark/bin:${PATH}" \
    PYTHONPATH=/opt/spark/python:/opt/py4j.zip \
    PYSPARK_PYTHON=/usr/local/bin/python3 \
    SPARK_WORKER_DIR=/home/lab/spark-work

RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless bash curl ca-certificates procps tini \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s "/usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture)" /opt/java

RUN set -eu; \
    file="spark-${SPARK_VERSION}-bin-hadoop3.tgz"; \
    base="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}"; \
    cd /tmp; \
    curl -fSL --retry 3 "$base/$file" -o "$file"; \
    curl -fsSL --retry 3 "$base/$file.sha512" -o spark.sha512; \
    sha512sum -c spark.sha512; \
    mkdir -p /opt/spark; \
    tar -xzf "$file" -C /opt/spark --strip-components=1; \
    rm "$file" spark.sha512; \
    ln -s /opt/spark/python/lib/py4j-*-src.zip /opt/py4j.zip; \
    test -f /opt/py4j.zip; \
    cd /opt/spark/jars; \
    test -f "hadoop-client-runtime-${HADOOP_VERSION}.jar"; \
    repo=https://repo.maven.apache.org/maven2; \
    curl -fSLO --retry 3 "$repo/org/apache/hadoop/hadoop-aws/$HADOOP_VERSION/hadoop-aws-$HADOOP_VERSION.jar"; \
    curl -fSLO --retry 3 "$repo/com/amazonaws/aws-java-sdk-bundle/$AWS_SDK_VERSION/aws-java-sdk-bundle-$AWS_SDK_VERSION.jar"

RUN pip install --no-cache-dir jupyterlab==4.4.10 \
    && groupadd -o -g "$LAB_GID" lab \
    && useradd -m -o -u "$LAB_UID" -g lab -s /bin/bash lab \
    && install -d -o lab -g lab /home/lab/spark-work /home/lab/notebooks

COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf
USER lab
WORKDIR /home/lab
ENTRYPOINT ["/usr/bin/tini", "--"]
