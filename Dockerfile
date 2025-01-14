FROM python:3.11.9

RUN apt-get update && \
    apt-get install -y default-jdk curl && \
    apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

WORKDIR /app
COPY . /app
RUN python3 -m pip install --no-cache-dir -r requirements.txt


