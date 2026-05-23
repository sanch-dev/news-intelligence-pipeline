FROM apache/airflow:2.13.0-python3.11

USER root

RUN apt-get update && apt-get install -y \
    git \
    curl \
    && apt-get clean
opt
USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /home/airflow