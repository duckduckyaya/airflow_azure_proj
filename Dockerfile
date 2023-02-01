# Use an official Python runtime as the base image
FROM python:3.9

USER root

RUN apt update && apt install -y python3 python3-pip

RUN mkdir /app

COPY scraper/* /app

WORKDIR /app

ENV AIRFLOW_HOME /app/airflow
ENV AIRFLOW_CONFIG /app/airflow/airflow.cfg

RUN pip3 install -r requirements.txt

CMD ["python3", "main.py"]