# syntax=docker/dockerfile:1

FROM python:3.9-slim

RUN mkdir -p /opt/dagster/dagster_home /opt/dagster/app

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . /opt/dagster/app/

ENV DAGSTER_HOME=/opt/dagster/dagster_home/

COPY dagster.yaml /opt/dagster/dagster_home/

WORKDIR /opt/dagster/app

EXPOSE 5000

ENTRYPOINT ["bash", "startup.sh"]