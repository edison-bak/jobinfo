FROM apache/airflow:2.5.1
LABEL Maintainer="wearelego@naver.com"

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip3 install --user -r /opt/airflow/requirements.txt

# DAG 파일 복사
COPY dags/ /opt/airflow/dags/
