"""
### DAG Documentation
이 DAG는 HivePartitionSensor를 사용하는 예제입니다.
"""
from __future__ import annotations

from textwrap import dedent

import pendulum
from airflow import DAG
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from airflow.sensors.web_hdfs_sensor import WebHdfsSensor
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.sktvane.operators.nes import NesOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from airflow.utils import timezone
from airflow.utils.edgemodifier import Label
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTablePartitionExistenceSensor


local_tz = pendulum.timezone("Asia/Seoul")

default_args = {
    "retries": 2
}

yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y%m%d')

with DAG(
    "delete_old_lunaId_daily",
    default_args=default_args,
    description="DAG with own plugins",
    schedule="0 22 * * *",
    start_date=pendulum.datetime(2023, 12, 25, tz=local_tz),
    catchup=False,
    max_active_runs=1,
    tags=["test"],
    ) as dag:   
    
    dag.doc_md = __doc__

    yyyy = '{{ ds_nodash[:4] }}'
    mm = '{{ ds_nodash[4:6] }}'
    dd = '{{ ds_nodash[6:8] }}'
    
    start = DummyOperator(task_id='start', dag=dag)
    end = DummyOperator(task_id='end', dag=dag)
    
    delete_old_in_music_home_log = NesOperator(
        task_id="music_home_log_parse",
        parameters={"current_dt": "{{ ds_nodash }}"},
        input_nb="./notebook/delete_apolloId_daily.ipynb",
    )
    
    start >> delete_old_in_music_home_log >> end    