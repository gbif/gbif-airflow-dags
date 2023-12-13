#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from airflow import DAG
from airflow.models import Variable
from airflow.models.param import Param
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime,timedelta
from gparams.default_params_for_spark import DefaultParamsForSpark
from operators.custom_spark_operator import CustomSparkKubernetesOperator
from sensors.extended_stackable_spark_sensor import ExtendedSparkKubernetesSensor
import requests


def update_download_status(dag_id, status, callback_url):
    response = requests.get(callback_url + '?job_id=' + dag_id + '&status=' + status)
    response.raise_for_status()

def get_download_key(context):
    return context['run_id'].replace('download-','')

def on_success_download(context):
    update_download_status(get_download_key(context),'SUCCEEDED', context["params"]["callbackUrl"])

def on_failure_download(context):
    update_download_status(get_download_key(context),'FAILED', context["params"]["callbackUrl"])

def on_execute_download(context):
    update_download_status(get_download_key(context),'RUNNING', context["params"]["callbackUrl"])

with DAG(
    dag_id='gbif_occurrence_download_dag',
    schedule_interval='0 5 * * *',
    start_date=datetime(2023, 7, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=180),
    tags=['spark_executor', 'GBIF', 'occurrence_download'],
    params = DefaultParamsForSpark.OCCURRENCE_DOWNLOAD,
) as dag:

    spark_submit_main_stage = CustomSparkKubernetesOperator(
        task_id='spark_submit_main_stage',
        namespace = Variable.get('namespace_to_run'),
        application_file="spark_job_template_spark3_3.yaml",
        custom_params="{{ params }}",
        computed_name="dwnld-{{ params.args[0] }}",
        do_xcom_push=True,
        dag=dag,
        on_success_callback=on_success_download,
        on_failure_callback=on_failure_download,
    )

    spark_monitor_main_stage = ExtendedSparkKubernetesSensor(
        task_id='spark_monitor_main_stage',
        namespace = Variable.get('namespace_to_run'),
        application_name="{{ task_instance.xcom_pull(task_ids='spark_submit_main_stage')['metadata']['name'] }}",
        poke_interval=10,
        on_success_callback=on_success_download,
        on_failure_callback=on_failure_download,
    )

    spark_submit_main_stage >> spark_monitor_main_stage
