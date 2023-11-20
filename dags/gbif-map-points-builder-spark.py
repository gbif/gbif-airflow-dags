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
from params.default_params_for_spark import DefaultParamsForSpark
from operators.custom_spark_operator import CustomSparkKubernetesOperator
from sensors.extended_stackable_spark_sensor import ExtendedSparkKubernetesSensor

with DAG(
    dag_id='gbif_map_points_builder_dag',
    schedule_interval='0 21 * * *',
    start_date=datetime(2023, 7, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=180),
    tags=['spark_executor', 'GBIF', 'map_points'],
    params= {
        "prepare": DefaultParamsForSpark.MAP_PREFLIGHT,
        "calculate": DefaultParamsForSpark.MAP_POINTS,
        "finalize": DefaultParamsForSpark.MAP_POSTFLIGHT,
    },
) as dag:

    spark_submit_calculate_stage = CustomSparkKubernetesOperator(
        task_id='spark_submit_calculate_stage',
        namespace = Variable.get('namespace_to_run'),
        application_file="spark_job_template.yaml",
        custom_params="{{ params.calculate }}",
        do_xcom_push=True,
        dag=dag,
    )

    spark_monitor_calculate_stage = ExtendedSparkKubernetesSensor(
        task_id='spark_monitor_calculate_stage',
        namespace = Variable.get('namespace_to_run'),
        application_name="{{ task_instance.xcom_pull(task_ids='spark_submit_calculate_stage')['metadata']['name'] }}",
        poke_interval=10,
        dag=dag,
    )

    spark_submit_calculate_stage >> spark_monitor_calculate_stage