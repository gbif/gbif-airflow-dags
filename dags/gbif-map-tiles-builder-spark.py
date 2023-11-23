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
    dag_id='gbif_map_tiles_builder_dag',
    schedule_interval='0 3 * * *',
    start_date=datetime(2023, 7, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=180),
    tags=['spark_executor', 'GBIF', 'map_tiles'],
    params= {
        "prepare": DefaultParamsForSpark.MAP_PREFLIGHT_TILES,
        "calculate": DefaultParamsForSpark.MAP_TILES,
        "finalize": DefaultParamsForSpark.MAP_POSTFLIGHT_TILES,
    },
) as dag:

    spark_submit_prepare_stage = CustomSparkKubernetesOperator(
        task_id='spark_submit_prepare_stage',
        namespace = Variable.get('namespace_to_run'),
        application_file="spark_job_using_local_hdfs_jar.yaml",
        custom_params="{{ params.prepare }}",
        timestamp="{{ ts_nodash }}",
        pass_timestamp_as_args=True,
        do_xcom_push=True,
        dag=dag,
    )

    spark_submit_calculate_stage = CustomSparkKubernetesOperator(
        task_id='spark_submit_calculate_stage',
        namespace = Variable.get('namespace_to_run'),
        application_file="spark_job_using_local_hdfs_jar.yaml",
        custom_params="{{ params.calculate }}",
        timestamp="{{ ts_nodash }}",
        pass_timestamp_as_args=True,
        do_xcom_push=True,
        dag=dag,
    )

    spark_submit_finalize_stage = CustomSparkKubernetesOperator(
        task_id='spark_submit_finalize_stage',
        namespace = Variable.get('namespace_to_run'),
        application_file="spark_job_using_local_hdfs_jar.yaml",
        custom_params="{{ params.finalize }}",
        timestamp="{{ ts_nodash }}",
        pass_timestamp_as_args=True,
        do_xcom_push=True,
        dag=dag,
    )

    spark_monitor_prepare_stage = ExtendedSparkKubernetesSensor(
        task_id='spark_monitor_prepare_stage',
        namespace = Variable.get('namespace_to_run'),
        application_name="{{ task_instance.xcom_pull(task_ids='spark_submit_prepare_stage')['metadata']['name'] }}",
        poke_interval=10,
        dag=dag,
    )

    spark_monitor_calculate_stage = ExtendedSparkKubernetesSensor(
        task_id='spark_monitor_calculate_stage',
        namespace = Variable.get('namespace_to_run'),
        application_name="{{ task_instance.xcom_pull(task_ids='spark_submit_calculate_stage')['metadata']['name'] }}",
        poke_interval=10,
        dag=dag,
    )

    spark_monitor_finalize_stage = ExtendedSparkKubernetesSensor(
        task_id='spark_monitor_finalize_stage',
        namespace = Variable.get('namespace_to_run'),
        application_name="{{ task_instance.xcom_pull(task_ids='spark_submit_finalize_stage')['metadata']['name'] }}",
        poke_interval=10,
        dag=dag,
    )

    spark_submit_prepare_stage >> spark_monitor_prepare_stage >> spark_submit_calculate_stage >> spark_monitor_calculate_stage >> spark_submit_finalize_stage >> spark_monitor_finalize_stage