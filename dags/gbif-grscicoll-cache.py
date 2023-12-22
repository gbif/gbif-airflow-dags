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
from operators.trino_operator import TrinoOperator

with DAG(
    dag_id='gbif_grscicoll_cache_dag',
    schedule_interval='0 1 * * 6',
    start_date=datetime(2023, 11, 8),
    catchup=False,
    dagrun_timeout=timedelta(minutes=180),
    tags=['spark_executor', 'GBIF', 'grscicoll_cache'],
    params = {
                     "sparkName": "grscicoll-cache",
                     "version": "1.33-H3-SNAPSHOT ",
                     "component": "kvs-indexing",
                     "main": "org.gbif.kvs.indexing.grscicoll.GrscicollPipelineWorkflow",
                     "hdfsClusterName": "gbif-hdfs",
                     "hiveClusterName": "gbif-hive-metastore",
                     "hbaseClusterName": "gbif-hbase",
                     "componentConfig": "grscicoll-cache",
                     "driverCores": "2000m",
                     "driverMemory": "2Gi",
                     "executorInstances": 6,
                     "executorCores": "6000m",
                     "executorMemory": "8Gi",
                 },

) as dag:

    create_trino_table = TrinoOperator(
      task_id='create_trino_table',
      trino_conn_id='gbif_trino_conn',
      sql="""DROP TABLE IF EXISTS marcos.occurrence_collections;
           CREATE TABLE marcos.occurrence_collections WITH (format = 'PARQUET') AS
           SELECT DISTINCT ownerInstitutionCode, institutionId, institutionCode, collectionCode,
           collectionId, datasetKey,
           CASE WHEN datasetKey = '4fa7b334-ce0d-4e88-aaae-2e0c138d049e' THEN 'US' ELSE publishingCountry END AS country
           FROM dev2.occurrence;""")

    spark_submit_main_stage = CustomSparkKubernetesOperator(
        task_id='spark_submit_main_stage',
        namespace = Variable.get('namespace_to_run'),
        application_file="spark_job_template.yaml",
        custom_params="{{ params }}",
        do_xcom_push=True,
        dag=dag,
    )

    spark_monitor_main_stage = ExtendedSparkKubernetesSensor(
        task_id='spark_monitor_main_stage',
        namespace = Variable.get('namespace_to_run'),
        application_name="{{ task_instance.xcom_pull(task_ids='spark_submit_main_stage')['metadata']['name'] }}",
        poke_interval=10,
        dag=dag,
    )

    create_trino_table >> spark_submit_main_stage >> spark_monitor_main_stage
