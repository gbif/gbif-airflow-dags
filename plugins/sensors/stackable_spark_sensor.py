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

# DEPRECATED in favor of extended_stackable_spark_sensor

# The code is found in Stackable documentation for Airflow usage: https://docs.stackable.tech/airflow/stable/usage.html
"""Spark-in-k8 sensor implemented by the Stackable team"""

from typing import TYPE_CHECKING, Optional, Dict
from kubernetes import client
from airflow.exceptions import AirflowException
from airflow.sensors.base import BaseSensorOperator
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook, _load_body_to_dict

class SparkKubernetesSensor(BaseSensorOperator):  
    """
    Monitors a SparkApplication resource in kubernetes:
    :param application_name: SparkApplication resource name
    :param namespace: the kubernetes namespace where the SparkApplication reside in
    :param kubernetes_conn_id: The :ref:`kubernetes connection<howto/connection:kubernetes>`
        to Kubernetes cluster.
    :param attach_log: determines whether logs for driver pod should be appended to the sensor log
    :param api_group: SparkApplication API group
    :param api_version: SparkApplication API version
    """

    template_fields = ("application_name", "namespace")
    FAILURE_STATES = ("Failed", "Unknown")
    SUCCESS_STATES = ("Succeeded")

    def __init__(
            self,
            *,
            application_name: str,
            attach_log: bool = False,
            namespace: Optional[str] = None,
            kubernetes_conn_id: str = 'kubernetes_in_cluster',  
            api_group: str = 'spark.stackable.tech',
            api_version: str = 'v1alpha1',
            plural: str = 'sparkapplications',
            poke_interval: float = 60,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.application_name = application_name
        self.attach_log = attach_log
        self.namespace = namespace
        self.kubernetes_conn_id = kubernetes_conn_id
        self.hook = KubernetesHook(conn_id=self.kubernetes_conn_id)
        self.api_group = api_group
        self.api_version = api_version
        self.poke_interval = poke_interval
        self.plural = plural

    def _log_driver(self, application_state: str, response: dict) -> None:
        if not self.attach_log:
            return
        status_info = response["status"]
        if "driverInfo" not in status_info:
            return
        driver_info = status_info["driverInfo"]
        if "podName" not in driver_info:
            return
        driver_pod_name = driver_info["podName"]
        namespace = response["metadata"]["namespace"]
        log_method = self.log.error if application_state in self.FAILURE_STATES else self.log.info
        try:
            log = ""
            for line in self.hook.get_pod_logs(driver_pod_name, namespace=namespace):
                log += line.decode()
            log_method(log)
        except client.rest.ApiException as e:
            self.log.warning(
                "Could not read logs for pod %s. It may have been disposed.\n"
                "Make sure timeToLiveSeconds is set on your SparkApplication spec.\n"
                "underlying exception: %s",
                driver_pod_name,
                e,
            )

    def poke(self, context: Dict) -> bool:
        self.log.info("Poking: %s", self.application_name)
        response = self.hook.get_custom_object(
            group=self.api_group,
            version=self.api_version,
            plural=self.plural,
            name=self.application_name,
            namespace=self.namespace,
        )
        try:
            application_state = response["status"]["phase"]
        except KeyError:
            self.log.debug(f"SparkApplication status could not be established: {response}")
            return False
        if self.attach_log and application_state in self.FAILURE_STATES + self.SUCCESS_STATES:
            self._log_driver(application_state, response)
        if application_state in self.FAILURE_STATES:
            raise AirflowException(f"SparkApplication failed with state: {application_state}")
        elif application_state in self.SUCCESS_STATES:
            self.log.info("SparkApplication ended successfully")
            return True
        else:
            self.log.info("SparkApplication is still in state: %s", application_state)
            return False

    def on_kill(self) -> None:
        self.log.info("Cleaning up after Sparkapplication")
        name = self.application_name
        namespace = self.namespace
        response = self.hook.delete_custom_object(
            group=self.api_group,
            version=self.api_version,
            plural=self.plural,
            namespace=namespace,
            name=name,
        )
        self.log.info("Finish cleaning")
        return response