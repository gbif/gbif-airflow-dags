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

from airflow.models import BaseOperator,Variable
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook,_load_body_to_dict
from datetime import datetime,timedelta
from jinja2 import Template
import json
from typing import TYPE_CHECKING, Optional, Sequence, Dict

if TYPE_CHECKING:
    from airflow.utils.context import Context

class CustomSparkKubernetesOperator(BaseOperator):  
    """
    Creates a SparkApplication resource in kubernetes:
    :param application_file: Defines a file name of avaiable templates to load
    :param custom_params: A json object containing the values to use while templating
    :param namespace: kubernetes namespace for the SparkApplication
    :param kubernetes_conn_id: The :ref:`kubernetes connection id <howto/connection:kubernetes>`
        for the Kubernetes cluster.
    :param api_group: SparkApplication API group
    :param api_version: SparkApplication API version
    """

    template_fields : Sequence[str] = ('custom_params', 'timestamp', 'computed_name')
    def __init__(
        self,
        *,
        application_file: Optional[str] = "default.yaml",
        custom_params: object = None,
        namespace: Optional[str] = None,
        kubernetes_conn_id: str = 'kubernetes_in_cluster',  
        api_group: str = 'spark.stackable.tech',
        api_version: str = 'v1alpha1',
        timestamp: str = None,
        computed_name: str = None,
        pass_timestamp_as_args: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.application_file = application_file
        self.custom_params = custom_params
        self.namespace = namespace
        self.kubernetes_conn_id = kubernetes_conn_id
        self.api_group = api_group
        self.api_version = api_version
        self.plural = "sparkapplications"
        self.timestamp = timestamp
        self.computed_name = computed_name
        self.pass_timestamp_as_args = pass_timestamp_as_args

    def procesTemplate(self, template, custom_params):
        # As the custom_params gets passed in as a string at runtime to the operator we need to sanitize the string so the json lib can convert it into a dictonary
        params_as_dict = json.loads(custom_params.replace("\'", "\""))
        self.log.info(params_as_dict)
        if self.timestamp != None:
            # Adding the datatime to the dict for the template
            params_as_dict.update({"timestamp": self.timestamp})
        if self.computed_name != None:
            # Adding a computed name based to the dict for the template
            params_as_dict.update({"computed_name": self.computed_name})
        if self.pass_timestamp_as_args == True:
            # Adding timestamp in at the end of the args string
            params_as_dict["args"].append(self.timestamp)
        if custom_params != None:
            with open(Variable.get('template_location') + template) as file_:
                sparkApptemplate = Template(file_.read())
            return_value = str(sparkApptemplate.render(params_as_dict))
            self.log.info(return_value)
            return return_value
        else:
            self.log.warn("No params were passed to the operator, won't process template")

    def execute(self, context: 'Context'):
        hook = KubernetesHook(conn_id=self.kubernetes_conn_id)
        self.log.info("Creating SparkApplication...")
        response = hook.create_custom_object(
            group=self.api_group,
            version=self.api_version,
            plural=self.plural,
            body=self.procesTemplate(self.application_file, self.custom_params),
            namespace=self.namespace,
        )
        return response