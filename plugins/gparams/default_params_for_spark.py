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

from datetime import datetime

class DefaultParamsForSpark:

    MAP_BASE: object = {
        "version": "1.0.4",
        "component": "spark-generate-maps",
        "hdfsClusterName": "gbif-hdfs",
        "hiveClusterName": "gbif-hive-metastore",
        "hbaseClusterName": "gbif-hbase",
        "componentConfig": "spark-generate-maps",
    }

    MAP_FLIGHT: object = {
        "driverCores": "2000m",
        "driverMemory": "2Gi",
        "executorInstances": 1,
        "executorCores": "2000m",
        "executorMemory": "2Gi",
    }

    MAP_TILES: object = {
        "sparkName": "generate-map-tiles",
        "args": ["tiles", "config.yaml"],
        "main": "org.gbif.maps.workflow.Backfill",
        "driverCores": "2000m",
        "driverMemory": "2Gi",
        "executorInstances": 6,
        "executorCores": "8000m",
        "executorMemory": "12Gi",
    }

    MAP_TILES.update(MAP_BASE)

    MAP_POINTS: object = {
        "sparkName": "generate-map-points",
        "args": ["points", "config.yaml"],
        "main": "org.gbif.maps.workflow.Backfill",
        "driverCores": "2000m",
        "driverMemory": "2Gi",
        "executorInstances": 6,
        "executorCores": "8000m",
        "executorMemory": "12Gi",
    }

    MAP_POINTS.update(MAP_BASE)

    MAP_PREFLIGHT_POINTS: object = {
        "sparkName": "generate-map-point-pre",
        "args": ["points", "config.yaml"],
        "main": "org.gbif.maps.workflow.PrepareBackfill",
    }

    MAP_PREFLIGHT_POINTS.update(MAP_BASE)
    MAP_PREFLIGHT_POINTS.update(MAP_FLIGHT)

    MAP_POSTFLIGHT_POINTS: object = {
        "sparkName": "generate-map-points-post",
        "args": ["points", "config.yaml"],
        "main": "org.gbif.maps.workflow.FinaliseBackfill",
    }

    MAP_POSTFLIGHT_POINTS.update(MAP_BASE)
    MAP_POSTFLIGHT_POINTS.update(MAP_FLIGHT)

    MAP_PREFLIGHT_TILES: object = {
        "sparkName": "generate-map-tiles-pre",
        "args": ["tiles", "config.yaml"],
        "main": "org.gbif.maps.workflow.PrepareBackfill",
    }

    MAP_PREFLIGHT_TILES.update(MAP_BASE)
    MAP_PREFLIGHT_TILES.update(MAP_FLIGHT)

    MAP_POSTFLIGHT_TILES: object = {
        "sparkName": "generate-map-tiles-post",
        "args": ["tiles", "config.yaml"],
        "main": "org.gbif.maps.workflow.FinaliseBackfill",
    }

    MAP_POSTFLIGHT_TILES.update(MAP_BASE)
    MAP_POSTFLIGHT_TILES.update(MAP_FLIGHT)

    OCCURRENCE_TABLE_BUILD: object = {
        "sparkName": "occurrence-table-build",
        "args": ["/etc/gbif/config.yaml", "CREATE", "ALL"],
        "version": "1.1.20",
        "component": "occurrence-table-build-spark",
        "main": "org.gbif.occurrence.table.backfill.TableBackfill",
        "hdfsClusterName": "gbif-hdfs",
        "hiveClusterName": "gbif-hive-metastore",
        "hbaseClusterName": "gbif-hbase",
        "componentConfig": "occurrence-table-build",
        "driverCores": "2000m",
        "driverMemory": "2Gi",
        "executorInstances": 6,
        "executorCores": "6000m",
        "executorMemory": "10Gi",
    }

    OCCURRENCE_DOWNLOAD: object = {
        "sparkName": "occurrence-downloads",
        "args": ["", "Occurrence"],
        "version": "0.194.0S43",
        "component": "occurrence-download-spark",
        "main": "org.gbif.occurrence.download.spark.SparkDownloads",
        "hdfsClusterName": "gbif-hdfs",
        "hiveClusterName": "gbif-hive-metastore",
        "hbaseClusterName": "gbif-hbase",
        "componentProperty": {
            "propertyName": "occurrence",
            "path": "/stackable/spark/jobs/",
            "file": "download.properties",
        },
        "driverCores": "2000m",
        "driverMemory": "2Gi",
        "executorInstances": 6,
        "executorCores": "6000m",
        "executorMemory": "10Gi",
        "callbackUrl": "",
    }

    GRIDDED_DATASETS: object = {
        "sparkName": "gridded-datasets",
        "args": ["config.yaml"],
        "version": "1.1.0-SNAPSHOT",
        "component": "gridded-datasets",
        "main": "org.gbif.gridded.datasets.GriddedDatasets",
        "hdfsClusterName": "gbif-hdfs",
        "hiveClusterName": "gbif-hive-metastore",
        "hbaseClusterName": "gbif-hbase",
        "componentConfig": "gridded-datasets",
        "driverCores": "2000m",
        "driverMemory": "2Gi",
        "executorInstances": 6,
        "executorCores": "6000m",
        "executorMemory": "8Gi",
    }

    GRSCICOLL_CACHE: object = {
        "sparkName": "grscicoll-cache",
        "args": ["config.properties"],
        "version": "1.33.2-H3-SNAPSHOT ",
        "component": "kvs-indexing",
        "main": "org.gbif.kvs.indexing.grscicoll.GrscicollPipelineWorkflow",
        "hdfsClusterName": "gbif-hdfs",
        "hiveClusterName": "gbif-hive-metastore",
        "hbaseClusterName": "gbif-hbase",
        "componentProperty": {
            "propertyName": "grscicoll-cache",
            "path": "/etc/gbif",
            "file": "config.properties",
        },
        "driverCores": "2000m",
        "driverMemory": "2Gi",
        "executorInstances": 6,
        "executorCores": "6000m",
        "executorMemory": "8Gi",
    }
