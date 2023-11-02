# Gbif-airflow-dags
This repsitory containing the DAGs and plugins used in Airflow installation in GBIF. We are relying on the images and operators provived by [Stackable](https://docs.stackable.tech).

## DAGs
The dags are synced into Airflow through the git-sync program installed in the Airflow image. The program is configured to fetch the head of a specfic branch. 

As an quick overview, the following list describes the environment and which branch they env should be fetching from:

| Envrionment   | Branch    |
| ------------- | --------- |
| Dev2          | develop   |

**NOTE:** To fully determine which branch an environment is currently syncing, consult the gbif-airflow values.yaml to check the latest branch used or the airflow object within the namespace.

## Plugins
The plugin folder is build into the official Stackable airflow image as an extension with the plugins located in the Airflow installation. The dockerfile, describing the extention, is located in the gbif-docker-images repository.