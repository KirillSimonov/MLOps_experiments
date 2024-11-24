
# MLOps experiments and tools 
This repo serves as small project ot practice and enhacne MLOps skills and experimenting with diffrent tools for that.
Driven by [course](https://stepik.org/course/181476/syllabus) from stepik I started with AirFlow, mlflow and jupyter pack being run inside the docker on my local PC.

## docker
There is docker-compose file to run mlflow, jupyter and airflow addons. Airflow itself is run from dockerfile. 

## jupyter
Jupyter was deployed to run python files and jupyter notebooks for testing.

## Airflow
In the folder /artefacts I have several python files that describe several [DAGs](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html) and ml experiments with logging to mlflow.
- example_dag.py - example DAG to check initialization and correct running of DAG under Airflow.
- read_and_use_s3.py - checking reading variables created in s3, connection to s3.
- check_file_existenca.py - I had problems with reading pickle file from s3 so I wrote it to check correct conneciton to s3.
- transfer_data.py - transferring data from each step to another as in DAG it is not straightforward as in python function.
- train_ml_model.py - cycle from downloading data from s3, pre-processing data, training ML model and saving file with obtained metrics to s3.

## mlflow

To use mlflow I used jupyter notebooks and mlflow web ui.
 - MLflow_check.ipynb - useful functions as creating experiments, runs inside of experiment, logging metrics for trainded models and etc.

- telecom_churn_classification.ipynb - experiments of predicting client churn for telecom company. Here I tested 4 classes models and different parameters for each and logged best in each class to web ui with metrics and all needed data for reproducibility.

Further I aim to practise other tools.
_________
# Running
1. To run this project you need to have installed [Docker](https://www.docker.com/) on your machine. Alternatively, you can use other sofware for containers (ex, Podman). Mostly dockerfile and docker-compose.yml works correctly, but it is not tested here yet.
2. You need to create s3 bucket. I use [Yandex Cloud](https://yandex.cloud/ru/) and create [static key](https://yandex.cloud/en/docs/iam/operations/sa/create-access-key) for object storage
3. Check and correctly fill in lines where s3 bucket static key credentials are needed in docker-compose.yml file.
4. Run ```docker compose -f docker-compose.yml up``` in cli in folder.

[!TIP]
If docker is deployed correctly you will have the following:
- http://localhost:8080/ - airflow UI with airflow-airflow credentials
- http://localhost:5050/ - mlflow UI
- http://localhost:8888/ - JupyterLab 

After successful deployment you will have /dags folder created in root. To use my py files and access them in Jupyter put neede py files to /dags folder.


# Credits
 - Authors of [MLOps course](https://stepik.org/course/181476/syllabus) on stepik platform

