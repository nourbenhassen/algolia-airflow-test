# Algolia - Data Engineer Assignment
***

This project consists in an Airflow DAG called *shopify_pipeline*  that fetches one or several files from a given url, transforms them given a set of business rules and ingests them in a dockerized PostgreSQL Database. In the example provided, the csv files fetched are the ones corresponding to the date range going from 2019-04-01 to 2019-04-07.


## Table of Contents
1. [Technologies](#technologies)
2. [Project Setup](#project-setup)
3. [Running data pipeline](#running-data-pipeline)
4. [Testing](#testing)
5. [Deleting created images, containers, volumes](#deleting)

## 1. Technologies
<a name="technologies"></a>

A list of technologies used within the project:
* [Docker](https://docs.docker.com/get-docker): Version 20.10.12
* [Docker-compose](https://docs.docker.com/compose/install): Version 1.29.2 

The Python libraries used are:
* Pandas: version 1.3.5 (within the Airflow image)
* Pytest: version 7.0.1
* Pathlib: version 1.0.1


### __2. PROJECT SETUP__
<a name="project-setup"></a>

Step 1: Please run the following command to build the images contained in docker-compose.yaml:
```
$ docker-compose --project-directory ./airflow build
```

Step 2: Execute this command to run the corresponding containers:

```
$ docker-compose --project-directory ./airflow up
```

Once the containers are running, you can go to  http://localhost:8080/ to interact with Airflow UI (login: airflow, password: airflow).

Please do these last steps on the UI to finalize the set up:
\
Step 3: Add a variable: 
 * Go to: Admin > Variables > + Add a new record 
 * Name the variable *shopify_pipeline_config* 
 * Assign it the following value
 ```
  {
    "url_pattern": "https://alg-data-public.s3.amazonaws.com/{}.csv",
    "start_date": "2019-04-01",
    "end_date": "2019-04-07",
    "db_table_name": "shopify_config"
  }
  ```

Step 4: Finally, add a connection to the PostgreSQL DB. \
Go to: Admin > Connections > + Add a new record \
Please add the following records in the corresponding cells:
* Connection Id -> postgres_default
* Connection Type -> Postgres 
* Host -> postgres_external 
* Login -> airflow 
* Password -> airflow 
* Port -> 5432

You're now ready to run the DAG.

 ### __3. RUNNING THE DATA PIPELINE__
 <a name="running-data-pipeline"></a>

#### __3.1 HOW TO CONFIGURE THE INPUT FILES TO BE FETCHED__
You can modify the requested data by updating the values of *start_date* and *end_date* in the variable *shopify_pipeline_config* variable. 

To query only one file and not a range of files (therefore for only one specific date), you can indicate the desired date in *start_date* and erase *end_date*.

You can also indicate the same date in *start_date* and *end_date* and the dag will as well only take into account the given date.

However: if you indicate only *end_date*, or neither *start_date* nor *start_date* then the dag will run taking into account today's date - 1 (yesterday), it means that only one file will be fetched, corresponding to the one ingested at 2 AM.

An assumption was made that the file generated at 2AM contains yesterday's date. 
To give an example, the file generated on 2022-02-26 at 2 AM, can be downloaded using the following url path: *alg-data-public.s3.amazonaws.com/2022-02-25.csv*

Once the dates are set, the DAG can be run.

#### __3.2 STRUCTURE OF THE DAG__

The first task creates a table called *shopify_config* in a dockerized PostgreSQL database.
The second task fetches the data (one or several csv files) from the given url, aggregates it (if more than one file), transfroms it and loads it into the *shopify_config* table.


### __4. TESTING__
<a name="testing"></a>
The functions used in the DAG were tested (tests can be found in *./tests/unit_tests*). 

The OSS Airflow project uses pytest, so the same was used in this project.

Before running the tests please install the required libs in your virtual environment by running the command:
```
$ pip install -r requirements.txt
```

Then copy the following command to run the unit tests:
```
$ PYTHONPATH=./airflow/dags pytest ./tests/unit_tests
```

 ### __5.DELETING IMAGES, CONTAINERS, VOLUMES THAT WERE CREATED__
 <a name="deleting"></a>

When you are done with the project and wish to stop the containers, and delete the created images & volumes please run:

```
$ docker-compose --project-directory ./airflow down --volumes --rmi all
```
