## First Airflow DAG: Data ingestion pipeline

1 - Prepare the database first `docker-compose up airflow-init`

This is going to created db/airflow.db sqlite database

2 - Launch Airflow `docker-compose up`

Wait for scheduler and webserver to get healthy, then go to `localhost:8081` 

```python
username: admin
password: airflow
```

Tutorial from: https://www.youtube.com/watch?v=28UI_Usxbqo
