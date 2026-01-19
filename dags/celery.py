from airflow.sdk import dag, task
from time import sleep


@dag
def celery_dag():
    
    @task
    def start_task():
        sleep(5)
        print("Start Task Completed")
    
    @task(
            queue="high_cpu"
    )
    def middle_task_a():
        sleep(5)
        print("Middle Task a Completed")

    @task(
            queue="high_cpu"
    )
    def middle_task_b():
        sleep(5)
        print("Middle Task b Completed")

    
    @task(
            queue="high_cpu"
    )
    def end_task():
        sleep(5)
        print("End Task Completed")
    
    start_task() >> [middle_task_a(), middle_task_b()] >> end_task()
    
celery_dag()