from airflow.sdk import dag, task


@dag
def branch_dag():

    @task
    def a():
        return 3

    @task.branch()
    def b(val: int):
        if val==1:
            return 'equal_1'
        else:
            return 'diferent_than_1'

    @task 
    def equal_1(val: int):
        print(f"Value is equal to {val}")
    
    @task
    def diferent_than_1(val: int):
        print(f"Value is different than 1: {val}")

    val = a()
    b(val) >> [equal_1(val), diferent_than_1(val)]

branch_dag()

