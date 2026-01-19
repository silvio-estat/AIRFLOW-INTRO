from airflow.sdk import dag, task, Context
from typing import Dict, Any

#forma numero 1 - mais complicada
# @dag
# def xcom_dag():

#     @task
#     def t1(context: Context):
#         val = 42
#         context['ti'].xcom_push(key='my_key', value=val)
        

#     @task
#     def t2(context: Context):
#         val = context['ti'].xcom_pull(task_ids='t1', key='my_key')
#         print(val)

#     t1() >> t2()


# xcom_dag()

@dag
def xcom_dag():

    @task
    def t1() -> Dict[str, Any]:
        my_val = 42
        my_sentence = "Hello-World"
        return {
            "my_val":my_val,
            "my_sentence":my_sentence
        }
        

    @task
    def t2(data: Dict[str, Any]):
        print(data['my_val'])
        print(data['my_sentence'])

    val = t1()
    t2(val)


xcom_dag()