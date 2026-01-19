from airflow.sdk import dag, task, task_group


@dag
def group():
    
    @task
    def a():
        print("Task A Completed")
        return 42
    
    @task_group(
            default_args={"retries": 2}
            )
    def b_group(val: int):
        @task
        def b(my_val: int):
            print("Task B1 Completed")
            resultado = my_val + 42
            print(resultado)

        @task_group(
                default_args={"retries": 3}
        )
        def nested_group():
            @task
            def n1():
                print("Task N1 Completed")
            n1()

        b(val) >> nested_group()

    val = a()
    b_group(val)

group()

            