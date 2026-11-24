# Create New DAG

This guide creates a simple DAG file by writing Python code.

Open an Ubuntu terminal and run to create `$AIRFLOW_HOME/dags/print_hello_dag.py` file:

```bash
cd $AIRFLOW_HOME/dags
touch print_hello_dag.py
ls
```

> **Note:** Make sure the Airflow virtual environment is active before running any Airflow command:
> ```bash
> source airflow-env/bin/activate
> ```

## Table of Contents
- [1.1 Import Modules](#11-import-modules)
- [1.2 Create Default Arguments](#12-create-default-arguments)
- [1.3 Define DAG Object](#13-define-dag-object)
- [1.4 Add Tasks](#14-add-tasks)
- [1.5 Set Task Dependencies](#15-set-task-dependencies)
- [1.6 Complete DAG Code](#16-complete-dag-code)
- [1.7 Testing DAG](#17-testing-dag)

---

## 1.1 Import Modules
A DAG file is a regular Python script. 

First, import the DAG class, the operators used to build tasks, and the `datetime` helpers used for scheduling:

```python
# ===============================
# Import necessary modules
# ===============================
# Import DAG object to create and define Directed Acyclic Graphs
from airflow import DAG # In Airflow 2.x and earlier
#from airflow.sdk import DAG # From Airflow 3.x and later

# Import PythonOperator to execute a Python function as a task
from airflow.operators.python import PythonOperator # In Airflow 2.x and earlier
# from airflow.providers.standard.operators.python import PythonOperator # 
From Airflow 3.x and later

# Import BashOperator to execute a Bash command as a task
from airflow.operators.bash import BashOperator # In Airflow 2.x and earlier
# from airflow.providers.standard.operators.bash import BashOperator # From Airflow 3.x and later

# Import datetime module to define the DAG's schedule
from datetime import datetime, timedelta
```

> **Note:** From Airflow 3, `DAG` is imported from `airflow.sdk`, and core operators such as `BashOperator` and `PythonOperator` live in the `apache-airflow-providers-standard` package (installed with Airflow by default). Old imports like `airflow.operators.bash` are deprecated.

## 1.2 Create Default Arguments
Default arguments are a dictionary of parameters applied to every task in the DAG, unless a task overrides them:

```python
default_arguments = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
```

| Argument | Meaning |
|---|---|
| `owner` | Owner of the tasks, shown in the UI |
| `start_date` | Date from which the DAG can be scheduled |
| `retries` | Number of retries before a task is marked failed |
| `retry_delay` | Wait time between retries |

## 1.3 Define DAG Object
Instantiate the DAG with a unique `dag_id`, the default arguments and a schedule. 

There are three ways to define a DAG object in Python code:
* Use the standarard constructor `DAG()` to pass it to operators explicitly:
  ```python
  sample_dag = DAG(
    dag_id="sample_dag",
    description='A simple DAG',
    schedule="@daily",
    default_args=default_arguments,
    catchup=False,
    tags=['example', 'hello'],
  )
  ```
* Use `with` statement (context manager), which applies all attributes inside it to the DAG implicitly:
  ```python
  with DAG(
    dag_id="sample_dag",  
    description='A simple DAG',
    schedule="@daily",
    default_args=default_arguments,
    catchup=False,
    tags=['example', 'hello'],
  ):
  ```
* Use `@dag` decorator to turn a function into a DAG generator:
  ```
  @dag(
    dag_id="sample_dag",
    description='A simple DAG',
    schedule="@daily",
    default_args=default_arguments,
    catchup=False,
    tags=['example', 'hello'],
  )
  ```

In the above DAG decalaration, the following attributes are set:  
* `dag_id` is set to `sample_dag` which is the unique identifier for the DAG across all DAGs. Each DAG must have a unique `dag_id` value.
* `schedule` _(in Airflow 2.x versions, it was `schedule_interval`)_ is set to `@daily` to run DAG daily once at midnight 12 AM UTC.
* `default_args` is set to `default_arguments` variable which contains DAG metadata.
* `catchup` is set to `False` to start executing the task from the current task _(by default, `catchup` is `True` to start running the tasks for all past intervals up to the current interval)_.
* `tags` are set to `example` and `hello` that are used as labels for filtering DAGs in the UI.

## 1.4 Add Tasks
Each task is an instance of an operator, linked to the DAG through the `dag` parameter.

Depending on the type of DAG declaration, a task can be created in many ways:
* Pass the DAG object explicitly to operators:
  ```python
  def print_hello():
    print("Hello World!")
  
  bash_task1 = BashOperatior(
    task_id="print_date",
    dag=dag1,
    bash_command="echo {{ ds }}"
  )

  bash_task2 = BashOperatior(
    task_id="sleep_5_sec",
    dag=dag1,
    bash_command="sleep 5"
  )

  python_task = PythonOperator(
    task_id="print_hello",
    dag=dag1,
    python_callable=print_hello
  )
  ```
  > **Note:** `task_id` must be unique within a DAG.

* Create task inside the DAG context manager (without setting `dag` attribute explicitly)
  ```python
  with DAG(
    dag_id="sample_dag",
    schedule_interval="@daily",
    default_args=default_arguments,
    catchup=False
  ):
    bash_task1 = BashOperatior(
        task_id="print_date",
        bash_command="echo {{ ds }}"
    )

    bash_task2 = BashOperator(
        task_id="sleep_5_sec",
        bash_command="sleep 5"
    )

    def print_hello():
        print("Hello World!")

    python_task = PythonOperator(
        task_id="print_hello",
        python_callable=print_hello
    )
  ```

* Use `@task` decorator to turn a Python function into a task:
  ```python
  bash_task1 = BashOperatior(
    task_id="print_date",
    bash_command="echo {{ ds }}"
  )

  bash_task2 = BashOperator(
    task_id="sleep_5_sec",
    bash_command="sleep 5"
  )

  @task
  def print_hello():
    print("Hello World!")
  ```
  
In this example, three simple tasks are created:  
* `bash_task1` is created using `BashOperator` with `task_id` as `print_date` and the `bash_command` attribute is set to `echo {{ ds }}`. Here `{{ ds }}` is a Jinja templated variable that stores the DAG execution date and the `bash_command` prints this date
* `bash_task2` is created using `BashOperator` with `task_id` as `sleep_5_sec` and the `bash_command` attribute is set to `sleep 5` to sleep for 5 seconds.
* `python_task` is created using `PythonOperator` with task_id as `hello_world` and the `python_callable` attribute is set to call a Python function `print_hello`. This function is defined to print `Hello World!` message.

## 1.5 Set Task Dependencies
Dependencies define the order in which tasks run. 

Use the bitwise left (`>>`) or right (`<<`) operators _(or use `set_downstream()` or `set_upstream()` functions)_ to specify the downstream or upstream fashion respectively.

```python
bash_task1 >> bash_task2 >> python_task 
```

This is equivalent to:
```python
bash_task1.set_downstream(bash_task2)
python_task.set_upstream(bash_task2)
```
Here, dependencies are set such that `bash_task1` runs first then `bash_task2` runs followed by `python_task` executes.

## 1.6 Complete DAG Code
Edit `$AIRFLOW_HOME/dags/print_hello_dag.py` and add the complete code:

```python
# ===============================
# Import necessary modules
# ===============================
# Import DAG object to create and define Directed Acyclic Graphs
from airflow import DAG # In Airflow 2.x and earlier
#from airflow.sdk import DAG # From Airflow 3.x and later

# Import PythonOperator to execute a Python function as a task
from airflow.operators.python import PythonOperator # In Airflow 2.x and earlier
# from airflow.providers.standard.operators.python import PythonOperator # From Airflow 3.x and later

# Import BashOperator to execute a Bash command as a task
from airflow.operators.bash import BashOperator # In Airflow 2.x and earlier
# from airflow.providers.standard.operators.bash import BashOperator # From Airflow 3.x and later

# Import datetime module to define the DAG's schedule
from datetime import datetime, timedelta

default_arguments = {
    'owner': 'airflow',
    'start_date': datetime(2025,9,24),
    'retries': 3
}

def print_hello():
    print("Hello World!")

with DAG(
    dag_id="print_hello_dag",
    schedule="@daily",
    default_args=default_arguments,
    catchup=False
):
    bash_task1 = BashOperator(
        task_id="print_date",
        bash_command="echo {{ ds }}"
    )

    bash_task2 = BashOperator(
        task_id="sleep_5_sec",
        bash_command="sleep 5"
    )

    python_task = PythonOperator(
        task_id="print_hello",
        python_callable=print_hello
    )

# Set dependencies using bitshift operators
bash_task1 >> bash_task2 >> python_task 
# or set dependencies using upstream or downstream functions
# bash_task1.set_downstrea(bash_task2)
# python_task.set_upstream(bash_task2)
```

## 1.7 Testing DAG
To run the DAG:
* Go to Airflow UI at [http://localhost:8080](http://localhost:8080) and search for `print_hello_dag` under **Dags** tab and you will see the DAG available.
  > **Note:** If you do not see the DAG, make sure **Scheduler** and **Dag Processor** services are up and running in **Home** page.
* When you see the `print_hello_dag` available, click on it which opens up the DAG where you see the DAG is in **paused** state currently.
* Click on **Trigger** button on the top right side which opens a dialog box where select **Unpause print_hello_dag on trigger** check box and select **Trigger** button to run the DAG manually.
* On **Task Instances** tab, you can observe that it starts running `print_date` task.
* Once `print_date` task is completed, `sleep_5_sec` and `print_hello` tasks would get executed sequentially after which you can notice the state of all tasks are marked as **Success**.
* To see the output, click on `print_date` task and go to **Task Instances** tab and click on the latest Dag Run date.
* It navigates to **Logs** tab where you can see the task output in the log.