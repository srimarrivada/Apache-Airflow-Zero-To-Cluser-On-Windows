# Configure Multi-Node Celery Airflow

This guide tells how to configure Airflow worker nodes using **CeleryExecutor** with a **Redis** broker and a **MySQL** metadata database.

## Table of Contents
- [1.1 Install MySQL Server in Windows](#11-install-mysql-server-in-windows)
- [1.2 Create Airflow User in MySQL](#12-create-airflow-user-in-mysql)
- [1.3 Install Redis Server in Docker](#13-install-redis-server-in-docker)
- [1.4 Create Python Virtual Environment in WSL](#14-create-python-virtual-environment-in-wsl)
- [1.5 Setup Airflow Directory](#15-setup-airflow-directory)
- [1.6 Install Airflow](#16-install-airflow)
- [1.7 Install Airflow Providers](#17-install-airflow-providers)
- [1.8 Install Client Dependencies](#18-install-client-dependencies)
- [1.9 Add Windows IP to WSL Hosts File](#19-add-windows-ip-to-wsl-hosts-file)
- [1.10 Verify Client Connectivity](#110-verify-client-connectivity)
- [1.11 Configure Airflow](#111-configure-airflow)
- [1.12 Initialize Airflow Database](#112-initialize-airflow-database)
- [1.13 Create DAG Directory](#113-create-dag-directory)
- [1.14 Start Airflow Components](#114-start-airflow-components)
- [1.15 Access Airflow Web UI](#115-access-airflow-web-ui)
- [1.16 Create DAG](#116-create-dag)
- [1.17 Execute DAG](#117-execute-dag)
- [1.18 Validate Data](#118-validate-data)

---

## 1.1 Install MySQL Server in Windows
If you do not have MySQL Server software installed, refer [MySQL Installation On Windows10](https://github.com/srimarrivada/MySQL-Installation-on-Windows10) on how to install and use it. 

Verify the installed version:
```bash
mysql --version
```
In this setup, **MySQL Community Server 8.0.44** version is installed.

## 1.2 Create Airflow User in MySQL
Open **Command Prompt** / **Windows PowerShell** and connect as `root` (enter the password when prompted):
```cmd
mysql -u root -p
```

At the `mysql>` prompt, run:
```sql
CREATE DATABASE airflow_db;
CREATE USER 'airflow'@'%' IDENTIFIED BY 'airflow123';
GRANT ALL PRIVILEGES ON airflow_db.* TO 'airflow'@'%';
FLUSH PRIVILEGES;
EXIT;
```

> **Note:** `%` lets the `airflow` user connect from any host.

These queries create new `airflow_db` database and new `airflow user` and grant all privileges to `airflow_db` database for `airflow` user and exit MySQL:

## 1.3 Install Redis Server in Docker
If you do not have Docker software installed, then install it from the [official Docker website](https://docs.docker.com/desktop/setup/install/windows-install/). For detailed steps on how to install and use it, please follow [these](https://github.com/srimarrivada/Docker-Installation-on-Windows/blob/main/Install%20Docker%20on%20Windows%2010%20and%20Configure%20Swarm%20Cluster.md) steps.

Open **Docker Desktop** application which starts the Docker Engine at the backend. 

Open a new **Command Prompt** / **Windows Powershell** and run to start Redis in a container named `redis`, mapping container port 6379 to port 6380 on Windows:
```bash
docker run --name redis -p 6380:6379 redis:latest
```

If the `redis` image is not available locally, Docker pulls `redis:latest` from Docker Hub first. Once ready, it displays `Ready to accept connections tcp`.

Verify the Redis status at command line:
```bash
docker exec -it redis redis-cli ping
```
If Redis is active, it responds with `PONG`.

## 1.4 Create Python Virtual Environment in WSL
Create a separate virtual environment so this setup doesn't interfere with the existing Airflow environment:

Open a new Ubuntu terminal and run:
```bash
mkdir ~/airflow-multinode
cd ~/airflow-multinode/
python3 -m venv airflow-multinode-env
source ~/airflow-multinode/airflow-multinode-env/bin/activate
```

The prompt changes to show the prefix `(airflow-multinode-env)`.

## 1.5 Setup Airflow Directory
By default, Airflow installs in the WSL home directory `/home/<username>`. To access Airflow data from both Windows and WSL, create a dedicated directory on a Windows drive (replace `d/ProgramFiles` with your own path):

```bash
cd /mnt/d/ProgramFiles
mkdir AirflowCelery
sudo chown $$USER:$$USER AirflowCelery
cd AirflowCelery
pwd
```

This path will be set in the `AIRFLOW_HOME` environment variable.

## 1.6 Install Airflow
Install **Airflow 3.1.3** with **Python 3.12**:

```bash
export AIRFLOW_HOME=/mnt/d/ProgramFiles/AirflowCelery
echo $AIRFLOW_HOME
AIRFLOW_VERSION=3.1.3
PYTHON_VERSION=3.12
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-$${AIRFLOW_VERSION}/constraints-$${PYTHON_VERSION}.txt"

pip install "apache-airflow==$${AIRFLOW_VERSION}" --constraint "$${CONSTRAINT_URL}"
```

Verify the installation:
```bash
export AIRFLOW_HOME=/mnt/d/ProgramFiles/AirflowCelery
airflow version
```

## 1.7 Install Airflow Providers
**CeleryExecutor** with **Redis** and **MySQL** needs these providers:
- `apache-airflow-providers-celery`
- `apache-airflow-providers-redis`
- `apache-airflow-providers-mysql`

**Install Celery provider (version 3.13.1):**
```bash
export AIRFLOW_HOME=/mnt/d/ProgramFiles/AirflowCelery
pip install apache-airflow-providers-celery==3.13.1
```
> **Note:** Celery provider versions above 3.13.1 cause Celery workers to reserve tasks but not execute them when workers are started with a custom hostname.

**Install Redis provider:**
```bash
export AIRFLOW_HOME=/mnt/d/ProgramFiles/AirflowCelery
pip install apache-airflow-providers-redis
```

**Install MySQL provider:**
```bash
export AIRFLOW_HOME=/mnt/d/ProgramFiles/AirflowCelery
pip install apache-airflow-providers-mysql
```

> **Note:** If it throws `pkg-config: not found`, it's because the `mysqlclient` library (a dependency of the MySQL provider) needs build tools to compile. Install them:
>
> ```bash
> sudo apt install -y pkg-config default-libmysqlclient-dev build-essential
> pip install apache-airflow-providers-mysql
> ```
>
> Then, install `apache-airflow-providers-mysql` provider:
>
> ```bash
> pip install apache-airflow-providers-mysql
> ```

## 1.8 Install Client Dependencies
Install client libraries for MySQL and Redis to connect to server:
```bash
sudo apt install -y mysql-client
sudo apt install -y redis-tools
```

## 1.9 Add Windows IP to WSL Hosts File
To connect to the Windows host from the Ubuntu WSL2 terminal, identify the Windows host IP address. Since WSL2 runs in a lightweight virtual machine, it has its own IP address separate from the Windows host.

Open new Ubuntu WSL2 terminal and run:
```bash
cat /etc/resolv.conf | grep nameserver
# or
ip route show | grep -i default
```

Add it to the Ubuntu `hosts` file as `winhost` so the Windows host can be resolved by hostname:

```bash
sudo sh -c "echo "$(cat /etc/resolv.conf | grep nameserver | awk '{print \$2}')" winhost >> /etc/hosts"
```

Verify (press `Ctrl + C` to stop):
```bash
ping winhost
```

> **Note:** The IP address added to the Ubuntu `/etc/hosts` file may be removed after the Ubuntu reboot. Add it again whenever WSL2 is restarted.

## 1.10 Verify Client Connectivity
MySQL (should respond `mysqld is alive`):
```bash
mysqladmin ping -h winhost -u airflow -pairflow123
```

Redis (should respond `PONG`):
```bash
redis-cli -h winhost -p 6380 ping
```

## 1.11 Configure Airflow
Initialize Airflow with the default configuration:
```bash
export AIRFLOW_HOME=/mnt/d/ProgramFiles/AirflowCelery
airflow db migrate
cd $AIRFLOW_HOME
ls
```

This creates `airflow.cfg` and `airflow.db` in the `$AIRFLOW_HOME` directory. 

Next, edit `$AIRFLOW_HOME/airflow.cfg` and modify the following parameters:

```ini
executor = CeleryExecutor
sql_alchemy_conn = mysql+mysqldb://airflow:airflow123@winhost:3306/airflow_db
broker_url = redis://winhost:6380/0
result_backend = redis://winhost:6380/0
base_url = http://localhost:8085
```

The above settings:
* Change executor to `CeleryExecutor`.
* Update `sql_alchemy_conn` connection string to point to `airflow_db` in MySQL running in local Windows system.
* Update `broker_url` and `result_backend` connection strings to point to Redis running in Docker container.
* Update `base_url` propery which is needed when the Celery Workers need to know their home API. It points to http://localhost:8085, where this Airflow instance's API server will run.

## 1.12 Initialize Airflow Database
Next, initialize the MySQL as the metadata database with necessary tables needed for Airflow functionality.
```bash
export AIRFLOW_HOME=/mnt/d/ProgramFiles/AirflowCelery
airflow db migrate
```

This creates the Airflow metadata tables in the MySQL `airflow_db` database.

From Airflow 3, the default `SimpleAuthManager` is used for authentication. Users are defined by `simple_auth_manager_users` (default `admin:admin`), and the `admin` password is generated when the API server first starts.

```bash
cat $AIRFLOW_HOME/airflow.cfg | grep simple_auth_manager
```

> **Note:** With SimpleAuthManager, Airflow 3 does not allow `airflow users create`. To manage users, install `apache-airflow-providers-fab` and set:
> ```ini
> auth_manager = airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager
> ```

## 1.13 Create DAG Directory
By default, Airflow scans for DAG files in `dags` folder available in `AIRFLOW_HOME` directory. 

Create it:
```bash
cd $AIRFLOW_HOME
mkdir dags
```
> **Note:** The default DAG location can be changed by updating `dags_folder` configuration in `airflow.cfg` file.

## 1.14 Start Airflow Components
Start Airflow Components including Scheduler, API Server and DAG Processor to bring the Airflow system into a usable state.

### 1.14.1 Start API Server
Start it in the new Ubuntu terminal _(use custom port **8085**, since **8080** is used by the single-node instance)_:
```bash
source ~/airflow-multinode/airflow-multinode-env/bin/activate
export AIRFLOW_HOME=/mnt/d/ProgramFiles/AirflowCelery
airflow api-server -p 8085
```
The admin password is saved to `simple_auth_manager_passwords.json.generated` in `$AIRFLOW_HOME`. Keep this terminal open.

### 1.14.2 Start Scheduler
Start it in the new Ubuntu terminal and keep it open:
```bash
source ~/airflow-multinode/airflow-multinode-env/bin/activate
export AIRFLOW__LOGGING__WORKER_LOG_SERVER_PORT=8799
export AIRFLOW_HOME=/mnt/d/ProgramFiles/AirflowCelery
airflow scheduler
```
The output confirms that the scheduler is using `CeleryExecutor`.

### 1.14.3 Start DAG Processor
Start it in the new Ubuntu terminal and keep it open:
```bash
source ~/airflow-multinode/airflow-multinode-env/bin/activate
export AIRFLOW_HOME=/mnt/d/ProgramFiles/AirflowCelery
airflow dag-processor
```

### 1.14.4 Start Celery Workers
By default, a Celery worker starts its log server on port **8793**, which is already used by the single-node instance. Give each worker a different port.

> **Note:** Avoid port 8794, which is the default port for the **Airflow Triggerer**.

* Start **Worker 1** (`celery-worker1`) on port 8795 assigned to `light` queue with a concurrency of `4`:
  ```bash
  source ~/airflow-multinode/airflow-multinode-env/bin/activate
  export AIRFLOW_HOME=/mnt/d/ProgramFiles/AirflowCelery
  export AIRFLOW__LOGGING__WORKER_LOG_SERVER_PORT=8795
  airflow celery worker --celery-hostname celery-worker1@%h -q light --concurrency 4
  ```
  The output confirms that the log server started on port 8795 and connected to `redis://winhost:6380/0` backend which makes `celery-worker1` ready.
  
* Start **Worker 2** (`celery-worker2`) on port 8796 assigned to `medium` queue with a concurrency of `4`:
  ```bash
  source ~/airflow-multinode/airflow-multinode-env/bin/activate
  export AIRFLOW_HOME=/mnt/d/ProgramFiles/AirflowCelery
  export AIRFLOW__LOGGING__WORKER_LOG_SERVER_PORT=8796
  airflow celery worker --celery-hostname celery-worker2@%h -q medium --concurrency 4
  ```
  The output confirms that the log server started on port 8796 and connected to `redis://winhost:6380/0` backend which makes `celery-worker2` ready.
  
* Start **Worker 3** (`celery-worker3`) on port 8797 assigned to `heavy` queue with a concurrency of `4`:
  ```bash
  source ~/airflow-multinode/airflow-multinode-env/bin/activate
  export AIRFLOW_HOME=/mnt/d/ProgramFiles/AirflowCelery
  export AIRFLOW__LOGGING__WORKER_LOG_SERVER_PORT=8797
  airflow celery worker --celery-hostname celery-worker3@%h -q heavy --concurrency 4
  ```
  The output confirms that the log server started on port 8797 and connected to `redis://winhost:6380/0` backend which makes `celery-worker3` ready.
  
Verify the status of all 3 Celery workers:
```bash
source ~/airflow-multinode/airflow-multinode-env/bin/activate
export AIRFLOW_HOME=/mnt/d/ProgramFiles/AirflowCelery
airflow celery list-workers
```

### 1.14.5 Start Celery Flower
Open new terminal and start flower _(use custom port **5556**, since **5555** is used by Flower in Docker)_:
```bash
source ~/airflow-multinode/airflow-multinode-env/bin/activate
export AIRFLOW_HOME=/mnt/d/ProgramFiles/AirflowCelery
airflow celery flower -p 5556
```
Open [http://localhost:5556](http://localhost:5556) to see the status of the Celery workers.

## 1.15 Access Airflow Web UI
Open [http://localhost:8085](http://localhost:8085) and log in as `admin` (the password is in `simple_auth_manager_passwords.json.generated`). The home page should show MetaDatabase, Scheduler and Dag Processor in green.

In **Dags** tab, it displays the list of the example DAGs which are shipped with the Airflow installation.

> **Note:** The example DAGs will fail to run, because the Celery workers only execute tasks assigned to their specific queues.

## 1.16 Create DAG
This DAG runs an ETL pipeline with `extract`, `clean`, `aggregate`, `transform` and `load` into MySQL. Each task is assigned to a specific Celery queue.

**Prerequisites:**

1. Install **pandas 2.1.4** and **NumPy 1.26.4**. Newer pandas versions need [SQLAlchemy 2.0+](https://pandas.pydata.org/docs/getting_started/install.html#sql-databases), but Airflow installs **SQLAlchemy 1.4**. Pandas 2.1 also needs [NumPy below 2.0](https://pandas.pydata.org/pandas-docs/version/2.1/getting_started/install.html#sql-databases).

   Install `pandas 2.1.4` and `numpy 1.26.4` libraries:
   ```bash
   source ~/airflow-multinode/airflow-multinode-env/bin/activate
   pip install --no-cache-dir "numpy==1.26.4" "pandas==2.1.4"
   ```
   > Note: Installing **Pandas 2.1** alone will install latest **NumPy version (2.4+)** and fails due to compatibility issue. So, install **NumPy <2.0** version separately along with **Pandas 2.1**.
   
2. Create a MySQL user for the processed data.
   Open **Command Prompt** / **Windows PowerShell** and run:
   ```cmd
   mysql -u root -p
   ```

   Run queries at `mysql>` prompt to create new `airflow_test_db` database and new `airflow_test` user and grant privileges and exit MySQL:
   ```sql
   CREATE DATABASE airflow_test_db;
   CREATE USER 'airflow_test'@'%' IDENTIFIED BY 'airflow_test123';
   GRANT ALL PRIVILEGES ON airflow_test_db.* TO 'airflow_test'@'%';
   FLUSH PRIVILEGES;
   EXIT
   ```
   > **Note:** % in the above query enables `airflow_test` user to accept connections from any host.

3. Create an Airflow connection at [http://localhost:8085](http://localhost:8085). Click on **Admin** → **Connections** → **Add Connection**:

   | Field | Value |
   |---|---|
   | Connection ID | `mysql_conn` |
   | Connection Type | MySQL |
   | Description | *(optional)* Connection to MySQL running on local Windows system |
   | Host | `winhost` |
   | Login | `airflow_test` |
   | Password | `airflow_test123` |
   | Port | `3306` |
   | Schema | `airflow_test_db` |

   This connects to the above MySQL `airflow_test` database.

   > **Note:** Airflow environment automatically resolves `winhost` IP address from the `/etc/hosts` file.


After prerequisities are done, create `$AIRFLOW_HOME/dags/etl_bulk_load.py` and add the below contents:

```python
from airflow.sdk import DAG, task
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
import datetime
import time
import pandas as pd
import random

def extract_data():
    print("Extracting raw data...")
    time.sleep(2)

    # Create a DataFrame with random data up to 5000 records
    data={
          "id": range(1,5001),
          "value": [random.randint(1,1000) for _ in range(5000)],
          "category": [random.choice(["A","B","C"]) for _ in range(5000)]
         }
    df=pd.DataFrame(data)

    # Save DataFrame to a CSV file (`index=False` prevents writing DataFrame's index to a file)
    df.to_csv("/tmp/etl_bulk_load_raw.csv", index=False)
    print("Raw data saved to file: /tmp/etl_bulk_load_raw.csv")

def clean_data():
    print("Reading raw data...")
    df=pd.read_csv("/tmp/etl_bulk_load_raw.csv")

    print("Cleaning data...")
    df.dropna()
    df["value"] = df["value"].astype(int)
    df.to_csv("/tmp/etl_bulk_load_clean.csv", index=False)
    print("Clean data saved to file: /tmp/etl_bulk_load_clean.csv")

def aggregate_data():
    print("Reading clean data...")
    df=pd.read_csv("/tmp/etl_bulk_load_clean.csv")
    print("Aggregating data...")

    # Group by category and sum (`reset_index` resets index values)
    agg_df=df.groupby("category")["value"].sum().reset_index()
    agg_df.to_csv("/tmp/etl_bulk_load_aggregate.csv")
    print("Aggregated data saved to file: /tmp/etl_bulk_load_aggregated.csv")

def calculate_data():
    print("Reading clean data...")
    df=pd.read_csv("/tmp/etl_bulk_load_clean.csv")

    print("Calculating data...")
    df["value_double"] = df["value"] * 2
    df["value_sqrt"] = df["value"] ** 0.5
    df["value_cube"] = df["value"] ** 3

    df.to_csv("/tmp/etl_bulk_load_calculated.csv", index=False)
    print("Calculated data saved to file: /tmp/etl_bulk_load_calculated.csv")

def bulk_load_data():
    print("Reading calculated data...")
    df=pd.read_csv("/tmp/etl_bulk_load_calculated.csv")

    # Create MySqlHook by calling `mysql_conn` connection
    mysql_hook=MySqlHook(mysql_conn_id="mysql_conn")
    conn=mysql_hook.get_sqlalchemy_engine()

    # Bulk load DataFrame to a SQL table in batches of 1000 records at a time
    df.to_sql(name="etl_bulk_load", con=conn, if_exists="replace", method="multi", index=False, chunksize=1000)
    print("Data bulk loaded to MySQL table: etl_bulk_load")

dag_args = {
    "owner": 'airflow',
    "retries": 3,
    "retry_delay": datetime.timedelta(seconds=10)
}

with DAG(dag_id="etl_bulk_load_with_queues",
         description="ETL pipeline with Celery queues and bulk load into MySQL",
         start_date=datetime.datetime(2026,3,17),
         schedule=None,
         catchup=False,
         default_args=dag_args
    ):
        extract_task=PythonOperator(task_id="extract", python_callable=extract_data, queue="light")
        clean_task=PythonOperator(task_id="clean", python_callable=clean_data, queue="medium")
        aggregate_task=PythonOperator(task_id="aggregate", python_callable=aggregate_data, queue="medium")
        calculate_task=PythonOperator(task_id="calculate", python_callable=calculate_data, queue="heavy")
        bulk_load_task=PythonOperator(task_id="bulk_load", python_callable=bulk_load_data, queue="light")

        extract_task >> clean_task >> aggregate_task >> calculate_task >> bulk_load_task
```

The above code performs the following:
- **`extract_data()`** generates 5000 rows of random data (`id`, `value`, `category`) and saves them to `/tmp/etl_bulk_load_raw.csv`.
- **`clean_data()`** drops nulls, casts `value` to integer and saves the result to `/tmp/etl_bulk_load_clean.csv`.
- **`aggregate_data()`** groups by `category`, sums `value` and saves the result to the aggregated CSV.
- **`calculate_data()`** adds the `value_double`, `value_sqrt` and `value_cube` columns and saves the result to `/tmp/etl_bulk_load_calculated.csv`.
- **`bulk_load_data()`** batch-loads the data (1000 rows at a time) into the MySQL table `etl_bulk_load` using MySqlHook.

| Task | Queue |
|---|---|
| extract | light |
| clean | medium |
| aggregate | medium |
| calculate | heavy |
| bulk_load | light |

## 1.17 Execute DAG
Go to Airflow UI at [http://localhost:8085](http://localhost:8085), search for `etl_bulk_load_with_queues` DAG, click **Trigger**, select **Unpause etl_bulk_load_with_queues on trigger** and click **Trigger**. The tasks run one after another and change to **Success** within a few seconds. Click each task → **Task Instances** → latest Dag Run → **Logs** to see its output.

Go to Airflow UI at [http://localhost:8085](http://localhost:8085):
* Search for `etl_bulk_load_with_queues` under **Dags** tab. Click on it and clcik **Trigger**, select **Unpause etl_bulk_load_with_queues on trigger** and select **Trigger** to run the DAG manually.
* On **Task Instances** tab, it displays `extract`, `clean`, `aggregate`, `heavy_transform` and `bulk_load` tasks and it starts executing `extract` task.
* The tasks run one after another and change all tasks state to **Success** within a few seconds
* To see the output of `extract` task, click the task on the left side and go to **Task Instances** tab which displays all Dag Runs.
* Click on the latest Dag Run date which navigates to **Logs** tab where it displays that the task is extracting raw data which is saved to `/tmp/etl_bulk_load_raw.csv` location.
* On the left pane, click on `clean` task and go to **Task Instances** tab. Then, click on the latest Dag Run date which navigates to **Logs** tab where it displays that the task is cleaning raw data and cleaned data is saved `/tmp/etl_bulk_load_clean.csv` location.
* On the left pane, click on `aggregate` task and go to **Task Instances** tab. Then, click on the latest Dag Run date which navigates to **Logs** tab where it displays that the task is reading cleaned data, aggregating by category and aggregated data is saved `/tmp/etl_bulk_load_aggregated.csv` location.
* On the left pane, click on `calculate` task and go to **Task Instances** tab. Then, click on the latest Dag Run date which navigates to **Logs** tab where it displays that the task is reading cleaned data, aggregating by category and aggregated data is saved `/tmp/etl_bulk_load_processed.csv` location.
* Then, click on `bulk_load` task and go to **Task Instances** tab. Then, click on the latest Dag Run date which navigates to **Logs** tab where it displays that the task is reading processed data and data loaded to MySQL table `etl_bulk_load`. 

## 1.18 Validate Data
Check the files in `/tmp`:
```bash
cd /tmp
ls -lrt etl*
```

Check the data in MySQL:
```bash
mysql -h winhost -u airflow_test -pairflow_test123
```

Run queries at `mysql>` prompt:
```sql
use airflow_test_db;
show tables;
select count(*) from etl_bulk_load;
select * from etl_bulk_load limit 15;
exit
```

The `etl_bulk_load` table should contain 5000 records processed by the Airflow DAG.