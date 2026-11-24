
# Integrate Airflow with Apache Spark

This guide tells how to connect to an Apache Spark system running on Windows and execute Spark jobs from Airflow DAGs. There are multiple ways that Airflow can connect to a remote Spark cluster installed on a different host.

## Table of Contents
- [1.1 Start Apache Spark](#11-start-apache-spark)
- [1.2 Install Airflow Spark Provider](#12-install-airflow-spark-provider)
- [1.3 Add Windows IP to Hosts File](#13-add-windows-ip-to-hosts-file)
- [1.4 Restart Airflow Services](#14-restart-airflow-services)
- [1.5 Connect Spark using SSHOperator](#15-connect-spark-using-sshoperator)
- [1.6 Connect Spark using SparkSubmitOperator](#16-connect-spark-using-sparksubmitoperator)
- [1.7 Connect Spark using Spark Connect](#17-connect-spark-using-spark-connect)

---

## 1.1 Start Apache Spark
Ensure Apache Spark is installed on the remote server (Windows). If you don’t have it installed already, follow [these steps](https://github.com/srimarrivada/BigData-Installation-on-Windows/blob/main/Install%20Apache%20Spark%203.5.1%20on%20Windows%2010%20and%20Integrate%20with%20Jupiter%20Notebook.md#3-install-spark-on-hadoop) to install Spark on top of existing Hadoop installation in local Windows operating system.

Verify the installation:

```cmd
pyspark --version
```

Start Spark services if not running:
* Start the **Spark Master** service in a new **Command Prompt** / **PowerShell**:
  ```cmd
  spark-class org.apache.spark.deploy.master.Master
  ```

  By default, the Spark Master UI tries to bind to port **8080**. If that port is busy, it tries the next port up (8081, 8082 and so on). When ports 8080 (occupied by Airflow in Ubuntu WSL2) and 8081 (occupied by Airflow in Docker) are busy, the Spark Master UI starts on port **8082**.

  Open the MasterWebUI at [http://localhost:8082](http://localhost:8082) (or whichever port it started on) and see the Spark Master is running at `spark://<ipaddress>:7077`.

* Start the **Spark Worker** service in a new **Command Prompt** / **PowerShell** (replace `your_spark_ip` with the IP from your Spark Master URL):
  ```cmd
  spark-class org.apache.spark.deploy.worker.Worker spark://your_spark_ip:7077
  ```

  By default, the Spark Worker UI tries port **8081** and moves up one port at a time if it's busy. Since 8081 and 8082 are occupied, the Worker UI started on port **8083**. Open [http://localhost:8083](http://localhost:8083) and see that the Spark Master service is running at URL `spark://your_spark_ip:7077`. 

## 1.2 Install Airflow Spark Provider
To interact with Apache Spark from Airflow, it is mandatory to install `apache-airflow-providers-apache-spark` package. 

Launch **Ubuntu** terminal and run:
```bash
source airflow-env/bin/activate
pip install apache-airflow-providers-apache-spark
```

## 1.3 Add Windows IP to Hosts File
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

## 1.4 Restart Airflow Services
After installing the provider package, restart the Airflow services for the changes to take effect.

* **Scheduler**: Press `Ctrl + C` in its terminal if running in the foreground.  
  If started in daemon mode (`-D`):
  ```bash
  kill $$(cat $$AIRFLOW_HOME/airflow-scheduler.pid)
  ```
* **API Server**: Press `Ctrl + C` if running in the foreground.  
  If started in daemon mode:
  ```bash
  kill $(pgrep "airflow api_ser")
  ```
* **DAG Processor**: press `Ctrl + C` if running in the foreground.  
  If started in daemon mode:
  ```bash
  kill $(pgrep "airflow dag")
  ```

Start each service again foreground in the separate Ubuntu terminals:
* **Scheduler:**
```bash
source airflow-env/bin/activate
airflow scheduler
```
* **API Server:**
```bash
source airflow-env/bin/activate
airflow api-server
```

* **DAG Processor:**
```bash
source airflow-env/bin/activate
airflow dag-processor
```

Once the Spark setup is completed, Airflow can connect to the remote Spark cluster in various ways.

## 1.5 Connect Spark using SSHOperator
The Airflow `SSHOperator` executes commands or scripts on a remote server via SSH within an Airflow DAG. This operator is part of the `apache-airflow-providers-ssh` package.

### 1.5.1 Install SSH Library
Install apache-airflow-providers-ssh package to get SSHOperator. 

Launch Ubuntu terminal and run :
```bash
source airflow-env/bin/activate
pip install apache-airflow-providers-ssh
```

### 1.5.2 Restart Airflow Services
Restart the Scheduler, API Server and DAG Processor by following the same steps as in [1.4 Restart Airflow Services](#14-restart-airflow-services).

### 1.5.3 Create Airflow Connection
The `SSHOperator` relies on an Airflow SSH connection that stores the remote server's hostname, port, username and authentication method. Refer [Airflow documentation](https://airflow.apache.org/docs/apache-airflow-providers-ssh/stable/connections/ssh.html) to know more about connection type

Go to Airflow UI at [http://localhost:8080](http://localhost:8080). Click **Admin** and choose **Connections**. Then select **Add Connection**, enter the following and click **Save**:

| Field | Value |
|---|---|
| Connection ID | `ssh_windows` |
| Connection Type | SSH |
| Description | *(optional)* SSH to Windows host |
| Host | `winhost` |
| Login | `<your Windows login ID>` |
| Password | `<your Windows login password>` |

Once connection is saved, it appears in **Admin** → **Connections** section.

> **Note:** The Airflow environment automatically resolves the `winhost` IP address from `/etc/hosts`.

### 1.5.4 Start SSH Server on Windows
The SSH server on the remote Windows host should be up and running _(install OpenSSH Server from the [official WinSCP documentation](https://winscp.net/eng/docs/guide_windows_openssh_server) if it isn't already installed)_. 

Open **Services** (`Windows + R` → `services.msc`), find **OpenSSH SSH Server** and make sure it is **Running**. If not, click **Start**.

Once the **OpenSSH SSH Server** service is **running**, the Windows host is ready to accept remote connections. 

### 1.5.5 Create Spark Application
Create a simple Spark application with some dummy data to be processed and output is saved to a file. This Spark application will be executed by the Airflow DAG later.

On the Windows system where Spark is running, create `D:\airflow_ssh_spark_app.py` file and add the below contents:

```python
# Import SparkSession
from pyspark.sql import SparkSession

# Define function to process data using Spark
def process_data():
    # Create SparkSession
    spark = SparkSession.builder.appName("AirflowSparkSubmitDemo").getOrCreate()
    print(f"Spark session created: {spark}")

    # Define data to be processed
    data = [
        (10,"Administration",200,1700),
        (20,"Marketing",201,1800),
        (30,"Purchasing",114,1700),
        (40,"Human Resources",203,2400),
        (50,"Executive",100,1700)
        ]

    # Create Spark DataFrame
    print("Reading data..")
    df=spark.createDataFrame(data, ["department_id", "department_name", "manager_id", "location_id"])
    print(f"Data read completed: {df.show()}")

    # Filter data
    print("Filtering data..")
    df_filtered=df.filter(df.location_id==1700)

    # Write data to file
    print(f"Writing filtered data to file..")
    output_file="/tmp/airflow_ssh_spark_output"
    df_filtered.write.mode("overwrite").csv(output_file)
    print(f"Data written to file: {output_file}.csv")

    # Stop SparkSession
    spark.stop()

# Call function
if __name__ == "__main__":
    process_data()
```

The above code performs the following:
* Creates a `SparkSession` object with applicaton name set to `AirflowSSHOperatorSparkDemo`.
* Creates a DataFrame with columns `department_id`, `department_name`, `manager_id` and `location_id` and stores the given data.
* Filters the DataFrame for `location_id=1700` value.
* Saves the filtered output to `/tmp/airflow_ssh_spark_output.csv` file.

> **Note:** When Apache Spark runs on top of an existing Hadoop cluster, files are read from and written to HDFS by default.

Copy the file from Windows to the Ubuntu home location:
```bash
cp /mnt/d/airflow_ssh_spark_app.py ~
ls -lrt /mnt/d/airflow_ssh_spark_app.py
```

### 1.5.6 Create DAG
Create a new DAG that utilizes `SSHOperator` to run the above PySpark application file. Refer to the [Airflow documentation](https://airflow.apache.org/docs/apache-airflow-providers-ssh/stable/_api/airflow/providers/ssh/operators/ssh/index.html#module-airflow.providers.ssh.operators.ssh) for full configuration of `SSHOperator`.

Create `$AIRFLOW_HOME/dags/run_ssh_spark_submit_dag.py` file and add the below contents:

```python
from airflow.sdk import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime

with DAG(
    dag_id="run_ssh_spark_submit",
    schedule=None,
    start_date=datetime(2025, 10, 28),
    catchup=False,
) as dag:
    spark_task=SSHOperator(
        task_id="ssh_spark_submit",
        ssh_conn_id="ssh_windows",
        command="%SPARK_HOME%\\bin\\spark-submit --master local[4] --deploy-mode client --conf spark.executor.memory=2g --conf spark.driver.memory=1g \
            --conf spark.executor.cores=2 --num-executors 1 --executor-cores 2 --executor-memory 2g --name AirflowSparkSSHDemo D:\\airflow_ssh_spark_app.py",
        cmd_timeout=3600
    )
```

The above code performs the following:
- Creates a DAG named `run_ssh_spark_submit` and sets its `start_date`.
- Creates a task named `ssh_spark_submit` with `SSHOperator`. It uses the `ssh_windows` connection to run `spark-submit` on the remote Windows server. `cmd_timeout` is set to 3600 seconds because `SSHOperator` and `SSHHook` time out after only 10 seconds by default, which is too short for Spark jobs.

### 1.5.7 Execute DAG
Go to Airflow UI at [http://localhost:8080](http://localhost:8080): 
* Search for `run_ssh_spark_submit`, click **Trigger**, select **Unpause run_ssh_spark_submit on trigger** and click **Trigger** to run the DAG manually.
* The `ssh_spark_submit` task starts running under **Task Instances** tab
* In few minutes, all tasks **state** changes to **Success**.
* To see the output of `ssh_spark_submit` task, click the task and open the latest Dag Run.
* In **Logs** tab, the task is running the Spark Submit command which creates Spark Session, reads data, processes it and writes to the output file `/tmp/airflow_ssh_spark_submit_output.csv` to HDFS (if Spark is running on top of Hadoop).

### 1.5.8 Validate Results
Open a new Command Prompt in Windows and verify files in HDFS:
```cmd
hadoop fs -ls /tmp/airflow_ssh_spark_submit_output
```

Spark creates partitioned output like `part-00000-*.csv`, `part-00002-*.csv`, `part-00003-*.csv`:
```cmd
hadoop fs -cat /tmp/airflow_ssh_spark_submit_output/part-00000*.csv
hadoop fs -cat /tmp/airflow_ssh_spark_submit_output/part-00002*.csv
hadoop fs -cat /tmp/airflow_ssh_spark_submit_output/part-00003*.csv
```

## 1.6 Connect Spark using SparkSubmitOperator
The `SparkSubmitOperator` is another way to run jobs on a remote Spark server. It requires a full Spark installation on the Airflow machine, because Airflow uses the local `spark-submit` binary to send the job to the remote Spark Master. It is part of the `apache-airflow-providers-apache-spark` package.

### 1.6.1 Install Spark Binaries
Install Spark and its prerequisite, Java, in the Airflow environment. The Spark and Java versions must match the remote Windows server.

#### Verify PySpark version in Windows
Open new Command Prompt and run:
```bash
pyspark --version
```
In this setup, Spark 3.5.1 and Java 1.8.0 are installed.

#### Install Java 1.8 in WSL
Download the latest Linux x64 JRE 8 archive (e.g. `jre-8u481-linux-x64.tar.gz`) from the [JRE 8 Downloads](https://www.java.com/en/download/manual.jsp) page _(it requires an Oracle account to download)_. 

Open a new Ubuntu terminal and run the following (replace `your_windows_user`) to copy the binary file from Windows **Downloads** folder to Ubuntu machine:

```bash
cd /mnt/c/Users/your_windows_user/Downloads/
ls -lrt jre-8u*-linux-x64.tar.gz
mkdir ~/java
cp jre-8u*-linux-x64.tar.gz ~/java
cd ~/java
ls -lrt jre-8u*-linux-x64.tar.gz
```

Extract the contents of binary file into the respective JRE folder such as `jre1.8.0_481`:
```bash
cd ~/java
tar -xzf jre-8u*-linux-x64.tar.gz
ls -lrt
```

#### Install Spark in WSL
Download `spark-3.5.1-bin-hadoop3.tgz` from the [Apache Spark Archive](https://archive.apache.org/dist/spark/spark-3.5.1/).

Then, copy the binary file `spark-3.5.1-bin-hadoop3.tgz` from Windows **Downloads** folder to Ubuntu machine:
```bash
cd /mnt/c/Users/your_windows_user/Downloads/
ls -lrt spark-3.5.1-bin-hadoop3.tgz
mkdir ~/spark
cp spark-3.5.1-bin-hadoop3.tgz ~/spark
cd ~/spark
ls -lrt spark-3.5.1-bin-hadoop3.tgz
```

Extract the contents of binary file into `spark-3.5.1-bin-hadoop3.tgz` folder:
```bash
cd ~/spark
tar -xzf spark-3.5.1-bin-hadoop3.tgz
ls -lrt
```

#### Configure Environment Variables in WSL
Apache Spark requires the `JAVA_HOME` and `SPARK_HOME` environment variables (use your installed JRE version):

```bash
echo "export JAVA_HOME=~/java/jre1.8.0_481" >> ~/.bashrc
echo "export SPARK_HOME=~/spark/spark-3.5.1-bin-hadoop3/" >> ~/.bashrc
source ~/.bashrc
```

#### Verify Spark Installation in WSL
Verify the installed Spark version after environment variables are set:
```bash
pyspark --version
```

### 1.6.2 Install Matching Python
The Python version in the Airflow environment must match the remote Windows server. 

Check it on Windows:
```bash
python --version
```

In this setup, it is Python 3.11.9. 

Install Python 3.11 on Ubuntu:
```bash
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt update
sudo apt install python3.11
```

### 1.6.3 Restart Airflow Services
Follow the same steps as in [1.4 Restart Airflow Services](#14-restart-airflow-services).

### 1.6.4 Create Airflow Connection
Next, configure a **Spark Submit** connection to allow Airflow to communicate with Apache Spark. The official [Airflow documentation](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/connections/spark-connect.html) provides further details on configuring this connection type. 

The `SparkSubmitOperator` uses this connection to get the remote server's hostname, port and credentials. 

Go to Airflow UI. Click on **Admin** → **Connections** → **Add Connection**. Add the following and save:

| Field | Value |
|---|---|
| Connection ID | `spark_default` |
| Connection Type | Spark |
| Description | *(optional)* Connection to Spark cluster |
| Host | `spark://your_spark_ip` |
| Port | `7077` |

> **Note:** The host should be the Spark Master IP address shown in the Spark MasterWebUI running at [http://localhost:8081](http://localhost:8081) or [http://localhost:8082](http://localhost:8082). **7077** is the default Spark Master port.

Once connection is saved, it appears in **Admin** → **Connections** section.

### 1.6.5 Create Spark Application
Next, create a simple Spark application with some dummy data to be processed and output is saved to a file in HDFS. This Spark application will be executed by the Airflow DAG later.

On Windows, create `D:\airflow_spark_submit_app.py` file and add the below contents to it _(replace `namenode_ip` with the Windows IP address where Hadoop is running)_:

```python
# Import SparkSession
from pyspark.sql import SparkSession

# Define function to process data using Spark
def process_data():
    # Create SparkSession
    spark = SparkSession.builder.appName("AirflowSparkSubmitDemo").getOrCreate()
    print(f"Spark session created: {spark}")

    # Define data to be processed
    data = [
        (10,"Administration",200,1700),
        (20,"Marketing",201,1800),
        (30,"Purchasing",114,1700),
        (40,"Human Resources",203,2400),
        (50,"Executive",100,1700)
        ]

    # Create Spark DataFrame
    print("Reading data..")
    df=spark.createDataFrame(data, ["department_id", "department_name", "manager_id", "location_id"])
    print(f"Data read completed: {df.show()}")

    # Filter data
    print("Filtering data..")
    df_filtered=df.filter(df.location_id==1700)

    # Write data to file
    print(f"Writing filtered data to file..")
    output_file="hdfs://namenode_ip:9820/tmp/airflow_spark_submit_output"
    df_filtered.write.mode("overwrite").csv(output_file)
    print(f"Data written to file: {output_file}.csv")

    # Stop SparkSession
    spark.stop()

# Call function
if __name__ == "__main__":
    process_data()
```

The above code performs the following:
*	Create a `SparkSession` object with applicaton name set to `AirflowSparkSubmitDemo`.
*	Create a DataFrame with columns `department_id`, `department_name`, `manager_id` and `location_id` and stores the given data.
*	Filter the DataFrame having data with `location_id=1700` value.
*	Save the filtered DataFrame output to HDFS location `hdfs://namenode_ip:9820/tmp/airflow_spark_submit_output`. 

> **Note:** Do not use `localhost` in place of `namenode_ip`. Here, `localhost` would refer to the Ubuntu machine, not Windows. Use the exact IP address of the Windows machine where the NameNode is running.
> 
> To know the IP address of Windows machine:
> ```cmd
> ipconfig | findstr /i "ipv4" | findstr /v 172.
> ```
> It displays IP addresses of your system connected using WiFi, Ehternet, etc. Use any of these IP addresses to update in the code.
>
> **Note:** The Airflow `SparkSubmitOperator` runs `spark-submit` on the Ubuntu instance, starting the Spark driver _(which creates the SparkContext, schedules tasks, and collects results)_. It then connects to the Spark Master on Windows, where the executors run the PySpark code.
Because the driver (Ubuntu) and executors (Windows) run on separate machines with different operating systems, writing output to a local path causes path conflicts:
> * **Using a Linux path** (e.g., `/tmp/spark_output.csv`) fails because the Windows executors cannot create the directory, throwing `Mkdirs failed to create file:/tmp/spark_submit_output/_temporary/0/`.
> * **Using a Windows path** (e.g., `D:/spark_output.csv`) fails because the executors write temporary files (`<output>/_temporary/0/task_.../part-xxxxx.csv`) on Windows, which the Ubuntu driver cannot access to rename into final output files (<output>/part-xxxxx.csv), throwing `FileNotFoundException: File file:/D:/spark_submit_output/_temporary/0 does not exist`.
> **Solution:** Write the output to a shared storage system accessible to both Ubuntu and Windows, such as HDFS.

Go to Ubuntu instance and run the following command to copy the `airflow_ spark_submit_app.py` file from Windows to home location in Ubuntu:
```bash
cp /mnt/d/airflow_spark_submit_app.py ~
cd ~
ls -lrt airflow_spark_submit_app.py
```

### 1.6.6 Configure Hadoop NameNode
To let Airflow on Ubuntu write to HDFS, the Hadoop NameNode on Windows must bind to its external IP rather than `localhost`. Check which address it is listening on:

```cmd
netstat -ano | findstr 9820 | findstr LISTENING
```

If it shows `127.0.0.1:9820` (i.e. HDFS NameNode RPC port 9820 is bound only to localhost), add the following to `%HADOOP_HOME%\etc\hadoop\hdfs-site.xml`:

```xml
<property>
  <name>dfs.namenode.rpc-bind-host</name>
  <value>0.0.0.0</value>
</property>
```

Restart HDFS in **Command Prompt** as **Administrator**:
```cmd
stop-dfs.cmd
start-dfs.cmd
```

Verify if Hadoop NameNode and DataNode services along with NodeManager and ResourceManager are running:
```cmd
jps
```

Verify Hadoop NameNode RPC port again:
```cmd
netstat -ano | findstr 9820 | findstr LISTENING
```
It should now show `0.0.0.0:9820`. 

Verify from Ubuntu if it can connect to HDFS:
```bash
hadoop fs -ls hdfs://winhost:9820/
```
It displays the list of folders available in the root location (`/`) indicating successful connection between Ubuntu and HDFS.

### 1.6.7 Create DAG
Create a new DAG that utilizes `SparkSubmitOperator` to run the above PySpark application file. Refer to the [Airflow documentation](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/_api/airflow/providers/apache/spark/operators/spark_submit/index.html#module-airflow.providers.apache.spark.operators.spark_submit) for complete configuration details of this operator.

Create `$AIRFLOW_HOME/dags/run_spark_submit_dag.py` and add the below contents (replace `D:/ProgramFiles/Python/Python311` with your Windows Python location):

```python
from airflow.sdk import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(dag_id="run_spark_submit",
         start_date=datetime(2025,11,6),
         schedule=None):
    spark_submit_task=SparkSubmitOperator(task_id="spark_submit",
        name="AirflowSparkSubmitDemo",
        application="airflow_spark_submit_app.py",
        conn_id="spark_default",
        conf={
            "spark.pyspark.python": "D:/ProgramFiles/Python/Python311/python.exe",  # Explicitly specify Python executable
            "spark.pyspark.driver.python": "/usr/bin/python3.11"
        },
        verbose=False)
```

The above code performs the following:
* Creates a DAG named `run_spark_submit` and sets its `start_date`.
* Creates a task named `spark_submit` with `SparkSubmitOperator`, using the `spark_default` connection with additional Spark configurations:
  * `spark.pyspark.python` sets the Python executable used by the Spark executors on the remote Spark server.
  * `spark.pyspark.driver.python` sets the Python executable used by the Spark driver running locally.

### 1.6.8 Execute DAG
Go to Airflow UI at [http://localhost:8080](http://localhost:8080): 
* Search for `run_spark_submit` under **Dags** tab. Click on it and click on **Trigger**, select **Unpause run_spark_submit on trigger** and click **Trigger** to run the DAG manually.
* The `spark_submit` task sets to **Queued** state.
* In few seconds, the task state changes from **Queued** to **Running** state.
* Click on `spark_submit` task ID which takes to **Logs** tab where you can see the Airflow has started preparing the `spark-submit` command and submitted the `AirflowSparkSubmitDemo` application to SparkContext manager.
* In few seconds, Spark Driver gets started and connects to Spark master running at `spark://windows_ip:7077` and starts reading data.
* Meanwhile, open Spark MasterWebUI at [http://localhost:8082](http://localhost:8082) (or [http://localhost:8081](http://localhost:8081) depending on which port MasterWebUI has started) where the `AirflowSparkSubmitDemo` application is running.
* Click on the displayed Application ID to see more details about Spark Worker and Executor.
* Click on the displayed Worker which goes to Spark Worker UI where `AirflowSparkSubmitDemo` job is running.
* Click on the `AirflowSparkSubmitDemo` job to see more information about Spark Jobs.
* Go back to Airflow UI and notice that it has completed reading data, filtering data and starts writing filtered data to file.
* In a couple of minutes, data is successfully written to HDFS location `hdfs://windows_ip:9820/tmp/airflow_spark_submit_output` and SparkUI has stopped.
 
### 1.6.9 Validate Data
Open Command Prompt and run to verify in HDFS:

```bash
hadoop fs -ls /tmp/airflow_spark_submit_output
hadoop fs -cat /tmp/airflow_spark_submit_output/part-00000*.csv
hadoop fs -cat /tmp/airflow_spark_submit_output/part-00002*.csv
hadoop fs -cat /tmp/airflow_spark_submit_output/part-00003*.csv
```

> **Note:** Spark creates partitioned output like `part-00000-*.csv`, `part-00002-*.csv`, and `part-00003-*.csv` in HDFS. 

## 1.7 Connect Spark using Spark Connect
From **Spark 3.4.0** version, the **Spark Connect** feature provides a decoupled client-server architecture. Airflow can use it to connect to a remote Spark cluster with either `SparkConnectHook` or the `@pyspark` decorator, without SSH or local Spark binaries. Unlike `SSHOperator` and `SparkSubmitOperator`, these let you embed the PySpark logic directly in the Airflow DAG.

The Airflow `SparkConnectHook` is included in the `apache-airflow-providers-apache-spark` package, so make sure this package is installed.

### 1.7.1 Verify Spark Version
Open **Command Prompt** / **Windows PowerShell** prompt and run :
```cmd
pyspark --version
```
Version 3.5.1 (above 3.4) supports Spark Connect.

### 1.7.2 Install Prerequisites
With Spark Connect, the Spark client in Airflow uses **Arrow serialization**, which needs libraries like `pyarrow` and `zstandard`. 

Install the PySpark Connect libraries, matching your Windows Spark version exactly:
```bash
source airflow-env/bin/activate
pip install "pyspark[connect]==3.5.1"
```

On **Python 3.12+**, Spark Connect may throw `ModuleNotFoundError: No module named 'distutils'`. The `distutils` module was removed from the standard library and now ships with `setuptools`. 

Install it: 
```bash
pip install --upgrade setuptools
```

### 1.7.3 Restart Airflow Services
Follow the same steps as in [1.4 Restart Airflow Services](#14-restart-airflow-services).

### 1.7.4 Start Spark Connect Server
On Windows, start the Spark Connect service:

```cmd
spark-submit --class org.apache.spark.sql.connect.service.SparkConnectServer --name "Spark Connect server" --packages org.apache.spark:spark-connect_2.12:3.5.1
```

Verify it is reachable from Ubuntu:
```bash
nc -vz winhost 15002
```

### 1.7.5 Create Airflow Connection
Create a **Spark Connect** connection to allow Airflow to communicate with Apache Spark via the Spark Connect interface. The official [Airflow documentation](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/connections/spark-connect.html) provides further details on configuring this connection type.

Go to Airflow UI. Click on **Admin** → **Connections** → **Add Connection**. Add the following and save:

| Field | Value |
|---|---|
| Connection ID | `spark_connect` |
| Connection Type | Spark Connect |
| Description | *(optional)* Connection to Spark Connect server |
| Host | `sc://winhost` |
| Port | `15002` |

> **Note:** 15002 is the default Spark Connect server port.

Once connection is saved, it appears in **Admin** → **Connections** section.

### 1.7.6 Create DAG
Create a new DAG that uses the `SparkConnectHook` (refer to [Airflow documentation](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/_api/airflow/providers/apache/spark/hooks/spark_connect/index.html) for the full configuration).

Create `$AIRFLOW_HOME/dags/run_spark_connect.py` and add the following contents:

```python
from airflow.decorators import dag, task
from airflow.providers.apache.spark.hooks.spark_connect import SparkConnectHook
from pyspark.sql import SparkSession
from datetime import datetime

@dag(dag_id="run_spark_connect", start_date=datetime(2025,11,1), schedule=None)
def spark_connect_dag():
    @task(task_id="spark_connect", retries=3)
    def run_spark_code_with_hook():
        hook=SparkConnectHook(conn_id="spark_connect")
        spark_connect_url=hook.get_connection_url()

        ###### Start PySpark logic #####

        # Create SparkSession
        spark = SparkSession.builder \
            .remote(spark_connect_url) \
                .appName("AirflowSparkDemo") \
                    .config("spark.sql.execution.pandas.inferPandasDictAsMap", "true") \
                        .config("spark.sql.pyspark.legacy.inferMapTypeFromFirstPair.enabled", "true")\
                            .getOrCreate()
        print(f"Spark session created: {spark}")

        # Define data to be processed
        data = [
            (10,"Administration",200,1700),
            (20,"Marketing",201,1800),
            (30,"Purchasing",114,1700),
            (40,"Human Resources",203,2400),
            (50,"Executive",100,1700)
            ]

        # Create Spark DataFrame
        print("Reading data..")
        df=spark.createDataFrame(data, ["department_id", "department_name", "manager_id","location_id"])
        print(f"Data read completed: {df.show()}")

        # Filter data
        print("Filtering data..")
        df_filtered=df.filter(df.location_id==1700)

        # Write data to file
        output_file="/tmp/airflow_spark_connect_output"
        print(f"Writing data to file..")
        df_filtered.write.mode("overwrite").csv(output_file)
        print(f"Data written to file: {output_file}.csv")

        # Stop SparkSession
        spark.stop()
        print("Spark session stopped")

        ###### End PySpark logic #####

    # Call Python function to create a task
    run_spark_code_with_hook()

# Call Python function to instantiate DAG
spark_connect_dag()
```

The above code performs the following:
* Creates a DAG named `run_spark_connect` with the `@dag` decorator and set its `start_date`.
* Creates a task named `spark_connect` with the `@task` decorator, which calls `SparkConnectHook` using the `spark_connect` connection.
* Creates a `SparkSession` object connected to the remote Spark Connect server with applicaton name set to `AirflowSparkConnectDemo`, and include two extra settings set to `true`:
  * `spark.sql.execution.pandas.inferPandasDictAsMap` handles data conversion between Pandas and Spark DataFrames when inferring schemas from Python dictionaries.
  * `spark.sql.pyspark.legacy.inferMapTypeFromFirstPair.enabled` handles a compatibility break in how Spark infers the schema for MapType columns
* Create a DataFrame with columns `department_id`, `department_name`, `manager_id` and `location_id` and stores the given data.
* Creates, filters (`location_id=1700`) and saves the DataFrame to `/tmp/airflow_spark_connect_output` location.

### 1.7.7 Execute DAG
Go to Airflow UI at [http://localhost:8080](http://localhost:8080):
* Search for `run_spark_connect` under **Dags** tab, click on it and click on **Trigger** button, select **Unpause run_spark_connect on trigger** and click **Trigger** to run the DAG manually.
* On **Task Instances** tab, it displays `spark_connect` task which gets completed in few minutes and **status** changes to **Success**.
* To see the output, click the task and open Logs tab to see the Spark Session created, the data read and output written to `/tmp/airflow_spark_connect_output.csv` file.

### 1.7.8 Validate Data
Verify the results in HDFS:
```bash
hadoop fs -ls /tmp/airflow_spark_connect_output
hadoop fs -cat /tmp/airflow_spark_connect_output/part-00000*.csv
hadoop fs -cat /tmp/airflow_spark_connect_output/part-00001*.csv
hadoop fs -cat /tmp/airflow_spark_connect_output/part-00002*.csv
```

> **Note:** Spark creates partitioned output like `part-00000-*.csv`, `part-00001-*.csv`, etc. in HDFS.