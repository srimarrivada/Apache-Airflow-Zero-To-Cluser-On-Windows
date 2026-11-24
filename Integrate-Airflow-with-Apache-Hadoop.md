# Integrate Airflow with Apache Hadoop

This guide tells how to connect to an Apache Hadoop system running on Windows and execute Hadoop commands in the latest Airflow 3 environment.  
There are multiple ways that Airflow can connect to a remote Hadoop cluster installed on a different host.

## Table of Contents
- [1.1 Verify Hadoop Version](#11-verify-hadoop-version)
- [1.2 Configure Hadoop DataNode](#12-configure-hadoop-datanode)
- [1.3 Start Hadoop Services](#13-start-hadoop-services)
- [1.4 Install Airflow HDFS Provider](#14-install-airflow-hdfs-provider)
- [1.5 Add Windows IP to Hosts File](#15-add-windows-ip-to-hosts-file)
- [1.6 Restart Airflow Services](#16-restart-airflow-services)
- [1.7 Connect Hadoop using BashOperator](#17-connect-hadoop-using-bashoperator)
- [1.8 Connect Hadoop using WebHDFSHook](#18-connect-hadoop-using-webhdfshook)

---

## 1.1 Verify Hadoop Version
First, install Apache Hadoop in the remote server (Windows). If you don’t have it installed already, follow [these steps](https://github.com/srimarrivada/BigData-Installation-on-Windows/blob/main/Install%20Apache%20Hadoop%203.3.6%20on%20Windows%2010.md) to install Hadoop in local Windows operating system.

Verify the installation:

```cmd
hadoop version
```

## 1.2 Configure Hadoop DataNode
To allow an Airflow instance running on Ubuntu to read or write data in HDFS through **WebHDFS** (HTTP-based REST API for Hadoop), the Hadoop DataNode hostname must be set to the Windows IP address. Otherwise, by default, Hadoop DataNode uses the Windows host name, which Ubuntu cannot resolve, leading to "Connection Refused" errors.

First, find the local IP address of the Windows machine:

```cmd
ipconfig | findstr /i "ipv4" | findstr /v 172.
```
It displays IP addresses of your system connected using WiFi, Ehternet, etc. 

Next, open `hdfs-site.xml` in `%HADOOP_HOME%\etc\hadoop` and add the following property _(replace `your_windows_ip` with one of the above IP addresses)_:

```xml
<property>
  <name>dfs.datanode.hostname</name>
  <value>your_windows_ip</value>
</property>
```

## 1.3 Start Hadoop Services
Open **Windows PowerShell** or **Command Prompt** as **Administrator** and run:

```cmd
start-all.cmd
```

If Hadoop services are already running, restart them:

```cmd
stop-all.cmd
start-all.cmd
```

> After executing the above command, it opens up four command prompt windows for hadoop `namenode`, hadoop `datanode`, yarn `resourcemanager` and yarn `nodemanager` services. Wait until `namenode` log displays `Quota initialization completed` message, `datanode` log displays `Successfully sent block report to namenode: localhost/127.0.0.1:9820` message, `resourcemanager` log displays `Transitioned to active state` message and `nodemanager` log dispalys `Registered with ResourceManager` message.
>
> Run the following command to verify if all four Hadoop services have started successfully:
> ```cmd
> jps
> ```
>
> Once the Hadoop services have started, the **Namenode UI** can be accessible at [http://localhost:9870](http://localhost:9870).
> 
> On the **Namenode UI**, go to **Datanodes** tab where you should see Datanode is accessible at [http://your_host_ip/9864](http://your_host_ip/9864).
> 
> Click on the **Http Address** shown above which navigates you to Datanode UI where you can see Datanode information.

## 1.4 Install Airflow HDFS Provider
To interact with Apache Hadoop from Airflow, it is mandatory to install the `apache-airflow-providers-apache-hdfs` package:

Launch **Ubuntu** terminal and run:
```bash
source airflow-env/bin/activate
pip install apache-airflow-providers-apache-hdfs
```

During the installation, it might throw the error `krb5-config: not found`. This is because the HDFS provider uses **Kerberos** for authentication but the underlying Kerberos development libraries are missing. To fix this, install them:

```bash
sudo apt-get update
sudo apt-get install -y libkrb5-dev
```

Then install the HDFS provider again:

```bash
pip install apache-airflow-providers-apache-hdfs
```

## 1.5 Add Windows IP to Hosts File
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

## 1.6 Restart Airflow Services
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

Once the HDFS setup is completed, Airflow can connect to the remote Hadoop cluster in various ways.

## 1.7 Connect Hadoop using BashOperator
The Airflow `BashOperator` executes Hadoop commands on the remote server over SSH.

### 1.7.1 Install OpenSSH Server on Windows
To establish a successful remote connection, an SSH server needs to be installed and active on the target Windows system. The [official WinSCP documentation](https://winscp.net/eng/docs/guide_windows_openssh_server) provides instructions for installing an SSH server on Windows via **OpenSSH**.

Open **Windows PowerShell** in **Administrator** mode and check if OpenSSH Server is already installed:

```powershell
Get-WindowsCapability -Online | Where-Object Name -like 'OpenSSH*'
```

If not installed, run:
```powershell
Add-WindowsCapability -Online -Name OpenSSH.Server~~~~0.0.1.0
```

Then open the **Services** window (`services.msc`), locate **OpenSSH SSH Server** and click **Start**. When the **OpenSSH SSH Server** service is in **Running** state, the Windows host is ready to accept remote connections. 

### 1.7.2 Verify SSH Connectivity
From the Ubuntu terminal, run this _(replace `your_username` with your Windows user ID)_ to make SSH connection to Windows host:
```bash
ssh your_username@winhost
```
To continue connection, enter `yes` and it asks for host password _(your Windows user password)_. When SSH is successfully connected, it displays Windows home path. Run `exit` to close the connection.

To automate the password entry, install `sshpass` Linux package:
```bash
sudo apt-get install sshpass
```

Make an SSH connection _(replace `your_username` and `your_password` with your Windows credentials)_:
```bash
sshpass -p your_password ssh your_username@winhost
```

Verify that Ubuntu WSL2 connects to HDFS on Windows _(replace `your_username` and `your_password` with your Windows credentials)_:
```bash
sshpass -p your_password ssh your_username@winhost hadoop fs -ls /
```
Once connected, it displays the list of folders available in the root (`/`) location in HDFS.

### 1.7.3 Create DAG
Create a new DAG that uses `BashOperator` to run Hadoop commands. 

Go to `$AIRFLOW_HOME/dags` and create a file named `run_hadoop_commands_dag.py`.

Verfiy it in Ubuntu:
```bash
cd $AIRFLOW_HOME/dags
ls -lrt run_hadoop_commands_dag.py
```

Enter the following DAG code in `run_hadoop_commands_dag.py` file _(replace `your_username` and `your_password` with your Windows credentials)_:

```python
# ===============================
# Import necessary modules
# ===============================
# Import DAG object to create and define Directed Acyclic Graphs
from airflow.sdk import DAG

# Import BashOperator to execute Hadoop command as a Bash task
from airflow.providers.standard.operators.bash import BashOperator

# Import datetime modules to define the DAG's schedule
from datetime import datetime, timedelta

default_arguments = {
    'owner': 'airflow',
    'start_date': datetime(2025,10,15),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate DAG
run_hadoop_commands = DAG(dag_id='run_hadoop_commands',
                          default_args=default_arguments,
                          schedule='@once',
                          catchup=False
                        )

# Define variables for ssh connectivity
user = 'your_username'
password = 'your_password'

ssh = f'sshpass -p {password} ssh {user}@winhost'
scp = f'sshpass -p {password} scp'

# Add task to create a directory in HDFS
create_hdfs_dir = BashOperator(
    task_id='create_hdfs_dir',
    bash_command=f'{ssh} hdfs dfs -mkdir -p /airflow/test',
    dag=run_hadoop_commands,
    run_as_user=user
)

# Add task to provide permissions to HDFS directory
assign_permissions_to_hdfs_dir = BashOperator(
    task_id='assign_permissions_to_hdfs_dir',
    bash_command=f'{ssh} hdfs dfs -chmod -R 777 /airflow/test',
    dag=run_hadoop_commands,
    run_as_user=user
)

# Add task to copy a local file into HDFS
copy_file_to_hdfs = BashOperator(
    task_id='copy_file_to_hdfs',
    bash_command=f'echo "Hello from Airflow" > /tmp/welcome.txt && {scp} /tmp/welcome.txt {user}@winhost:/users/{user} && {ssh} hdfs dfs -copyFromLocal /users/{user}/welcome.txt /airflow/test',
    dag=run_hadoop_commands,
    run_as_user=user
)

# Add task to list contents of HDFS directory
list_hdfs_dir = BashOperator(
    task_id='list_hdfs_dir',
    bash_command=f'{ssh} hdfs dfs -ls /airflow/test',
    dag=run_hadoop_commands,
    run_as_user=user
)

# Add task to remove HDFS directory created
remove_hdfs_dir = BashOperator(
    task_id='remove_hdfs_dir',
    bash_command=f'{ssh} hdfs dfs -rm -R /airflow/test',
    dag=run_hadoop_commands,
    run_as_user=user
)

# Define task dependencies
create_hdfs_dir >> assign_permissions_to_hdfs_dir >> copy_file_to_hdfs >> list_hdfs_dir >> remove_hdfs_dir
```

The above code involves:
* Importing necessary Airflow libraries such as `DAG` and `BashOperator` to instantiate a DAG and necessary Python libraries such as `datetime` to schedule the DAG.
* Defining default DAG specific arguments to apply common attributes to its underlying operators.
* Creating DAG object by assigning a specific name, configuring its schedule and setting default arguments.
* Defining various tasks such as `create_hdfs_dir`, `assign_permissions_to_hdfs_dir`, `copy_file_to_hdfs`, `list_hdfs_dir`, `remove_hdfs_dir` which execute specific Hadoop commands for creating a directory, providing permissions, copyting local file to HDFS, listing the contents of HDFS directory, removing the file from HDFS.
* Defining dependencies for tasks to run in a specific order. 

### 1.7.4 Execute DAG
Go to Airflow UI at [http://localhost:8080](http://localhost:8080):
* Search for `run_hadoop_commands` under the **Dags** tab, click the DAG, click **Trigger** on the top right side which opens a dialog box where select **Unpause run_hadoop_commands on trigger** and click **Trigger** to run the DAG manually.
* On the **Task Instances** tab, it displays 5 tasks where it starts running `create_hdfs_dir` task.
* In few minutes, all tasks should be completed and their **State** changes to **Success**.
* To see the output of `create_hdfs_dir` task, click the task and go to **Task Instances** tab where it displays Dag Runs.
* Click on the latest Dag Run date which navigates to **Logs** tab where you can see the task running Hadoop command `hdfs dfs -mkdir -p /airflow/test` to create the directory in HDFS.
* To see the output of `assign_permissions_to_hdfs_dir` task, click the task on the left side and go to **Task Instances** tab and click on the Dag Run date which navigates to **Logs** tab where you can see the task running Hadoop command `hdfs dfs -chmod -R 777 /airflow/test` to set the permissions of new HDFS directory.
* To see the output of `copy_file_to_hdfs` task, click the task on the left side and go to **Task Instances** tab and click on the Dag Run date which navigates to **Logs** tab where you can see the task running commands to create `welcome.txt` file in `/tmp` location in local Ubuntu instance and copy `/tmp/welcome.txt` file from Ubuntu to Windows host at `C:\Users\<username>` and then copy `welcome.txt` file from Windows host to HDFS `/airflow/test` directory.
* To see the output of `list_hdfs_dir` task, click the task on the left side and go to **Task Instances** tab and click on the Dag Run date which navigates to **Logs** tab where you can see the task running Hadoop command `hdfs dfs -ls /airflow/test` which displays the listing of `/airflow/test/welcome.txt` file.
* To see the output of `remove_hdfs_dir` task, click the task on the left side and go to **Task Instances** tab and click on the Dag Run date which navigates to **Logs** tab where you can see the task running Hadoop command `hdfs dfs -rm -r /airflow/test/welcome.txt` which deletes `welcome.txt` file from `/airflow/test` HDFS directory.

### 1.7.5 Validate Results
Open **Command Prompt** in Windows and run:
```bash
hadoop fs -ls /
hadoop fs -ls /airflow
```

## 1.8 Connect Hadoop using WebHDFSHook
Airflow provides a [WebHDFSHook](https://airflow.apache.org/docs/apache-airflow-providers-apache-hdfs/4.0.0/_api/airflow/providers/apache/hdfs/hooks/webhdfs/index.html#airflow.providers.apache.hdfs.hooks.webhdfs.WebHDFSHook), a Python class wrapper around the [hdfscli](https://hdfscli.readthedocs.io/en/latest/api.html#module-hdfs.client) library that allows interaction with HDFS via the [WebHDFS REST API](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html). 

**WebHDFS** is an HTTP-based REST API provided by Apache Hadoop to connect to HDFS from outside the cluster. The `WebHDFSHook` is part of the `apache-airflow-providers-apache-hdfs` package.

Verify the installed package version:
```bash
pip show apache-airflow-providers-apache-hdfs
```

### 1.8.1 Create Airflow Connection
Setup WebHDFSHook connection in Airflow to make a successful connection to HDFS. Refer [Airflow documentation](https://airflow.apache.org/docs/apache-airflow-providers-apache-hdfs/stable/connections.html#apache-hdfs-connection) to know more about this connection type.

Go to Airflow UI at [http://localhost:8080](http://localhost:8080), select **Admin** and choose **Connections**. Then select **Add Connection**, enter the following and click **Save**:

| Field | Value |
|---|---|
| Connection ID | `hadoop_webhdfs` |
| Connection Type | Apache WebHDFS |
| Description | *(optional)* Connection to Hadoop Distributed File System |
| Host | `winhost` |
| Port | `9870` |

> **Note:** 9870 is the default port of the **Hadoop NameNode** web UI, which also serves **WebHDFS**. The Airflow environment automatically resolves the `winhost` IP address from `/etc/hosts`.

Once the connection is saved, it appears in **Admin** → **Connections** section.

### 1.8.2 Set Inbound Traffic Rule
For WebHDFS connectivity to work from Ubuntu WSL2, Windows Firewall must allow traffic on port **9870**:

1. In the Windows task bar, search for **Windows Defender Firewall with Advanced Security** and open it.
2. In the left panel, select **Inbound Rules** and click **New Rule** in the **Actions** pane.
3. Choose Rule Type **Port**, then click **Next**.
4. Choose **TCP** and enter port **9870**, then click **Next**.
5. Select **Allow the connection**, then click **Next**.
6. Choose the appropriate profiles (Domain, Private or Public) _(best practice is to select Private or Public profile)_, then click **Next**.
7. Give a descriptive rule name such as **Hadoop NameNode Port**, then click **Finish**.

### 1.8.3 Create DAG
Create a new DAG that uses `PythonOperator` style `@task` functions. The `get_conn()` method of `WebHDFSHook` returns the `hdfscli` client object. This hook returns either `hdfs.InsecureClient` or `hdfs.ext.kerberos.KerberosClient`, depending on `core.security` set in the Airflow configuration. These clients allow operations like `makedirs`, `list`, `read`, `upload` and `download` on HDFS.

Create `$AIRFLOW_HOME/dags/run_hadoop_webhdfs_dag.py` file and add below content:

```python
# Import modules
from airflow.sdk import DAG, task
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from datetime import datetime

##### Define Python tasks using `@task` decorator with explicit task_id ####
# Define task to check file in HDFS
@task(task_id='webhdfs_check_path', retries=3)
def check_hdfs_file():
    # Call WebHDFSHook with defined connection
    webhdfs_hook=WebHDFSHook(webhdfs_conn_id='hadoop_webhdfs')
    hdfs_file='/airflow/test/sample.txt'

    # Check for HDFS path
    if(webhdfs_hook.check_for_path(hdfs_file)):
       print(f"Path '{hdfs_file}' exists in HDFS")
    else:
        print(f"Path '{hdfs_file}' does not exist in HDFS")

# Define task to upload file to HDFS
@task(task_id='webhdfs_upload_file', retries=3)
def upload_file_to_hdfs():
    # Call WebHDFSHook with defined connection
    webhdfs_hook=WebHDFSHook(webhdfs_conn_id='hadoop_webhdfs')
    client=webhdfs_hook.get_conn()

    # Set local and HDFS file paths
    local_file='/tmp/sample.txt'
    hdfs_file='/airflow/test/sample.txt'

    # Write something to a text file in local host
    with open(local_file,'w') as f:
        f.write("This is a test file for web HDFS")

    # Upload file from local to HDFS
    client.upload(hdfs_file,local_file,overwrite=True)
    print(f"File uploaded to HDFS location '{hdfs_file}'")

# Define task to get file content from HDFS
@task(task_id='webhdfs_get_file_content', retries=3)
def get_file_content():
    # Call WebHDFSHook with defined connection
    webhdfs_hook=WebHDFSHook(webhdfs_conn_id='hadoop_webhdfs')
    client=webhdfs_hook.get_conn()

    # Set HDFS file path
    hdfs_file='/airflow/test/sample.txt'

    # Get file content from HDFS path
    with client.read(hdfs_file, encoding='utf-8') as reader:
        file_content=reader.read()

    print(file_content)

# Define DAG with explicit dag_id
with DAG(
    dag_id='run_hadoop_webhdfs',
    start_date=datetime(2025,10,26),
    schedule='@once'
):
    # Call Python functions to create tasks and set dependencies
    check_hdfs_file() >> upload_file_to_hdfs() >> get_file_content()
```

The above code performs:
* Importing the necessary Airflow libraries: `DAG` and `task` to create the DAG and tasks, `WebHDFSHook` to connect to HDFS, and `datetime` to schedule the DAG.
* Defining tasks using the `@task` decorator with underlying Python functions:
  * `webhdfs_check_path` task creates a `WebHDFSHook` object and calls `check_for_path()` to verify the HDFS path.
  * `webhdfs_upload_file` task creates a `WebHDFSHook` object, gets a client using `get_conn()` and calls `upload()` to copy the file from the local path to HDFS.
  * `webhdfs_get_file_content` task creates a `WebHDFSHook` objects, gets a client using `get_conn()` and calls `read()` to read the HDFS file and print its content.
* Creating the DAG object with a name, schedule and default arguments.
* Setting dependencies so the tasks run in order.

### 1.8.4 Execute DAG
Go to Airflow UI at [http://localhost:8080](http://localhost:8080): 
* Search for `run_hadoop_webhdfs` under **Dags**, click **Trigger**, select **Unpause run_hadoop_webhdfs on trigger** and click **Trigger** to run the DAG manually.
* In the **Task Instances** tab, it displays 3 tasks, starting with `webhdfs_check_path` task.
* In few minutes, all tasks should be completed and their **State** changes to **Success**.
* To see the output of `webhdfs_check_path` task, click the task on the left side and go to **Task Instances** tab where it displays Dag Runs.
* Click on the latest Dag Run date which navigates to **Logs** tab where you can see the status of file.
* To see the output of `webhdfs_upload_file` task, click the task on the left side and go to **Task Instances** tab and click on the Dag Run date which navigates to **Logs** tab where you can see the file uploaded to HDFS location `/airflow/test/sample.txt`.
* To see the output of `webhdfs_get_file_content` task, click the task on the left side and go to **Task Instances** tab and click on the Dag Run date which navigates to **Logs** tab where you can see the task is reading file `/airflow/test/sample.txt` and display contents of file.

### 1.8.5 Validate Results
Open **Command Prompt** in Windows and run:
```cmd
hadoop fs -ls /airflow/test/sample.txt
hadoop fs -cat /airflow/test/sample.txt
```