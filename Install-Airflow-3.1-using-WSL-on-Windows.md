# Install Airflow 3.1 using WSL on Windows

> This guide installs Apache Airflow 3.1 on Windows using WSL2 (Ubuntu). Airflow installation using WSL2 primarily requires Windows Subsystem for Linux2 (WSL2) software which provides a Linux-compatible environment that runs natively on Windows, allowing to use Linux command-line tools and utilities on a Windows machine. 

## Table of Contents
- [1.1 Install WSL2](#11-install-wsl2)
- [1.2 Update And Upgrade Ubuntu](#12-update-and-upgrade-ubuntu)
- [1.3 Install Python Libraries](#13-install-python-libraries)
- [1.4 Create Python Virtual Environment](#14-create-python-virtual-environment)
- [1.5 Setup Airflow Directory](#15-setup-airflow-directory)
- [1.6 Install Apache Airflow](#16-install-apache-airflow)
- [1.7 Install Dependent Libraries](#17-install-dependent-libraries)
- [1.8 Initialize Airflow Database](#18-initialize-airflow-database)
- [1.9 Create DAG Directory](#19-create-dag-directory)
- [1.10 Start Airflow](#110-start-airflow)
- [1.11 Access Airflow Web UI](#111-access-airflow-web-ui)
- [1.12 Enable/Disable Example DAGs](#112-enabledisable-example-dags)

---

## 1.1 Install WSL2
Follow the below steps to install WSL2
* Verify that WSL is enabled on Windows 10 machine.
  * In the Windows search bar, start typing “**turn windows features**” and select the first match which opens up **Windows Features** dialog.
  * On the **Windows Features** dialog, check the option **Windows Subsystem for Linux** and press **OK**.
  * WSL can also be enabled via command line by executing the command below in **Windows PowerShell** as an **Administrator**:
  
    ```powershell
    Copydism.exe /online /enable-feature /featurename:Microsoft-Windows-Subsystem-Linux /all /norestart
    ```
	
* After WSL is enabled, open a **Command Prompt** or **Windows PowerShell** as **Administrator** and run the following command to install the default WSL distribution which is **Ubuntu**.

  ```powershell
  wsl --install
  ```
  
* It starts downloading **Ubuntu** and installs it.

Once the installation is complete, it prompts to enter a new username (or accept the default username) and password to create a Unix/Linux account. 

Once the Linux account has been created successfully, the Windows command prompt `>` changes to the Linux prompt `$` with a mount location of current Windows directory in Linux system. WSL by default, mounts all Windows drives on `/mnt` location in Linux such that **C:** drive in Windows is mapped to `/mnt/c` path in Linux, **D:** drive in Windows is mapped to `/mnt/d` path in Linux and so on.

Open a new **Command Prompt** or **Windows PowerShell** and run the following command to see the status of WSL installed.

```powershell
wsl --list --verbose
```

or

```powershell
wsl -l -v
```

Apart from Ubuntu, other Linux distributions can also be installed. Use the command below to see the available distributions:

```powershell
wsl --list --online
```

Use the following syntax to install a specific distribution:

```powershell
wsl --install -d <DISTRO_NAME>
```

For example, to install **Debian** distribution, run this command:

```powershell
wsl --install -d Debian
```

Now that Ubuntu is ready to use, launch Ubuntu application by searching from the Start menu.

Once Ubuntu instance has been launched, enter the following command to verify the system information:

```bash
uname --a
```
 
It displays GNU/Linux distribution with WSL2 version details.

## 1.2 Update And Upgrade Ubuntu 
After the Linux distribution has been installed successfully, update and upgrade the Linux system to the latest version where necessary packages will be installed. 

In Ubuntu terminal, run the following commands:

```bash
sudo apt update	
sudo apt upgrade -y
```

Note that when a `sudo` command is executed for the first time, it asks for the password of the Linux account that was created during the Linux distribution installation.

## 1.3 Install Python Libraries 
Since **Apache Airflow** works on **Python** programming entirely, it is mandatory to have Python running on the machine before installing Airflow. **The latest version of Airflow works on Python 3.8 or high versions** only. 

Install the necessary Python libraries such as `pip` _(needed to install Python related packages)_, and `vnenv` _(needed to create a Python virtual environment)_ in WSL system.

Run the following command to install `python3-pip` and `python3-venv` libraries:

```bash
sudo apt install python3-pip python3-venv -y
```

Verify the Python version installed which must be Python 3.8 or later.

```bash
python3 --version
```

In my system, **Python 3.12.3** was installed.

## 1.4 Create Python Virtual Environment 
A virtual environment is needed for Airflow to keep Airflow files separate from other Python projects to avoid conflicts.

Run the following command to create a virtual environment named `airflow-env`. After this command is executed, you can see a new folder `airflow-env` created.

```bash
python3 -m venv airflow-env
```

Then activate the virtual environment using the below command:

```bash
source airflow-env/bin/activate
```

After the virtual environment got activated successfully, you can see the prompt changes with a prefix `(airflow-env)`.

## 1.5 Setup Airflow Directory
By default, Apache Airflow installs in WSL home directory `/home/<username>`. To enable easy access to Airflow data from Windows file system as well as WSL environment, create a dedicated Airflow directory on a Windows drive instead.

Here, I create an `Airflow` directory under `D:\ProgamFiles` in my system but you can create directory anywhere of your choice. 

Run the following commands to create a folder named `Airflow` in the `/mnt/d/ProgramFiles` _(replace `d/ProgramFiles` path with your own path)_:

```bash
cd /mnt
cd d/ProgramFiles 
mkdir Airflow
sudo chown $USER:$USER Airflow
cd airflow
pwd
```

Next, this Airflow directory path should be stored it in an environment variable named `AIRFLOW_HOME` for Airflow installation.

Run the following commands to open a nano editor to modify the `.bashrc` script:

```bash
cd ~
nano .bashrc
```

Enter the following line _(replace `d/ProgramFiles` with your path)_ in the script to save the airflow directory path in `AIRFLOW_HOME` environment variable:

```bash
export AIRFLOW_HOME=/mnt/d/ProgramFiles/Airflow
```

After the `AIRFLOW_HOME` environment variable is added in the script, press `Ctrl + S` to save and `Ctrl + X` to exit the nano editor. Next time, when the Ubuntu instance is started, we can navigate to Airflow directory using this variable.

Run the following command to source the `.bashrc` script to access `AIRFLOW_HOME` variable _(alternatively, you can close this instance and open a new Ubuntu instance to access the variable directly)_:

```bash
source ~/.bashrc
```

## 1.6 Install Apache Airflow
The latest version of Apache Airflow at the time of writing this document is **3.1.8** that is going to be installed. The standard Airflow 3 release includes core components like **API Server**, **Scheduler**, **LocalExecutor** (running as part of Scheduler), **DAG Processor** and **Triggerer**, utilizing **SQLite** as the default metadata database.

Run the following commands to install the Apache Airflow in the `AIRLFOW_HOME` path in the Python virtual environment. This installs Apache Airflow of whatever the latest version is.

```bash
source airflow-env/bin/activate
pip install apache-airflow
```

If a different version of Apache Airflow is needed, a `constraint` parameter must be provided to Python package installer `pip`.

For example, use the following commands to install **Airflow 2.6.1** version with **Python 3.10** installed:

```bash
AIRFLOW_VERSION=2.6.1
PYTHON_VERSION=3.10
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```

Verify if Airflow has been installed successfully using the below command:

```bash
airflow version
```
 
It displays the installed version of Apache Airflow (in my system, Airflow 3.1.8 version has been installed).

## 1.7 Install Dependent Libraries
Starting with Airflow 3 release, Python `graphviz` library is needed to display DAGs in graphical format through command line. If `graphviz` library is not installed, you can see DAGs but cannot run certain DAG commands through command line as it throws the following warning:

> /home/hp/airflow-multinode/venv/lib/python3.12/site-packages/airflow/cli/commands/dag_command.py:49 UserWarning: Could not import graphviz. Rendering graph to the graphical format will not be possible.

Run the following command to install `graphviz` library in the current Airflow environment:

```bash
pip install graphviz
```

## 1.8 Initialize Airflow Database
After the Apache Airflow has been installed successfully, run the following command to initialize the default **SQLite** as the metadata database with necessary tables needed for Airflow functionality. 

```bash
airflow db migrate
```

**Note:** The `airflow db init` command has been removed from Airflow 3 release. Instead, the `airflow db migrate` command should be used to initialize or migrate the metadata database. For more details, refer to the official [Airflow 3 Release Notes](https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html#removed-cli-flags-and-commands).

In the output, it displays the metadata database `sqlite` has been created and available at `$AIRFLOW_HOME/airflow.db`.

Navigate to Airflow directory and verify files created using the following commands:

```bash
cd $AIRFLOW_HOME
ls
```

You will observe that two files, `airflow.cfg` and `airflow.db` and a `logs` directory have been created. Note that the Airflow automatically generates the `airflow.cfg` file containing default settings in the `$AIRFLOW_HOME` directory.

Beginning with Airflow 3 release, the default **SimpleAuthManager** is used for authentication and user names are defined by the `simple_auth_manager_users` setting in the `airflow.cfg` file. This setting has a default value of `admin:admin` which indicates that a user named `admin` with the `admin` role is created and its password will be automatically generated up on the API server initial startup.

Open `airflow.cfg` file from the Windows system and verify the `simple_auth_manager_users` setting.

Alternatively, run the following command to verify the same from WSL:

```bash
cat $AIRFLOW_HOME/airflow.cfg | grep simple_auth_manager
```

**Note:** Airflow 3 does not allow to create users using `airflow users create` command _(in fact, `airflow users command` itself is not available)_ when using `SimpleAuthManager`. To create users in Airflow 3 version, install the Python package `apache-airflow-providers-fab` to get `FabAuthManager` and modify the `auth_manager` option as below in `airflow.cfg` file to enable it. After this, users can be created using `airflow users create` command.

```bash
auth_manager = airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager
```

## 1.9 Create DAG Directory
By default, Airflow scans for DAG files in `dags` folder available in `AIRFLOW_HOME` directory. The default DAG location can be changed by updating `dags_folder` configuration in `airflow.cfg` file. For now, we will go with the default DAG location.

Run the following commands to create `dags` folder in `AIRFLOW_HOME` directory:

```bash
cd $AIRFLOW_HOME
mkdir dags
```

## 1.10 Start Airflow
Now that Airflow has been initialized and the metadata tables were setup, Airflow components including **Scheduler**, **API Server** and **DAG Processor** must be started to bring the Airflow system into a usable state.

### Start Airflow Scheduler
Open a new Ubuntu terminal and start the **Airflow Scheduler** service using the below commands:

```bash
source airflow-env/bin/activate
airflow scheduler
```

In the scheduler output, you can see that Airflow 3 uses `LocalExecutor` by default. Keep this terminal open for the Scheduler to continue running as closing it will stop the service.

### Start Airflow API Server
Open another Ubuntu terminal and start the **Airflow API Server** service using the below commands which starts the service on the default port `8080`:

```bash
source airflow-env/bin/activate
airflow api-server
```

**Note:** The `airflow webserver` command has been removed and replaced with the `airflow api-server` command from Airflow 3 version. For more details, refer [Airflow 3 Release Notes](https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html#removed-cli-flags-and-commands).

In the API server’s output, you can observe that the password for `admin` user has been generated and the application has started running on `http://0.0.0:8080`. Keep this terminal open as closing the window will stop the API server service.

If you encounter an error indicating that the default port `8080` is already in use, start the API server service on an alternative port (e.g. `8089`) using the below command:

```bash
source airflow-env/bin/activate
airflow api-server -p 8089
```

After the API server starts successfully, the password for `admin` user is saved to the `simple_auth_manager_passwords.json.generated` file in the `AIRFLOW_HOME` directory.

### Start Airflow Dag Processor
Next, open another Ubuntu terminal and start the **Airflow Dag Processor** service using the below commands:

```bash
source airflow-env/bin/activate
airflow dag-processor
```

In the Dag Processor's output, it confirms that the `dag-folder` and `example_dags` are added to the database and searches for files in `dags-folder` at `$AIRFLOW_HOME/dags` directory and files in `example-dags` at `/home/<username>/airflow-env/lib/python3.12/site-packages/airflow/example_dags` directory.

## 1.11 Access Airflow Web UI
After the Airflow API server starts successfully, launch the UI at [http://localhost:8080](http://localhost:8080) _(or if your API server was started on a different port, say `8089`, then access UI at [http://localhost:8089](http://localhost:8089))_ and login using the `admin` user credentials _(the `admin` password is available in `simple_auth_manager_passwords.json.generated` file in the `AIRFLOW_HOME` directory)_.

After log in, it displays a home page with the history of DAG runs and status of Airflow components where you should see that **MetaDatabase**, **Scheduler** and **Dag Processor** in green.

In **Dags** tab, it displays the list of the example DAGs which are shipped with the Airflow installation.

## 1.12 Enable/Disable Example DAGs
By default, Airflow displays example DAGs in Airflow UI. However, these can be turned off by following the below steps:
* Open `airflow.cfg` file and set `load_examples = False` _(by default, this is set to `True`)_ and save the file.
* Then, open new Ubuntu terminal and run the below commands to reset the Airflow metadata database _(it asks you for confirmation to drop existing tables and enter `y`)_:

  ```bash
  source airflow-env/bin/activate
  airflow db reset
  ```
  
* Now, refresh the Airflow UI and example DAGs are gone.

To enable example DAGs back, follow the below steps:
* Open `airflow.cfg` file and set `load_examples = True` and save the file.
* Restart DAG processor. Go to the Ubuntu terminal where DAG processor was running earlier and restart it using the following command:
  
  ```bash
  airflow dag-processor
  ```
* Now, refresh the Airflow UI to see example DAGs again.