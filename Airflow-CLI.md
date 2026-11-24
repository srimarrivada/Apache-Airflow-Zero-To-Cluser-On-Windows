# Airflow CLI
Apache Airflow has a rich Command Line Interface (CLI) to interact with Airflow and allows to perform many operations such as starting services, create users, set variables, set configuration, test DAG and more.

Use the following basic command to get the list of all high-level Airflow commands provided:

```bash
airflow -h
# or
airflow --help
```

> Using `-h` or `--help` next to any command name provides list of all options that the specific command can accept.

> **Note:**
> * If you are using local Airflow installation on Windows, then connect to **WSL2** (default **Ubuntu**) instance and activate the virtual environment to run Airflow CLI commands:
>   ```bash
>   source airflow-env/bin/activate
>   ```
> * If you are using Docker, then connect to any Airflow Docker container to run Airflow CLI commands.  
>   For instance, run this to connect to **Airflow Scheduler** container:
>   ```bash
>   docker exec -it $(docker container ls -f name=airflow23-airflow-scheduler-1 -q) bash
>   ```

## Table of Contents
- [1.1 Common Commands](#11-common-commands)
- [1.2 Process Commands](#12-process-commands)
- [1.3 Config Commands](#13-config-commands)
- [1.4 Database Commands](#14-database-commands)
- [1.5 User Commands](#15-user-commands)
- [1.6 Role Commands](#16-role-commands)
- [1.7 Connection Commands](#17-connection-commands)
- [1.8 Variable Commands](#18-variable-commands)
- [1.9 DAG Commands](#19-dag-commands)
- [1.10 Task Commands](#110-task-commands)
- [1.11 Job Commands](#111-job-commands)
- [1.12 Provider Commands](#112-provider-commands)
- [1.13 CeleryExecutor Commands](#113-celeryexecutor-commands)

---

## 1.1 Common Commands

### airflow version
It displays the version details of all Airflow components installed on the host.

```bash
airflow version
```

### airflow info 
It provides the complete information of the Airflow environment including version, executor, DAG folder location, task handler that is currently set, system information (such as `OS`, `architecture`, `python_version`, etc.), various tools being used, various paths that Airflow uses (such as `airflow_home`, `system_path`, `python_path`, etc.), provider details, etc.

```bash
airflow info
```

### airflow cheat-sheet
It displays a quick cheat sheet with the list of all available Airflow CLI commands.

```bash
airflow cheat-sheet
```

## 1.2 Process Commands
Airflow provides a set of handy commands to manage the core components of Airflow.

### airflow scheduler
It starts the Airflow Scheduler component on the default port **8793**.  

```bash
airflow scheduler
```

Some common options it can accept are:
* `-D` or `--daemon` to run process in the background
* `-n` or `--num-runs` to set the number of runs before exiting
* `-l` or `--logfile` to provide location of the Scheduler log file
* `--pid` to specify the location of PID file

To start the Airflow Scheduler in daemon mode:

```bash
airflow scheduler -D
```

When started in daemon mode successfully, it creates `airflow-scheduler.pid` file in the `$AIRFLOW_HOME` directory.

> **Note:** If it throws `[ERROR] connection to ('::', 8793) failed: [Errno 98] Address already in use`, a previous scheduler instance is still running on the port. Follow the below steps to resolve this issue:
> * Find all system processes occupying port 8793
>   ```bash
>   lsof -i :8793
>   ```
> * Terminate those unwanted processes (or old Airflow scheduler instances) using the below command:
>   ```bash
>   kill $(lsof -t -i :8793)
>   ```

To stop the scheduler started in daemon mode:
```bash
kill $$(cat $$AIRFLOW_HOME/airflow-scheduler.pid)
```
If started in the foreground (without `-D`), press `Ctrl + C` to shut it down gracefully.

### airflow webserver *(deprecated from Airflow 3 release)*
It starts the Airflow Webserver component on the default port **8080**.  

```bash
airflow webserver
```

Some common options it can accept are:
* `-D` or `--daemon` to run process in the background
* `-H` or `--hostname` to set a different host on which to run webserver
* `-p` or `--port` to set a different port on which to run webserver
* `-w` or `--workers` to set the number of workers to run on the webserver
* `-l` or `--logfile` to provide location of the webserver log file
* `--pid` to specify the location of PID file

To start the Airflow Webserver in daemon mode:
```bash
airflow webserver -D
```

> **Note:** If it throws `ERROR: [Errno 98] Address already in use`, another process is running on port **8080**. Start the webserver on a different port (e.g. 8081):
> ```bash
> airflow webserver -p 8081 -D
> ```

If started in the foreground, press `Ctrl + C` to shut it down gracefully.

### airflow api-server *(available from Airflow 3 release)*
It starts the Airflow API Server component on the default port **8080**.  

```bash
airflow api-server
```

Some common options it can accept are:
* `-D` or `--daemon` to run process in the background
* `-H` or `--host` to set a different host on which to run API server
* `-p` or `--port` to set a different port on which to run API server
* `-w` or `--workers` to set the number of workers to run on the API server
* `-l` or `--logfile` to provide location of the API server log file
* `--pid` to specify the location of PID file

To start the Airflow API Server in daemon mode:
```bash
airflow api-server -D
```
When started in daemon mode successfully, it creates `airflow-api_server.pid` file in the `$AIRFLOW_HOME` directory.

> **Note:** If API Server throws `ERROR: [Errno 98] Address already in use`, start the API server on a different port (e.g. 8081):
> ```bash
> airflow api-server -p 8081 -D
> ```

If started in the foreground, press `Ctrl + C` to shut it down gracefully.

In **Airflow 3.1.8**, starting the API server in daemon mode (`-D`) may cause it to crash immediately after launch. While an `airflow-api_server.pid` file is generated in `$AIRFLOW_HOME` directory, an `airflow-api_server.err` file also appears with the error: `libuv: io_uring_enter(getevents): Bad file descriptor`. This happens because the Airflow API server runs on **Uvicorn**, which relies on `libuv` for asynchronous I/O. Starting the server with `-D` flag uses Python's `daemon` functionality to fork and detach the process, which breaks asyncio's internal file descriptors. To work around this, run the API server in the foreground (without `-D`) or manage it in the background using a process manager like `systemd`, `supervisord`, or `nohup`.

### airflow dag-processor
It starts the Airflow DAG Processor component.  

```bash
airflow dag-processor
```

Some common options it can accept are:
* `-D` or `--daemon` to run process in the background
* `-n` or `--num-runs` to set the number of runs before exiting
* `-l` or `--logfile` to provide location of the DAG Processor log file
* `--pid` to specify the location of PID file

To start the Airflow DAG Processor in daemon mode:
```bash
airflow dag-processor -D
```
When started in daemon mode successfully, it creates `airflow-dag-processor.pid` file in the `$AIRFLOW_HOME` directory.

If started in the foreground, press `Ctrl + C` to shut it down gracefully.

When started in daemon mode successfully, it creates `airflow-dag-processor.pid` file in `$AIRFLOW_HOME` directory. However, the default installation of **Airflow 3.1.8** fails to start the DAG Processor in daemon mode: it starts and exits abruptly and generates `airflow-dag-processor.err`, which throws `sqlalchemy.exc.OperationalError: (sqlite3.OperationalError) locking protocol error`. This is because the DAG processor forks separate processes to parse DAG files, and these processes try to access the single SQLite database file at the same time, causing lock conflicts. Hence, run the DAG Processor in the foreground (without `-D`) or switch to **PostgreSQL** or **MySQL** as metadata database.

To stop the DAG processor started in daemon mode:
```bash
kill $$(cat $$AIRFLOW_HOME/airflow-dag-processor.pid)
```

### airflow triggerer
It starts the Airflow Triggerer component on default port **8794**.  
```bash
airflow triggerer
```

Some common options it can accept are:
* `-D` or `--daemon` to run process in the background
* `-l` or `--logfile` to provide location of the Triggerer log file
* `--pid` to specify the location of PID file

To start the Airflow Triggerer in daemon mode:
```bash
airflow triggerer -D
```

When started in daemon mode successfully, it creates `airflow-triggerer.pid` file in the `$AIRFLOW_HOME` directory.

> **Note:** If it throws `[ERROR] connection to ('::', 8794) failed: [Errno 98] Address already in use`, a previous trigerrer instance is still running which should be terminated:
> ```bash
> kill $(lsof -t -i :8794)
> ```

To stop the triggerer started in daemon mode:
```bash
kill $$(cat $$AIRFLOW_HOME/airflow-triggerer.pid)
```

If started in the foreground, press `Ctrl + C` to shut it down gracefully.

### airflow standalone
It sets up a standalone Airflow instance in the local environment by initializing the database and starting the Scheduler, Webserver (or API server), DAG Processor and Triggerer components in the foreground.

```bash
airflow standalone
```

Press `Ctrl + C` to shut down the components started by the standalone process.

> **Note:** If starting Airflow throws `sqlalchemy.exc.DatabaseError: (sqlite3.DatabaseError) database disk image is malformed`, your SQLite metadata database is corrupted. This usually happens when multiple processes access SQLite simultaneously.
> 
> Follow the steps below to fix it:
> * Stop all Airflow services
>   ```bash
>   kill $(ps aux | grep "airflow scheduler" | grep -v grep | awk '{print \$2}')
>   kill $(ps aux | grep "airflow dag-processor" | grep -v grep | awk '{print \$2}')
>   kill $(ps aux | grep "airflow triggerer" | grep -v grep | awk '{print \$2}')
>   kill $(ps aux | grep "airflow api_server" | grep -v grep | awk '{print \$2}')
>   ```
>
> * Rename the corrupted database
>   ```bash
>   mv $$AIRFLOW_HOME/airflow.db $$AIRFLOW_HOME/airflow.db.bak
>   ```
>
> * Re-initialize the metadata database
>   ```bash
>   airflow db migrate
>   ```
>
> * Start all Airflow services foreground in separate Ubuntu terminals
>   ```bash
>   source airflow-env/bin/activate
>   airflow api-server
>   
>   source airflow-env/bin/activate
>   airflow scheduler
>  
>   source airflow-env/bin/activate
>   airflow dag-processor
>   ```

## 1.3 Config Commands
Airflow provides a set of handy commands to manage the Airflow configuration about the DAG folders, logging, metrics, API, etc. Use `airflow config --help` to see the list of all available commands.

### airflow config list
It provides the current configuration details of the Airflow environment as found in the default `airflow.cfg` file.  

```bash
airflow config list
```

Some common options it can accept are: _(these options are available from **Airflow 2.7+** versions)_
* `-c` or `--comment-out-everything` to comment out all configuration options. This is helpful for new installation in Production environment
* `-a` or `--defaults` to show only the default configuration by commenting out everything
* `-d` or `--include-description` to show descriptions for the configuration options
* `-p` or `--exclude-providers` to exclude provider configuration in the output

To generate the default configuration and redirect it to `airflow.cfg` file in `AIRFLOW_HOME` directory:
```bash
airflow config list --defaults > "${AIRFLOW_HOME}/airflow.cfg"
```

### airflow config get-value 
It retrieves the specific configuration from the Airflow configuration file.
```bash
airflow config get-value <SECTION> <OPTION>
```

To retrieve the current executor that is currently set:
```bash
airflow config get-value core executor
```

To retrieve the secret backend that is currently set:
```bash
airflow config get-value secrets backend
```

## 1.4 Database Commands
Airflow provides a set of handy commands to perform various operations on the Airflow metadata database. Use `airflow db --help` to see the list of all available commands. 

### airflow db check
It verifies if the Airflow metadata can be reached which is useful for troubleshooting purposes.  

```bash
airflow db check
```

Some common options it can accept are: _(these options are available from Airflow 2.7+ versions)_
* `--retry` to specify if database check should be retried upon failure
* `--retry-delay` to provide the wait time between retries

### airflow db init _(deprecated from Airflow 3 release)_
It initializes the metadata database with the necessary tables. 
```bash
airflow db init
```

### airflow db upgrade _(deprecated from Airflow 3 release)_ 
It upgrades the metadata database to the latest version.  

```bash
airflow db upgrade
```

Some common options it can accept are:
* `-s` or `--show-sql-only` to print SQL scripts for offline migration
* `--from-version` to provide the Airflow version to upgrade from
* `-n` or `--to-version` to provide the Airflow version to upgrade to

### airflow db migrate _(available from Airflow 3 release)_
It migrates the metadata database to the latest version. From Airflow 3 release, it should be used to initialize or migrate the metadata database.  

```bash
airflow db migrate
```

Some common options it can accept are:
* `-s` or `--show-sql-only` to print SQL scripts for offline migration
* `--from-version` to provide the Airflow version to upgrade from
* `-n` or `--to-version` to provide the Airflow version to upgrade to

### airflow db downgrade 
It downgrades the metadata database to the lower version.  

```bash
airflow db downgrade
```

Some common options it can accept are:
* `-s` or `--show-sql-only` to print SQL scripts for offline migration
* `--from-version` to provide the Airflow version to downgrade to
* `-n` or `--to-version` to provide the Airflow version to downgrade to
* `-y` or `--yes` to not prompt for confirmation

### airflow db clean
It purges old records before the given timestamp in the metadata database tables. It requires `--clean-before-timestamp` option to specify the timestamp in `YYYY-MM-DD hh:mm:ss.s±hh:mm` format.  

```bash
airflow db clean --clean-before-timestamp <date or timestamp>
```

Some common options it can accept are:
* `--clean-before-timestamp` to specify the date or timestamp before which data should be purged. If no timezone is supplied then dates are assumed to be in Airflow default timezone
* `--dry-run` to perform a dry run
* `-t` or `--tables` to provide specific metadata table names to purge data
* `-y` or `--yes` to not prompt for confirmation

To purge data created before March 20th 2026 in all metadata tables: 
```bash
airflow db clean --clean-before-timestamp 2026-03-20
```

### airflow db shell
It launches the metadata database shell to execute database commands directly.

```bash
airflow db shell
```

In Ubuntu terminal, the above command might throw `No such file or directory: 'sqlite3'` error since `sqlite3` client is not installed by default.

Alternately, run the following commands in **Windows Powershell** to connect to `airflow-scheduler` Docker container and then launch **Airflow DB Shell** which connects to **Postgres** database to execute queries:
```
docker exec -it $(docker container ls -f name=airflow23-airflow-scheduler-1 -q) bash

airflow db shell
```

### airflow db reset
It erases all current information available in the metadata database and rebuild the database freshly. It is useful to create a “clean slate” database that may arise when setting up Airflow initially. This command only affects the database, and does not remove logs.  

```bash
airflow db reset
```

Some common options it can accept are:
* `-s` or `--skip-init` to only remove tables without initializing the database
* `-y` or `--yes` to not prompt for confirmation

## 1.5 User Commands
Airflow provides a set of handy commands to manage the Airflow users in the metadata database. From Airflow 3 release, these commands are not available in the default Airflow installation which uses **Simple Auth Manager**. To manage users in Airflow 3, the **Fab Auth Manager** must be installed and specified in the Airflow configuration file.

> **Note:** Airflow user commands can be executed in the Docker container which has **Airflow 2.3** installed. Connect to any Airflow container first as below:
> ```bash
> docker exec -it $(docker container ls -f name=airflow23-airflow-scheduler-1 -q) bash
> ```
> or
> ```bash
> docker exec -it $(docker container ls -f name=airflow23-airflow-worker-1 -q) bash
> ```
> or
> ```bash
> docker exec -it $(docker container ls -f name=airflow23-airflow-triggerer-1 -q) bash
> ```

Use `airflow users --help` to see the list of all available commands.

### airflow users list 
It lists the available users in the metadata database.  

```bash
airflow users list
```

Some common options it can accept are:
* `-o` or `--output` to print the output in the specific format such as `table` (default) or `json` or `yaml` or `plain`
  
### airflow users create 
It creates the user in the metadata database.  

```bash
airflow users create --username <username> --password <password> --firstname <firstname> --lastname <lastname> --role <role> --email <email>
```

Some common options it can accept are:
* `-u` or `--username` to specify the username of the new user to be created
* `-p` or `--password` to specify the password of the new user
* `--use-random-password` to confirm using a random password while creating a user without `-p` option
* `-f` or `--firstname` to specify the first name of the new user
* `-l` or `--lastname` to specify the last name of the new user
* `-r` or `--role` to specify the role of the new user. Default roles include **Admin**, **User**, **Op**, **Viewer**, and **Public**
* `-e` or `--email` to specify the email ID of the new user

To create a test user with **Viewer** role in the database:
```bash
airflow users create -u test -p test -f test -l user -r Viewer -e test_user@airflow.com
```

### airflow users add-role 
It adds a role to the given user available in the metadata database.  

```bash
airflow users add-role -r <role> -u <user name>
# or
airflow users add-role -r <role> -e <email>
```

Some common options it can accept are:
* `-r` or `--role` to specify the role to be added to the user. Default roles include **Admin**, **User**, **Op**, **Viewer**, and **Public**
* `-u` or `--username` to specify the name of the user
* `-e` or `--email` to specify the email ID of the user

To add **Admin** role to the `test` user: 
```bash
airflow users add-role -r Admin -u test
```
 
### airflow users remove-role 
It removes a specific role from the given user available in the metadata database.  

```bash
airflow users remove-role -r <role> -u <user name>
# or
airflow users remove-role -r <role> -e <email>
```

Some common options it can accept are:
* `-r` or `--role` to specify the role to be removed from the user. Default roles include **Admin**, **User**, **Op**, **Viewer**, and **Public**
* `-u` or `--username` to specify the name of the user
* `-e` or `--email` to specify the email ID of the user

To remove the **Admin** role from the user email `test_user@airflow.com`: 
```
airflow users remove-role -r Admin -e test_user@airflow.com
```

### airflow users export
It exports all available users to the given JSON file.

```bash
airflow users export <file path>
```

To export users to `airflow_users.json` file in the current directory: 
```bash
airflow users export airflow_users.json
```

Then, see the contents of `airflow_users.json` file:
```bash
cat airflow_users.json
```

### airflow users import
It imports all users from the given JSON file, updating any existing users.

```bash
airflow users import <file path>
```

To import users from `airflow_users.json` file available in the current directory: 
```bash
airflow users import airflow_users.json
```

### airflow users delete
It deletes an existing user from the metadata database.  

```bash
airflow users delete -u <user name>
# or
airflow users delete -e <email>
```

Some common options it can accept are:
* `-u` or `--username` to specify the name of the user
* `-e` or `--email` to specify the email ID of the user

To delete the `test` user: 
```bash
airflow users delete -u test
```

## 1.6 Role Commands
Airflow provides a set of handy commands to manage the Airflow roles in the metadata database. From **Airflow 3** release, these commands are not available in the default Airflow installation which uses **Simple Auth Manager**. To manage the Airflow roles in Airflow 3, the **Fab Auth Manager** must be installed and specified in the Airflow configuration file. 

> **Note:** Airflow role commands can be executed in the Docker container which has **Airflow 2.3** version installed. Connect to any Airflow container first as below:
> ```bash
> docker exec -it $(docker container ls -f name=airflow23-airflow-scheduler-1 -q) bash
> ```
> or
> ```bash
> docker exec -it $(docker container ls -f name=airflow23-airflow-worker-1 -q) bash
> ```
> or
> ```bash
> docker exec -it $(docker container ls -f name=airflow23-airflow-triggerer-1 -q) bash
> ```

Use `airflow roles --help` to see the list of all available commands.

### airflow roles list
It displays a list of all available roles in the metadata database. The default roles that comes with Airflow installation are **Admin**, **Op**, **Public**, **User** and **Viewer**. 

```bash
airflow roles list
```

Some common options it can accept are:
* `-o` or `--output` to print the output in the specific format such as `table` (default) or `json` or `yaml` or `plain`
* `-p` or `--permission` to show roles along with permissions (this option is available from Airflow 2.5+ versions)

### airflow roles create
It creates a custom role in the metadata database. 
```bash
airflow roles create <rolename>
```

To create a `dag_editor` role in the database:
```bash
airflow roles create dag_editor
```

### airflow roles add-perms _(available from Airflow 2.5+ versions)_
It adds permission to the specific role.  

```bash
airflow roles add-perms <role_name>
```

Some common options it can accept are:
* `-r` or `--resource` to specify the name of permissions
* `-a` or `--action` to specify the action of persmisons

To add **Dag Runs** permission with `can_read` action to `dag_editor` role:
```bash
airflow roles add-perms dag_editor -r "Dag Runs" -a can_read
```

### airflow roles del-perms _(available from Airflow 2.5+ versions)_
It adds permission to the specific role.  

```bash
airflow roles del-perms <role_name>
```

Some common options it can accept are:
* `-r` or `--resource` to specify the name of permissions
* `-a` or `--action` to specify the action of persmisons 

To remove **Dag Runs** permission from `dag_editor` role:
```bash
airflow roles del-perms dag_editor -r "Dag Runs"
```

### airflow roles export
It exports user created roles to the given JSON file.  

```bash
airflow roles export <file path>
```

Some common options it can accept are:
* `-p` or `--pretty` format JSON file by sorting role names and indenting by 4 spaces

To export roles to `airflow_roles.json` file in the current directory: 
```bash
airflow roles export airflow_roles.json
```

Then, see the contents of `airflow_roles.json` file:
```bash
cat airflow_roles.json
```

### airflow roles import
It imports custom roles from the given JSON file. 

```bash
airflow roles import <file path>
```

To import roles from `airflow_roles.json` file in the current directory: 
```bash
airflow roles import airflow_roles.json
```

### airflow roles delete _(available from Airflow 2.4+ versions)_
It deletes the custom role from the metadata database. 

```bash
airflow roles delete <role name>
```

To delete the `dag_editor` role: 
```bash
airflow roles delete dag_editor
```

## 1.7 Connection Commands
Airflow provides a set of handy commands to manage the Airflow connections in the metadata database. Use `airflow connections --help` to see the list of all available commands.

### airflow connections add
It creates a connection in the metadata database.  

```bash
airflow connections add <conn_id> --conn-uri <uri string>
# or
airflow connections add <conn_id> --conn-json <json string>
# or
airflow connections add <conn_id> --conn-type <connection type> --conn-host <host name> --conn-port <port number> --conn-login <user ID> --conn-password <password> --conn-schema <schema>
```

Some common options it can accept are:
* `--conn-uri` to specify the connection URI which is needed to add a connection using URI format
* `--conn-json` to specify the connection JSON which is needed to add a connection using JSON format
* `--conn-type` to specify the connection type which is needed to add connection without conn-uri or --conn-json option
* `--conn-host` to specify the host name which is needed with conn-type option
* `--conn-port` to specify the port which is needed with conn-type option
* `--conn-login` to specify the login id which is needed with conn-type option
* `--conn-password` to specify the password which is needed with conn-type option
* `--conn-schema` to specify the schema name which is needed with conn-type option
* `--conn-extra` to specify the conection parameter values which can be used with conn-type option

For example:
* To add a `postgres_airflow_db` connection using URI format: 
  ```bash
  airflow connections add postgres_airflow_db --conn-uri db+postgresql://airflow:airflow@postgres:5432/airflow?sslmode=verify-full&application_name=Airflow
  ```

* To add a `mysql_airflow_db` connection using JSON format: 
  ```bash
  airflow connections add 'mysql_dev_db' \
      --conn-json '{
          "conn_type": "mysql",
          "login": "airflow",
          "password": "airflow",
          "host": "mysql-host",
          "port": 3306,
          "schema": "airflow",
          "extra": {
              "ssl-mode": "REQUIRED",
              "connect_timeout": 10 
          }
      }'
  ```

### airflow connections list
It displays all connections available in the metadata database.  

```bash
airflow connections list
```

Some common options it can accept are:
* `--conn-id` to display the details of a given conection id 
* `-o` or `--output` to print the output in the specific format such as `table` (default) or `json` or `yaml` or `plain`

### airflow connections get
It retrieves the specific connection details from the Airflow metadatabase.  

```bash
airflow connections get <conn_id>
```

Some common options it can accept are:
* `-o` or `--output` to print the output in the specific format such as `table` (default) or `json` or `yaml` or `plain`

To retrieve the `mysql_dev_db` connection details:
```bash
airflow connections get mysql_dev_db
```

### airflow connections test _(availble from Airflow 3 release)_
It tests a specific connection available in the metadata database.

```bash
airflow connections test <conn_id>
```

To test the `postgres_airflow_db` connection: 
```bash
airflow connections test postgres_airflow_db
```

### airflow connections export
It exports all available connections to the given file.  

```bash
airflow connections export <file path>
```

Some common options it can accept are:
* `--file-format` to specify the file format for the export. Valid formats include `json` (default) or `yaml` or `env`.
* `--serialization-format` to specify the serialization format for the export when exporting as .env format. Valid formats include `uri` (default) or `json`.

To export connections in `airflow_connections.json` file in the current directory: 
```bash
airflow connections export airflow_connections.json
```

Then, see the contents of `airflow_connections.json` file:
```bash
cat airflow_connections.json
```

### airflow connectins import
It imports all connections from the given JSON or YAML or .env file, ignoring existing connections.

```bash
airflow connections import <file path>
```

To import connections from `airflow_connections.json` file in the current directory: 
```bash
airflow connections import airflow_connections.json
```

### airflow connections delete
It deletes an existing connection from the metadata database. 

```bash
airflow connections delete <conn_id>
```

To delete the `mysql_dev_db` connection: 
```bash
airflow connections delete mysql_dev_db
```

## 1.8 Variable Commands
Airflow provides a set of handy commands to manage the Airflow variables in the metadata database. Use `airflow variables --help` to see the list of all available commands.

### airflow variables set
It sets a variable in the metadata database by specifying variable key and a value.  

```bash
airflow variables set <variable_name> <variable_value>
# or
airflow variables set -j <variable_name> <json_string>
```

Some common options it can accept are:
* `-j` or `--json` to set a variable in JSON format by serializing that JSON variable

For example:
* To set a `RECIPIENT_EMAIL` variable: 
  ```bash
  airflow variables set RECIPIENT_EMAIL airflow_user@airflow.com
  ```

* To set the `ETL_CONFIG` JSON variable:
  ```bash
  airflow variables set -j ETL_CONFIG '{"start_date": "2026-01-01", "end_date": "2026-03-31", "is_full_load": false}'
  ```

### airflow variables list
It displays all variables available in the metadata database.  
Some common options it can accept are:
* `-o` or `--output` to print the output in the specific format such as `table` (default) or `json` or `yaml` or `plain`

```bash
airflow variables list
```

### airflow variables get
It retrieves the specific variable details from the Airflow metadatabase.  
Some common options it can accept are:
* `-d` or `--default` to specify a default value if variable doesnot exist
* `-j` or `--json` to retrieve variable in JSON format by deserializing that JSON variable

```bash
airflow variables get <variable_name>
# or
airflow variables get -j <variable_name>
```

For example:
* To get the `RECIPIENT_EMAIL` variable with a default value:
  ```bash
  airflow variables get RECIPIENT_EMAIL -d airflow_user@airflow.com
  ```
* To get the `ETL_CONFIG` JSON variable:
  ```bash
  airflow variables get -j ETL_CONFIG 
  ```

### airflow variables export
It exports all available variables to the given JSON file. 

```bash
airflow variables export <JSON_FILE_PATH>
```

To export users to `airflow_variables.json` file in the current directory: 
```bash
airflow variables export airflow_variables.json
```

Then, see the contents of `airflow_variables.json` file:
```bash
cat airflow_variables.json
```

### airflow variables import
It imports all variables from the given JSON file., updating the variable if already available.

```bash
airflow variables import <JSON_FILE_PATH>
```

To import variables from `airflow_variables.json` file in the current directory: 
```bash
airflow variables import airflow_variables.json
```

### airflow variables delete
It deletes an existing variable from the metadata database. 

```bash
airflow variables delete <VARIABLE_NAME>
```

To delete the `ETL_CONFIG` variable: 
```bash
airflow variables delete ETL_CONFIG
```

## 1.9 DAG Commands
Airflow provides a set of handy commands to manage DAGs. Use `airflow dags --help` to see the list of all available commands. 

### airflow dags list
It provides the list of all DAGs available in the metadata database. These can be prebuilt DAGs from the example directory and/or user created DAGs in the `DAG_FOLDER`. It can be used for troubleshooting purposes as well since it displays an error when there is an issue with a DAG.  

```bash
airflow dags list
```

Some common options it can accept are:
* `-S` or `--sub-dir` to specify the file/folder location to look for the DAG _(this option has been deprecated from Airflow 3 release)_
* `-B` or `--bundle-name` to specify the name of DAG bundle to use _(this option is available from Airflow 3 release)_
* `-o` or `--output` to print the output in the specific format such as `table` (default) or `json` or `yaml` or `plain`

> **Note:**
> If it responds with `No data found` message, Airflow Scheduler or DAG processor or both are not running. Login to Airflow UI at `http://localhost:8080` to verify.
>
> If you notice Scheduler and Dag Processor are in failed state in Airflow UI:
> * Open new **Ubuntu** terminal and start the Scheduler in foreground:
>   ```bash
>   source airflow-env/bin/activate
>   airflow scheduler
>   ```
>
> * Open new **Ubuntu** terminal and start the DAG Processor in foreground:
>   ```bash
>   source airflow-env/bin/activate
>   airflow dag-processor
>   ```
> * Then go back to the Ubuntu terminal and run the following command which should display the available DAGs:
>   ```bash
>   airflow dags list
>   ```

> **Note:**
> If you do not see example DAGs, make sure to set `load_examples = True` in `airflow.cfg` file and restart DAG processor.

### airflow dags show
It displays tasks defined in a specific DAG along with the task dependencies.  

```bash
airflow dags show <DAG_ID>
```

Some common options it can accept are:
* `--imgcat` to display graph using imgcat tool
* `-s` or `--save` to save output to the given file

To show tasks from the `example_bash_operator` DAG: 
```bash
airflow dags show example_bash_operator
```

### airflow dags test
It runs an adhoc run of a DAG.  

```bash
airflow dags test <DAG_ID> <LOGICAL_DATE>
```

Some common options it can accept are:
* `-s` or `--subdir` to specify the path of DAG file _(From Airflow 3 release, this option has been replaced with `-f` or `--dagfile-path`)_
* `--imgcat-dagrun` to print a diagram on the screen for the current DAG Run using the imgcat tool

To test the `example_python_operator` DAG with 2026-01-01 back fill date:
```bash
airflow dags test example_python_operator 2026-01-01
```

### airflow dags list-jobs
It displays the list of all jobs associated to DAGs.  

```bash
airflow dags list-jobs
```

Some common options it can accept are:
* `-d` or `--dag-id` to display jobs associated to a specific DAG ID
* `--limit` to return a limited number of records 
* `--state` to only list jobs in a given state. Acceptable states are `running`, `success`, `restarting`, `failed`
* `-o` or `--output` to print the output in the specific format such as `table` (default) or `json` or `yaml` or `plain`

To display all jobs associated to the `example_bash_operator` DAG: 
```bash
airflow dags list-jobs -d example_bash_operator
```

### airflow dags list-runs
It displays the list of all DAG runs associated to a specific DAG. 

```bash
airflow dags list-runs <DAG_ID>
```

Some common options it can accept are:
* `-s` or `--start-date` to specify start date in `YYYY-MM-DD` format
* `-e` or `--end-date` to specify end date in `YYYY-MM-DD` format
* `--state` to only list jobs in a given state. Acceptable states are `queued`, `running`, `success`, `failed`
* `--no-backfill` to filter all backfill DAG runs of the given DAG
* `-o` or `--output` to print the output in the specific format such as `table` (default) or `json` or `yaml` or `plain`

To display the history of `example_python_operator` DAG runs:
```bash
airflow dags list-runs example_python_operator
```

### airflow dags state
It displays the state of a current DAG run for a specific DAG.

```bash
airflow dags state <DAG_ID> <LOGICAL_EXECUTION_DATE>
```

To get the state of `example_python_operator` DAG with 2026-01-01 logical execution date:
```bash
airflow dags state example_python_operator 2026-01-01
```

### airflow dags trigger
It manually triggers a DAG to start execution.  

```bash
airflow dags trigger <DAG_ID>
```

Some common options it can accept are:
* `-e` or `--exec-date` to specify the execution date of DAG (From Airflow 3 release, this option has been replaced with `-l` or `--logical-date`)
* `-r` or `--run-id` to specify the run id for this DAG run
* `-c` or `--conf` to substiture JSON string into DagRun's conf attribute
* `-o` or `--output` to print the output in the specific format such as `table` (default) or `json` or `yaml` or `plain`

For example:
* To trigger the `example_bash_operator` DAG with the current execution date:
  ```bash
  airflow dags trigger example_bash_operator
  ```

* To trigger the DAG `example_bash_operator` with execution date as 2026-03-01 in Airflow 3 environment:
  ```bash
  airflow dags trigger example_bash_operator -l 2026-03-01
  ```
  When the execution date is in the past, it will trigger tasks only when `catchup=True` is set in that DAG definition.

* To trigger the `example_bash_operator` DAG with the future execution date:
  ```bash
  airflow dags trigger example_bash_operator -l '2026-03-20T21:18:00+00:00'
  ```
  > **Note:** Mention the date here with 2 minutes later than the current date displayed in the server and DAG will be scheduled to run at that date.

### airflow dags unpause
It unpauses a given DAG, allowing its tasks to be scheduled and executed again. By default, all example DAGs are in paused state unless explicitly unpaused.  

```bash
airflow dags unpause <DAG_ID>
```

Some common options it can accept are: _(these options are available from Airflow 3 release)_
* `-o` or `--output` to print the output in the specific format such as `table` (default) or `json` or `yaml` or `plain`
* `-y` or `--yes` to not prompt for confirmation

To unpause the `example_bash_operator` DAG:
```bash
airflow dags unpause example_bash_operator
```

### airflow dags pause
It pauses a given DAG, preventing its tasks from being scheduled or DAG being executed. By default, all example DAGs are in paused state unless explicitly unpaused.  

```bash
airflow dags pause <DAG_ID>
```

Some common options it can accept are: _(these options are available from Airflow 3 release)_
* `-o` or `--output` to print the output in the specific format such as `table` (default) or `json` or `yaml` or `plain`
* `-y` or `--yes` to not prompt for confirmation

To pause the `example_bash_operator` DAG:
```bash
airflow dags pause example_bash_operator
```

### airflow dags next-execution
It displays the next execution datetimes of a given DAG.  

```bash
airflow dags next-execution <DAG_ID>
```

Some common options it can accept are: _(these options are available from Airflow 3 release)_
* `-n` or `–num-executions` to show the number of next logical execution date times

For example:
* To display the next execution datetime of the `example_bash_operator` DAG:
  ```bash
  airflow dags next-execution example_bash_operator
  ```

* To display 3 next execution datetimes of the `example_bash_operator` DAG:
  ```bash
  airflow dags next-execution example_bash_operator -n 3
  ```

### airflow dags report
It displays the DAG loading report with information like file location to look for a specific DAG, DAG duration, number of tasks, etc.  

```bash
airflow dags report
```

Some common options it can accept are: 
* `-S` or `--sub-dir` to specify the file/folder location to look for DAGs _(this option has been deprecated from Airflow 3 release)_
* `-B` or `--bundle-name` to specify the name of DAG bundle to use _(this option is available only in latest Airflow 3 release)_
* `-o` or `--output` to print the output in the specific format such as `table` (default) or `json` or `yaml` or `plain`

### airflow dags delete
It deletes all DB records related to a specific DAG.  

```bash
airflow dags delete <DAG_ID>
```

Some common options it can accept are:
* `-y` or `--yes` to not prompt for confirmation

To delete the `example_bash_operator` DAG:
```bash
airflow dags delete example_bash_operator
```

### airflow dags backfill _(deprecated from Airflow 3 release)_
It executes the given DAG for a specified historical date. It performs backfills of historical data between start date and end date provided without the need to run the scheduler. This is useful when some business logic is changed in the existing workflow and needs to update historical data. Backfills do not create DagRun entries in the database, as they are not run by the Scheduler.  

```bash
airflow dags backfill <DAG_ID> -s <START_DATE> -e <END_DATE>
```

Some common options it can accept are:
* `-s` or `--start-date` to specify start date in `YYYY-MM-DD` format
* `-e` or `--end-date` to specify end date in `YYYY-MM-DD` format
* `-n` or `--dry-run` to perform dry run of each task
* `-B` or `--run-backwards` to run tasks from the most recent day first 
* `-l` or `--local` to run task using LocalExecutor

To run `example_bash_operator` DAG with specific start and end dates:
```bash
airflow dags backfill -s 2026-02-10 -e 2026-03-20 example_bash_operator
```

### airflow backfill create
It executes the given DAG for a specified historical date range. From Airflow 3, this command should be used for DAGs backfill.  

```bash
airflow backfill create --dag-id <DAG_ID> --from-date <START_DATE> --to-date <END_DATE>
```

Some common options it can accept are:
* `--dag-id` to specify the DAG ID to backfill
* `--from-date` to specify start date in `YYYY-MM-DD` format to backfill
* `--to-date` to specify end date in `YYYY-MM-DD` format to backfill
* `--dry-run` to perform dry run
* `--run-backwards` to run tasks from the most recent day 

To run `example_python_operator` DAG with specific start and end dates:
```bash
airflow backfill create --dag-id example_bash_operator --from-date 2025-02-10 --to-date 2025-03-20
```
While creating a backfill, it throws `database is locked` error since the default SQL Alchemy database doesnot accept multiple connections and it is already being accessed by the Scheduler and DAG Processor.

## 1.10 Task Commands
Airflow provides a set of handy commands to manage tasks such as running a task, testing a task, checking the task status, etc. Use `airflow tasks --help` to see the list of all available commands.

### airflow tasks list
It displays the list of all tasks available for a specific DAG. 

```bash
airflow tasks list <DAG_ID>
```

Some common options are:
* `-S` or `--sub-dir` to specify the file/folder location to look for the DAG _(this option has been deprecated from Airflow 3 release)_
* `-B` or `--bundle-name` to specify the name of DAG bundle to use _(this option is available from Airflow 3 release)_
* `-o` or `--output` to print the output in the specific format such as `table` (default) or `json` or `yaml` or `plain`

To get the list of tasks for `example_dag_decorator` DAG:
```bash
airflow tasks list example_dag_decorator
```

### airflow tasks test
It tests a specific task within a DAG without checking for dependencies or recording its state in the database. It can be used to run an adhoc task without affecting the metadata database, or being concerned about task dependencies. It is also helpful for a quick testing and debugging of a DAG task. 

```bash
airflow tasks test <DAG_ID> <TASK_ID> <EXECUTION_DATE>
# or
airflow tasks test <DAG_ID> <TASK_ID> <DAG_RUN_ID>
```

Some common options it can accept are:
* `-S` or `--sub-dir` to specify the file/folder location to look for the DAG _(this option has been deprecated from Airflow 3 release)_
* `-B` or `--bundle-name` to specify the name of DAG bundle to use _(this option is available from latest Airflow 3 release)_
* `--env-vars` to set environment variables in both parsing time and runtime for each of entry supplied in a JSON dictionary
* `-t` or `--task-params` to send JSON parameters dictionary to the task

To test the `print_the_context` task in the `example_python_operator` DAG:
```bash
airflow tasks test example_python_operator print_the_context 2026-03-21
```

### airflow tasks run _(deprecated from Airflow 3 release)_
It runs a specific task in a DAG. 

```bash
airflow tasks run <DAG_ID> <TASK_ID> <EXECUTION_DATE>
# or
airflow tasks run <DAG_ID> <TASK_ID> <DAG_RUN_ID>
```

Some common options it can accept are:
* `-S` or `--sub-dir` to specify the file/folder location to look for the DAG
* `--cfg-path` to specify the path of custom Airflow configuration file
* `-i` or `--ignore-dependencies` to ignore task-specific dependencies such as upstream, depends_on_past, and retry delay dependencies
* `-A` or `--ignore-all-dependencies` to ignore all non-critical dependencies 
* `-f` or `--force` to ignore previous task instance state and rerun regardless if task already succeeded/failed 
* `-l` or `--local` to run task using LocalExecutor

To run the `runme_0` task in the `example_bash_operator` DAG:
```bash
airflow run example_bash_operator runme_0 2026-03-20
```

### airflow tasks state
It displays the latest status of a specific task executed in a specific DAG run. 

Syntax:
```bash
airflow tasks state <DAG_ID> <TASK_ID> <LOGICAL_EXECUTION_DATE>
# or
airflow tasks state <DAG_ID> <TASK_ID> <DAG_RUN_ID>
```

Some common options it can accept are:
* `-S` or `--sub-dir` to specify the file/folder location to look for the DAG _(this option has been deprecated from Airflow 3 release)_
* `-B` or `--bundle-name` to specify the name of DAG bundle to use_ (this option is available from Airflow 3 release)_
* `--map-index` to include mapped task index

To get the state of `print_the_context` task in `example_python_operator` DAG with 2026-01-01 logical execution date:
```bash
airflow tasks state example_python_operator print_the_context 2026-01-01
```

### airflow tasks states-for-dag-run
It displays the latest status of all task instances in a specific DAG run. 

```bash
airflow tasks states-for-dag-run <DAG_ID> <LOGICAL_EXECUTION_DATE>
# or
airflow tasks states-for-dag-run <DAG_ID> <DAG_RUN_ID>
```

To get the status of all task instances in `example_python_operator` DAG with 2026-01-01 logical execution date:
```bash
airflow tasks states-for-dag-run example_python_operator 2026-01-01
```

### airflow tasks clear
It removes a set of task instances of a given DAG in the metadata database for the specific DAG ID. This is useful when iterating on the functionality of a workflow/DAG.

```bash
airflow tasks clear <DAG_ID>
```

Some common options are:
* `-s` or `--start-date` to specify start date in `YYYY-MM-DD` format
* `-e` or `--end-date` to specify end date in `YYYY-MM-DD` format
* `-f` or `--only-failed` to delete tasks instances related to only failed jobs
* `-r` or `--only-running` to delete tasks instances related to only failed jobs
* `-u` or `--upstream` to include upstream tasks
* `-d` or `--downstream` to include downstream tasks
* `-y` or `--yes` to not prompt for confirmation

To clear tasks instances of `example_python_operator` DAG:
```bash
airflow tasks clear example_python_operator
```

## 1.11 Job Commands
Airflow provides a set of handy commands to manage jobs such as verifying jobs if they are still active. Use `airflow jobs --help` to see the list of all available commands. Currently, there is only job command available:

### airflow jobs check
It checks if one or more jobs are still alive. This is useful to verify the health of Scheduler, DAG Processor and Triggerer components if they are functioning correctly. This command exits with a non-zero error code if there is a problem. 

```bash
airflow jobs check
```

Some common options it can accept are:
* `--job-type` to specify the type of job to be checked which can be either `SchedulerJob`, `DagProcessorJob` or `TriggererJob`
* `--local` to display jobs only from the local host
* `--hostname` to display jobs form the specified host
* `--limit` to check of number of recent jobs (set this to 0 to disable the limit)
* `–allow-multiple` to allow for muliltple matching alive jobs to display

To check if the local scheduler is working properly:
```bash
airflow jobs check --job-type SchedulerJob --local
```

## 1.12 Provider Commands
Airflow provides a set of handy commands to displays information about Airflow providers. Use `airflow providers --help` to see the list of all available commands.

### airflow providers list
It provides the list of installed providers in the Airflow environment. 

```bash
airflow providers list
```

Some common options it can accept are:
* `-o` or `--output` to print the output in the specific format such as `table` (default) or `json` or `yaml` or `plain`

### airflow providers configs _(available from Airflow 3 release)_
It displays the information about the provider configuration. 

```bash
airflow providers configs
```

Some common options it can accept are:
* `-o` or `--output` to print the output in the specific format such as `table` (default) or `json` or `yaml` or `plain`

### airflow providers get
It retrieves the details of the specific installed providers in the Airflow environment. 

```bash
airflow providers get <provider_name>
```

Some common options it can accept are:
* `-f` or `--full` to display the full information of a provider
* `-o` or `--output` to print the output in the specific format such as `table` (default) or `json` or `yaml` or `plain`

To get the details of `apache-airflow-providers-standard` provider that comes with the standalone Airflow 3 installation:
```bash
airflow providers get apache-airflow-providers-standard
```

### airflow providers logging
It retrieves the list of task logging handlers provided. 

```bash
airflow providers logging
```

Some common options it can accept are:
* `-o` or `--output` to print the output in the specific format such as `table` (default) or `json` or `yaml` or `plain`

### airflow providers hooks
It retrieves the list of registered provider hooks. 

```bash
airflow providers hooks
```

Some common options are:
* `-o` or `--output` to print the output in the specific format such as `table` (default) or `json` or `yaml` or `plain`

### airflow providers links
It retrieves the list of extra links registered by the providers. 

```bash
airflow providers links
```

Some common options it can accept are:
* `-o` or `--output` to print the output in the specific format such as `table` (default) or `json` or `yaml` or `plain`

## 1.13 CeleryExecutor Commands
Celery provides a set of handy commands to manage **Celery Executor**. These commands are visible only when **CeleryExecutor** is configured in Airflow environment. Refer [Airflow Documentation](https://airflow.apache.org/docs/apache-airflow-providers-celery/stable/cli-ref.html#celery-executor-commands) to get the complete details of all Celery commands. 

### airflow celery worker
It starts the Airflow Celery Worker component. 

```bash
airflow celery worker
```

Some common options it can accept are:
* `-H` or `--celery-hostname` to specify the hostname on which Celery worker should run
* `-a` or `--autoscale` to specify the minimum and maximum number of Celery workers to auto scale
* `-c` or `--concurrency` to set the number of worker processes
* `-q` or `--queues` to specify a list of queues to be served by this Celery worker
* `-D` or `--daemon` to run process in the background
* `-l` or `--logfile` to provide location of the Celery worker log file
* `--pid` to specify the location of PID file

To start the celery worker running in the background:
```bash
airflow celery worker -D
```

### airflow celery stop
It stops the Airflow Celery Worker gracefully. 

```bash
airflow celery stop
```

Some common options it can accept are:
* `--pid` to specify the location of PID file

### airflow celery flower
It starts the Airflow Celery Flower component. 

```bash
airflow celery flower
```

Some common options are:
* `-H` or `--hostname` to specify the hostname on which Flower should run
* `-p` or `--port` to specify the port on which Flower should run
* `-c` or `--flower-conf` to specify the configuration file for Flower
* `-D` or `--daemon` to run process in the background
* `-l` or `--logfile` to provide location of the Celery worker log file
* `--pid` to specify the location of PID file