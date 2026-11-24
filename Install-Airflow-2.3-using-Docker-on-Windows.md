# Install Airflow 2.3 using Docker on Windows

> This guide installs Apache Airflow 2.3 on Windows using Docker Desktop and the official Docker Compose file (CeleryExecutor + PostgreSQL + Redis).

## Table of Contents
- [1.1 Verify Docker](#11-verify-docker)
- [1.2 Create Directory for Airflow Docker](#12-create-directory-for-airflow-docker)
- [1.3 Create Docker Compose File](#13-create-docker-compose-file)
- [1.4 Create Env File](#14-create-env-file)
- [1.5 Start Docker Compose](#15-start-docker-compose)
- [1.6 Validate Metadata Database](#16-validate-metadata-database)
- [1.7 Access Airflow UI](#17-access-airflow-ui)
- [1.8 Access Flower UI](#18-access-flower-ui)
- [1.9 Run Example DAG](#19-run-example-dag)

---

## 1.1 Verify Docker
If you do not have Docker software installed, then install it from the [official Docker website](https://docs.docker.com/desktop/setup/install/windows-install/). For detailed steps on how to install and use it, please follow [these](https://github.com/srimarrivada/Docker-Installation-on-Windows/blob/main/Install%20Docker%20on%20Windows%2010%20and%20Configure%20Swarm%20Cluster.md) steps.

Verify the installed Docker version using the below command:

```powershell
docker --version
```
 
Open **Docker Desktop** application which starts the **Docker Engine** at the backend.

Use the command below to verify the version of all Docker components:

```powershell
docker version
```

## 1.2 Create Directory for Airflow Docker
Create a new directory for the Airflow Docker project in Windows system. Here, I am using Gitbash software to create a directory and launch Microsoft VS Code application (for easy management) to write the Docker code. 

Open **Git Bash** command prompt and run the following commands to create a new directory for the Dockerized Airflow application at `D:\Learning\Docker\Projects` location and opens up **VSCode** application. You can choose any location to create a new directory.	

```bash
cd "D:\Learning\Docker\Projects"
mkdir Airflow2.3
cd Airflow2.3
code .
```
> **No Git software?** Install it on Windows from the [Git website](https://git-scm.com/install/windows).
> 
> **No VSCode software?** Follow [these](https://code.visualstudio.com/docs/setup/windows) steps.

## 1.3 Create Docker Compose File
Prepare a Docker Compose file to setup **Airflow 2.3** environment. Apache Airflow provides an official **docker-compose.yaml** file which can be downloaded from [here](https://airflow.apache.org/docs/apache-airflow/2.3.0/docker-compose.yaml) and used directly to setup an **Airflow 2.3.0** environment with **CeleryExecutor**, **[Redis](https://redis.io/)** (to act as message queue broker), **Postgres** (to act as metadata database as well as result backend), optional **[Flower](https://flower.readthedocs.io/en/latest/)** (to manage Celery workers components).

To use the Celery executor, the `executor` setting must be set to `CeleryExecutor` in the Airflow configuration file (`airflow.cfg)`. Additionally, the `broker_url` and `result_backend` settings must also be configured for Celery and the `sql_alchemy_conn` setting must be updated with the metadata database connection string.

Let us prepare a Docker Compose file similar to the one provided by the Apache Airflow.

* In VS Code application, Create **New File** named `docker-compose.yml` (or `docker-compose.yaml`) under `AIRLFOW2.3` project.
* In the `docker-compose.yml` file, write the following lines of code and save it.

```yaml
# Define common configurations using YAML anchors for reuse, reducing repetition.
x-airflow-common:
  &airflow-common
  # Specifies the Docker image to use for Airflow services. Uses the default value if the environment variable is not set
  image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.3.0} 
  environment:
    &airflow-common-env
    # Configures Airflow to use CeleryExecutor, enabling distributed task processing across workers
    AIRFLOW__CORE__EXECUTOR: CeleryExecutor
    # Connection string for the metadata database (PostgreSQL)
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow123@postgres:5432/airflowdb
    # Connection string for Celery results backend to store task results in the database
    AIRFLOW__CELERY__RESULT_BACKEND:  db+postgresql://airflow:airflow123@postgres:5432/airflowdb
    # Connection string for the Celery message broker (Redis), used for queuing tasks
    AIRFLOW__CELERY__BROKER_URL: redis://redis:6379/0
    # Controls whether Airflow loads built-in example DAGs.
    AIRFLOW__CORE__LOAD_EXAMPLES: true
  volumes:
    # Mounts the local 'dags' folder into the container, allowing Airflow to discover workflows.
    - ./dags:/opt/airflow/dags
    # Mounts the local 'logs' folder into the container to persist task logs outside the container.
    - ./logs:/opt/airflow/logs
    # Mounts the local 'plugins' folder into the container for custom Airflow plugins and extensions.
    - ./plugins:/opt/airflow/plugins
  # Sets the user ID within the container to match the host machine's user (often 50000) for consistent file permissions.
  user: "${AIRFLOW_UID:-50000}:0"
  depends_on:   
    &airflow-common-depends-on
    # Ensures Redis is healthy before starting the Airflow service.
    redis:
      condition: service_healthy
    # Ensures Postgres is healthy before starting the Airflow service.
    postgres:
      condition: service_healthy
  networks:
    # Connects to the internal backend network.
    - backnet

# Define services for each respective Airflow components
services:
  # Specifies PostgreSQL as the metadata database service
  postgres:
    image: postgres:15
    environment:
      # Sets up the default database, user, and password for Airflow
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow123
      POSTGRES_DB: airflowdb
    volumes:
      # Persists the database data using a named volume to prevent data loss on container removal.
      - postgres-db-volume:/var/lib/postgresql/data
    healthcheck:
      # Checks if the database is ready to accept connections
      test: ["CMD", "pg_isready", "-U", "airflow", "-d", "airflowdb"]
      interval: 5s
      retries: 5
    restart: always
    networks:
      - backnet
  
  # Specifies Redis as the message broker service which is used by Celery
  redis:
    image: redis:latest
    expose:
      # Exposes port 6379 internally within the Docker network
      - 6379
    healthcheck:
      # Checks if the Redis server is responsive
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 30s
      retries: 5
    restart: always
    networks:
    - backnet   
  
  # Specifies a one-off service to initialize the database and create the default admin user before other services start
  airflow-init:
    <<: *airflow-common
    entrypoint: /bin/bash
    # Checks Airflow version at entrypoint
    command:
      - -c
      - |
        exec /entrypoint airflow version
        #sleep infinity
    environment:
      <<: *airflow-common-env
      # Environment variables to trigger DB migration and user creation on startup script execution
      _AIRFLOW_DB_UPGRADE: 'true'
      _AIRFLOW_WWW_USER_CREATE: 'true'
      _AIRFLOW_WWW_USER_USERNAME: ${_AIRFLOW_WWW_USER_USERNAME:-airflow}
      _AIRFLOW_WWW_USER_PASSWORD: ${_AIRFLOW_WWW_USER_PASSWORD:-airflow}
      
  # Specifies the Airflow Webserver service, providing the UI
  airflow-webserver:
    <<: *airflow-common
    command: webserver
    healthcheck:
      # Checks the webserver's health endpoint to ensure it is running
      test: ["CMD", "curl", "--fail", "http://localhost:8080/health"] 
      interval: 10s
      timeout: 10s
      retries: 5
    ports:
      # Maps local port 8081 to the container's port 8080 (where the webserver runs)
      - 8081:8080
    restart: always
    depends_on:
      <<: *airflow-common-depends-on
      # Ensures initialization finishes successfully before starting the webserver
      airflow-init: 
        condition: service_completed_successfully
    networks:
      # Connects to both public (frontnet) and private (backnet) networks
      - frontnet
      - backnet
  
  # Specifies the Airflow Scheduler service, responsible for triggering tasks based on schedules
  airflow-scheduler:
    <<: *airflow-common
    command: scheduler
    healthcheck:
      # Checks if the scheduler job is running correctly
      test: ["CMD-SHELL", 'airflow jobs check --job-type SchedulerJob --hostname "$${HOSTNAME}"'] 
      interval: 10s
      timeout: 10s
      retries: 5
    restart: always
    depends_on:
      <<: *airflow-common-depends-on
      airflow-init: 
        condition: service_completed_successfully

  # Specifies the Airflow Worker service, which executes the actual tasks using Celery
  airflow-worker:
    <<: *airflow-common
    command: celery worker
    healthcheck:
      # Checks if the Celery worker is pingable and responsive
      test: 
        - "CMD-SHELL" 
        - 'celery --app airflow.executors.celery_executor.app inspect ping -d "celery@$${HOSTNAME}"' 
      interval: 10s
      timeout: 10s
      retries: 5
    restart: always
    depends_on:
      <<: *airflow-common-depends-on
      airflow-init: 
        condition: service_completed_successfully
  
  # Specifies the Airflow Triggerer service, used for running deferrable operators efficiently
  airflow-triggerer:
    <<: *airflow-common
    command: triggerer
    depends_on:
      <<: *airflow-common-depends-on
      airflow-init:
        condition: service_completed_successfully
    healthcheck:
      # Checks if the triggerer job is running correctly
      test: ["CMD-SHELL", 'airflow jobs check --job-type TriggererJob --hostname "$${HOSTNAME}"']
      interval: 10s
      timeout: 10s
      retries: 5
    restart: always
    
  # Specifies a utility service for running ad-hoc Airflow CLI commands
  airflow-cli:
    <<: *airflow-common
    command: bash -c "airflow -h && sleep infinity"
    environment:
      <<: *airflow-common-env
      # Disables the built-in health checks within the default entrypoint script, allowing CLI commands to execute immediately
      CONNECTION_CHECK_MAX_COUNT: 0
    profiles:
      # Marks this service as only starting when specifically requested
      - debug
  
  # Specifies a monitoring interface for Celery workers and tasks
  celery-flower:
    <<: *airflow-common
    command: celery flower
    ports:
      # Maps local port 5555 to the container's port 5555 for accessing the Flower UI
      - 5555:5555
    healthcheck:
      # Checks if the Flower web interface is up and running
      test: ["CMD-SHELL", "curl --fail http://localhost:5555"]
      interval: 10s
      timeout: 10s
      retries: 5
    restart: always
    depends_on:
      <<: *airflow-common-depends-on
      airflow-init:
        condition: service_completed_successfully    
    networks:
      # Connects to both public and private networks to allow UI access and internal communication
      - frontnet
      - backnet

# Define named volumes for Postgres DB storage
volumes:
  postgres-db-volume:

# Define custom networks for service isolation
networks:
  # Public network for external access and communication
  frontnet:
    driver: bridge

  # Private, internal network for backend services only
  backnet:
    driver: bridge
    internal: true
```

> **New to YAML?** Explore [Mastering YAML](https://github.com/srimarrivada/Mastering-YAML) or get quick awareness on [YAML Syntax](https://github.com/srimarrivada/YAML-Syntax).

The above code performs the following:
* Define `x-airflow-common` key to set a YAML anchor for configuration reuse across services.
  * Define YAML anchor `&airflow-common` to store the following configurations for later use:
    * Set the image name to the value of the `AIRFLOW_IMAGE_NAME` environment variable if it was already defined, otherwise default the image to `apache/airflow:2.3.0` which is an official Airflow 2.3 Docker image.
    * Define `environment` for the image by setting `&airflow-common-env` YAML anchor to reuse the following block of environment variables:
      * Set `AIRFLOW__CORE__EXECUTOR` environment variable to `CeleryExecutor`.
      * Set `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` environment variable to Postgres database URI as `postgresql+psycopg2://airflow:airflow123@postgres:5432/airflowdb` where `airflow` is the user name, `airflow123` is the password, `postgres` is the service/container name and `airflowdb` is the database name in PostgreSQL. Airflow recommends to use `psycopg2` driver in the SqlAlchemy connection string. Refer to [Airflow documentation](https://airflow.apache.org/docs/apache-airflow/stable/howto/set-up-database.html#setting-up-a-postgresql-database) to know more about setting up PostgreSQL Database.
      * Set `AIRFLOW__CELERY__RESULT_BACKEND` environment variable to Postgres database URI as `db+postgresql://airflow:airflow123@postgres:5432/airflowdb`. Refer to [Airflow documentation](https://airflow.apache.org/docs/apache-airflow-providers-celery/stable/configurations-ref.html#result-backend) about the configuration of Celery Result Backend.
      * Set `AIRFLOW__CELERY__BROKER_URL` environment variable to the default Redis URI as `redis://redis:6379/0` where `redis` is the service/container name and `6379` is the default port for Redis. Refer to [Airflow documentation](https://airflow.apache.org/docs/apache-airflow-providers-celery/stable/configurations-ref.html#broker-url) on setting up Celery Broker URL.
      * Set `AIRFLOW__CORE__LOAD_EXAMPLES` environment variable to `true` to load example DAGs when Airflow is started.
    * Define `volumes` by bind mounting various directories.
      * Map the local `dags` folder in the current location to the container's `/opt/airflow/dags` folder which is the default Airflow DAG directory where Python DAG files are placed.
      * Map the local `logs` folder in the current location to the container's `/opt/airflow/logs` folder where Airflow writes task execution logs making them accessible locally for debugging.
      * Map the local `plugins` folder to the container's `/opt/airflow/plugins` folder where all custom operators, hooks or interfaces for Airflow are placed.
    * Define `user` to the value of the `AIRFLOW_UID` environment variable if it was already defined, otherwise default the user to `-50000:0` which is used to run Airflow. If the `user` is not set, the processes inside the Docker container runs as `root` user by default which is not recommended.
  * Define YAML anchor `&airflow-common-depends-on` to store the following dependencies:
    * Set `redis` service condition to `service_healthy` to ensure the `redis` container is up and running.
    * Set `postgres` service condition to `service_healthy` to ensure the `postgres` container is up and running.
  * Define `networks` to connect to internal `backend` network.
* Create 9 services / containers:
  * `postgres` container as the Airflow metadata database service that:
    * Pulls `postgres:15` image.
    * Sets environment variables `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` to setup the default database, user and password for Airflow.
    * Maps `postgres-db-volume` volume to `/var/lib/postgresql/data` directory inside container to persist the database data using this named volume.
    * Performs health check of container with the `test` command `CMD pg_isready -U airflow -d airflowdb` at a regular `interval` of `5` seconds for a maximum of `5 retries` before setting the container status to unhealthy.
    * Restarts the container `always` in the event of failure.
    * Maps the container to `backnet` network.
  * `redis` container as the Celery message broker service for Airflow that:
    * Pulls `redis:latest` image.
    * Exposes the default redis port `6379` internally within Docker network.
    * Performs health check of container with the `test` command `CMD redis-cli ping` with a `timeout` of `30` seconds at a regular `interval` of `5` seconds for a maximum of `5 retries` before setting the container status to unhealthy.
    * Restarts the container `always` in the event of failure.
    * Maps the container to `backnet` network.
  * `airflow-init` container as the one-off service to initialize the Airflow database and create admin user that:
    * References `airflow-common` alias to load all predefined configuration into this service.
    * Sets `entrypoint` to `/bin/bash` as the default command the container runs when it starts up.
    * Sets command to run `exec /entrypoint airflow version` when container starts up.
    * References `airflow-common-env` alias to load all predefined environment variables and then append the following envrionment variables:
      * Set `_AIRFLOW_DB_UPGRADE` to `true` to trigger DB initialization or migration.
      * Set `_AIRFLOW_WWW_USER_CREATE` to `true` to create the DB user.
      * Set `_AIRFLOW_WWW_USER_USERNAME` to the value of the `_AIRFLOW_WWW_USER_USERNAME` environment variable if it was already defined, otherwise default the value to `airflow`.
      * Set `_AIRFLOW_WWW_USER_PASSWORD` to the value of the `_AIRFLOW_WWW_USER_PASSWORD` environment variable if it was already defined, otherwise default the value to `airflow`.
  * `airflow-webserver` container as the the Airflow Webserver service that:
    * References `airflow-common` alias to load all predefined configuration into this service.
    * Runs the command `webserver` to start the Airflow Webserver.
    * Performs health check of container with the `test` command `CMD curl --fail http://localhost:8080/health` with a `timeout` of `10` seconds at a regular `interval` of `10` seconds for a maximum of `5 retries` before setting the container status to unhealthy. This is to ensure if Webserver’s health endpoint is running.
    * Maps the local port `8081` to container port `8080` where the Airflow webserver runs.
    * Restarts the container `always` in the event of failure.
    * References `airflow-common-depends-on` alias to load all predefined dependencies into this service and then append dependency on `airflow-init` container status to become `service_completed_successfully`.
    * Connects to both public (`frontend`) and private (`backend`) networks to allow UI access and internal communication.
  * `airflow-scheduler` container as the the Airflow Scheduler service that:
    * References `airflow-common` alias to load all predefined configuration into this service.
    * Runs the command `scheduler` to start the Airflow Scheduler.
    * Performs health check of container with the `test` command `CMD-SHELL airflow jobs check --job-type SchedulerJob --hostname "$${HOSTNAME}"` with a `timeout` of `10` seconds at a regular `interval` of `10` seconds for a maximum of `5 retries` before setting the container status to unhealthy. This is to ensure if the Scheduler job is running correctly.
    * Restarts the container `always` in the event of failure.
    * References `airflow-common-depends-on` alias to load all predefined dependencies into this service and then append dependency on `airflow-init` container status to become `service_completed_successfully`.
  * `airflow-worker` container as the the Airflow Worker service (which executes actual tasks using Celery) that:
    * References `airflow-common` alias to load all predefined configuration into this service.
    * Runs the command `celery worker` to start the Celery Worker.
    * Performs health check of container with the `test` command `CMD-SHELL celery --app airflow.executors.celery_executor.app inspect ping -d "celery@$${HOSTNAME}"` with a `timeout` of `10` seconds at a regular interval of `10` seconds for a maximum of `5 retries` before setting the container status to unhealthy. This is to ensure if the Celery worker is pingable and responsive.
    * Restarts the container `always` in the event of failure.
    * References `airflow-common-depends-on` alias to load all predefined dependencies into this service and then append dependency on `airflow-init` container status to become `service_completed_successfully`.
  * `airflow-triggerer` container as the Airflow Triggerer service (used for running deferrable operators efficiently) that:
    * References `airflow-common` alias to load all predefined configuration into this service.
    * Runs the command `triggerer` to start the Airflow Triggerer.
    * Performs health check of container with the `test` command `CMD-SHELL airflow jobs check --job-type TriggererJob --hostname "$${HOSTNAME}"` with a `timeout` of `10` seconds at a regular `interval` of `10` seconds for a maximum of `5 retries` before setting the container status to unhealthy. This is to ensure if the triggerer job is running correctly.
    * Restarts the container `always` in the event of failure.
    * References `airflow-common-depends-on` alias to load all predefined dependencies into this service and then append dependency on `airflow-init` container status to become `service_completed_successfully`.
  * `airflow-cli` container as the utility service (to run adhoc CLI commands) that:
    * References `airflow-common` alias to load all predefined configuration into this service.
    * Runs the command `bash -c airflow`  which displays the usage of `airflow` command.
    * References `airflow-common-env` alias to load all predefined environment variables and then append `CONNECTION_CHECK_MAX_COUNT` environment variable to `0` to disable the built-in health checks within the default entrypoint script in the official Airflow Docker image that automatically attempts to wait for a successful connection to both the metadata database (PostgreSQL) and the Celery broker (Redis) before proceeding to run the main service command. Setting `MAX_COUNT` to `0` tells the entrypoint script to skip this waiting behavior entirely, allowing the CLI command to execute immediately.
    * Sets `profiles` as `debug` so that this service is only started when specifically requested with `--profile debug` option during `docker compose up`.
  * `celery-flower` container as the monitoring interface for Celery workers and tasks that:
    * References `airflow-common` alias to load all predefined configuration into this service.
    * Runs the command `celery flower` to start the Celery Flower.
    * Maps the local port `5555` to container port `5555` where the Airflow webserver runs.
    * Performs health check of container with the `test` command `CMD-SHELL curl --fail http://localhost:5555` with a `timeout` of `10` seconds at a regular `interval` of `10` seconds for a maximum of `5 retries` before setting the container status to unhealthy. This is to ensure if the Flower web interface is up and running.
    * Restarts the container `always` in the event of failure.
    * References `airflow-common-depends-on` alias to load all predefined dependencies into this service and then append dependency on `airflow-init` container status to become `service_completed_successfully`.
    * Connects to both public (frontend) and private (backend) networks to allow UI access and internal communication:
* Create named volume `postgres-db-volume` for Postgre DB storage.
* Create custom networks:
  * Create `frontnet` with `bridge` driver as the public network for external access and communication.
  * Create `backnet` with `bridge` driver (and set `internal` to `true`) as the private internal network for backend services.

## 1.4 Create Env File
Create an environment file called `.env` and add the below lines in it:

```env
AIRFLOW_IMAGE_NAME=apache/airflow:2.3.0
AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
```

The above file ensures the following environment variables are set:
* `AIRFLOW_IMAGE_NAME` is set to `apache/airflow:2.3.0` to use this Airflow image while pulling from Docker Hub.
* `AIRFLOW_UID` is set to `50000` which is needed for Linux operating system to look for non-root user id of Airflow, otherwise all Airflow files will be created with root user (`UID=0`) which is not at all recommended due to security reasons. This variable setting is not required for Windows or MAC systems but required for people using Linux operating system locally.
* `_AIRFLOW_WWW_USER_USERNAME` is set to `airflow` to create an Admin user called airflow in Airflow metadata database.
* `_AIRFLOW_WWW_USER_PASSWORD` is set to `airflow` for Admin user password in Airflow metadata database.

These variables override the default values set in `docker-compose.yml` file.
> Refer Airflow Docker Variables to know more.
 
# 1.5 Start Docker Compose
Follow the below steps to start Docker Compose and verify:
* Open **Windows PowerShell** or **Command Prompt** and navigate to the directory containing the above Docker Compose file as below:

  ```powershell
  D:
  cd "D:\Learning\Docker\Projects\Airflow2.3"
  ```
  
* Then, run the following command to initialize the metadata database and create a user:

  ```powershell
  docker compose up airflow-init
  ```
  
* It tries to download the images `redis:latest`, `postgres:15` and `apache/airflow:2.1.1-python3.8` first, if not already available:
* In couple of minutes, all images will be pulled successfully.
* Then, it creates `airflow23_backnet`,  `airflow23_postgres-db-volume` and three containers named `airflow23-postgres-1`, `airflow23-redis-1` and `airflow23-airflow-init-1`.
* Then, it starts `airflow-init` container where you can see it tries to connect to Postgress DB and performs database upgrade.
* Meanwhile, when the `airflow-init` container is starting, it automatically creates the `dag`, `logs` and `plugin` folders within the current project directory.
* In few secs, the container initializes `airflow.db` database and creates an `airflow` user with `Admin` role and exits the container as visible in the Docker Compose output.
* In Docker Desktop, you can see that `airflow23` container is available under which `postgres`, `redis` subcontainers are running and `airflow-init-1` sub container exited.
* Go to **Windows Powershell** and run the following command to start the remaining containers:

  ```powershell
  docker compose up 
  ```
  
  While starting containers, you may encounter the below error. Note that `5555` is the default port for Celery Flower UI
  > Ports are not available: exposing TCP port 0.0.0.0:5555

  When this error occurs, verify port `5555` has been occupied by another application:
  * Open **PowerShell** as an **Administrator** and run the command below to find the Process ID (PID) using the port:
  
    ```powershell
    netstat -ano | findstr :5555
    ```
  
    If a result appears, the last number in that row is the PID. You can kill the process occupying the port if needed using the below command:
	
    ```powershell
    taskkill /F /PID PID_From_Above
    ```
    
  * Sometimes, Windows reserves large blocks of ports for system services (like WSL2 or Hyper-V), making them forbidden for Docker.
    
    In this case, run the following command in Admin **PowerShell** to verify the reserved ports:
	
    ```powershell
    netsh interface ipv4 show excludedportrange protocol=tcp
    ```

    If `5555` falls under reserved port ranges due to which Docker is unable to start `celery-flower` container, then to resolve this issue, change the `celery-flower` container mapping port to a different value which is not reserved by the Windows.
	
    * Open `docker-compose.yaml` in VS Code and update `ports` attribute under `celery-flower` by exposing Celery Flower default port `5555` to local port `5285` as below and save it:
	
     ```yaml
      ports:
        # Maps local port 5555 to the container's port 5555 for accessing the Flower UI
        - 5255:5555
     ```
 
* Then, start Docker containers using the following command:

  ```powershell
  docker compose up
  ```

  You can observe that `celery-flower` container has started and `Connected to redis://redis:6379/0`. 

In **Docker Desktop**, you can see five additional containers `airflow-webserver-1`, `airflow-triggerer-1`, `airflow-scheduler-1`, `airflow-worker-1` and `celery-flower-1` have been created and running. 

Now, open another **Windows PowerShell** or **Command Prompt** and run the following commands to start `airflow-cli` container:

```powershell
D:
cd "D:\Learning\Docker\Projects\Airflow2.3"
docker compose --profile debug up 
```

In **Docker Desktop**, you can see the last container `airflow-cli-1` is created and running. 

## 1.6 Validate Metadata Database
Now that the Docker Airflow has been initialized and created the necessary metadata tables in the Postgres database, follow the below steps to verify the setup: 
* Open a new **Windows PowerShell** and run the following command to connect to postgres container and launch psql command line tool by connecting with airflow user.

  ```powershell
  docker exec -it $(docker container ls -f name=airflow23-postgres-1 -q) psql -U airflow -d airflowdb
  ```
  
  When `psql` is connected successfully, it displays `airflow=#` prompt to execute PostgreSQL commands.
  
* Run the below `psql` command to list the available tables in `airflow` database.
  **To see basic table information:**
  
  ```psql
  \dt
  ```
  
  **To see additional information of tables:**
  
  ```psql
  \dt+
  ```

  It displays total 35 tables such as `dag`, `dag_code`, `dag_run`, `job`, `task_instance`, `log`, `xcom`, etc. which store various metadata information. Run exit command to come out of psql.

## 1.7 Access Airflow UI
In the `docker-compose.yml` file, the Airflow webserver service port `8080` (container) is mapped to port `8081` on the local host. As a result, the Airflow UI can be accessed at `http://localhost:8081`.

* Open the web browser and enter `http:localhost:8081` URL which displays the Airflow UI. Enter the username as `airflow` and password as `airflow` to sign in.
* After login, it displays **DAGs** tab with a list of all prebuilt DAGs (examples loaded) from Apache Airflow.
* The **DAGs** tab displays the name of the **DAG**, it’s respective **Owner**, number of DAG **Runs** completed till now, **Schedule** time of DAG job, **Last Run** time, **Next Run** time, status of **Recent Tasks**, **Actions** to run or delete a DAG and **Links** to navigate to the respective sub tabs of a DAG.

## 1.8 Access Flower UI
Since the Celery Flower service has started on default port `5555` (container) and is mapped to port `5255` on the local host, the Flower UI is accessible at `http://localhost:5255`.

Open the web browser and enter `http://localhost:5255` URL where you can see Celery worker status.

## 1.9 Run Example DAG
Follow below steps to run an example DAG:
* Go to Airflow UI at `http://localhost:8081` and click on the example DAG `example_bash_operatior`.
* It opens `example_bash_operator` DAG page and displays the **Grid** view of DAG.
* Click on **Graph** tab to see the graph view of DAG with multiple tasks represented in boxes. In this DAG, there are 7 tasks available. Each task box changes its color when it moves to respective status as indicated on the top right side _(currently all tasks are in grey, indicating they are queued)_.
* Currently the DAG is in paused state. Click on **Pause/Unpause DAG** toggle displayed before the DAG name on the top left side to unpause this DAG first. Then click on run button on top right side and select **Trigger DAG** option to run the DAG manually.
* When the DAG is triggered, it displays a message `Triggered example_bash_operator, it should start any moment now` and shows each task box changing their colours _(for example, when a task is in running state, the box colour changes to parrot green)_.
* Once the DAG execution has completed successfully, it displays few tasks changed to dark green indicating **success** execution and few tasks changed to pink, indicating they are **skipped**. 
* Hover on any task to see the task status, start time, end time, duration, operator etc.
* Click on any task to see more information such as task **Instance Details**, **Rendered** template, **Log**, **XCom** data, etc.
* Click on **Calendar** tab to see the job execution history where it displays one job executed on `2026-03-20`.
* Click on **Task Duration** tab to see each task execution time for the current as well as any previous runs in a graphical format.
* Click on **Task Tries** tab to see the number of retries for each task _(here, retry is one since all tasks are completed in one try itself)_.
* Click on **Landing Times** tab to see the landing time of each task.
* Click on **Gantt** tab to see each task duration in a graphical form. Hovering on a task displays additional information such as task id, execution time, type of operation, task duration, number of tries, etc.
* Click on **Details** tab to see the details of the job such as number of success tasks, number of skipped tasks, **Schedule Interval**, **Catchup**, **Start Date**, **End Date**, **Max Active Runs**, etc.
* Click on **Code** tab to see the written Python code for this DAG.
* Click on **Audit Log** tab to see the selected events and operations that have been taken on this dag. The included and excluded events are set in the Airflow configuration, which by default remove view only actions.