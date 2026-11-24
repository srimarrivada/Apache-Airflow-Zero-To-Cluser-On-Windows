# Airflow Docker Variables

When Airflow is setup in a Docker container, there are standard Docker environment variables that are passed to Airflow containers. These variables are typically prefixed with `_AIRFLOW `and used in Airflow’s Docker image entry point to control the container's startup behavior. 

* `_AIRFLOW_DB_MIGRATE` variable automatically runs the database migration command (`airflow db migrate`) at container startup when this variable is set to any non-empty value (such as `true` or `1`).
* `_AIRFLOW_WWW_USER_CREATE` variable triggers the automatic creation of a webserver admin user at container startup when this variable is set to any non-empty value. It is primarily used for quick testing or development environments.
* `_AIRFLOW_WWW_USER_USERNAME` variable sets the username for the admin user created with `_AIRFLOW_WWW_USER_CREATE`. By default, this is set to `admin` value.
* `_AIRFLOW_WWW_USER_PASSWORD` variable is used to specify the password for the automatically created admin user. For security reasons, there is no default value, so it must be set for user creation to succeed.
* `_AIRFLOW_WWW_USER_PASSWORD_CMD` variable is used to run the specified shell command and use its output as the password, preventing the password from being stored in plaintext configuration files. This a more secure alternative to `_AIRFLOW_WWW_USER_PASSWORD` variable.
* `_AIRFLOW_WWW_USER_FIRSTNAME` variable is used to specify the first name for the automatically created admin user. By default, this is set to `Airflow` value.
* `_AIRFLOW_WWW_USER_LASTNAME` variable is used to specify the last name for the automatically created admin user. By default, this is set to `Admin` value.
* `_AIRFLOW_WWW_USER_EMAIL` variable is used to specify the email address for the automatically created admin user. By default, this is set to `airflowadmin@example.com` value.
* `_AIRFLOW_WWW_USER_ROLE` variable is used to specify the role for the automatically created user. By default, this is set to `Admin` value.

Airflow also uses a variety of other environment variables, particular for deployment in Docker.
* `AIRFLOW_IMAGE` variable specifies the Docker image to use for Airflow containers.
* `AIRFLOW_VERSION` variable is used to set the Airflow version when building the custom Docker image.
* `AIRFLOW_UID` variable is used to set the User ID (UID) for running Airflow containers to manage file permissions and avoid conflicts with the host system _(Before Airflow 2.2, `AIRLFOW_GID` variable was also used to specify the group ID to prevent permission issues but this variable has been deprecated in latest Airflow versions)_.
* `AIRFLOW_LOAD_EXAMPLES` variable controls whether the example DAGs are loaded and visible in the Airflow UI when the environment starts up. By default, this is set to `no`.
* `AIRFLOW_COMPONENT_TYPE` variable is used to specify which Airflow component a container will run, defining the role of a container. This variable typically expects `webserver` or `scheduler` or `worker` values.
* `C_FORCE_ROOT` variable forces Airflow components to run as the `root` user within the container. It is typically used for debugging permission issues and is not recommended for production.
* `_PIP_ADDITIONAL_REQUIREMENTS` variable is helpful to specify additional Python packages to be installed when the Airflow containers start. It is an easy way to add custom dependencies for the respective DAGs without rebuilding the Docker image. 
* `PYTHONWARNINGS` variable is a standard Python variable that can be used to control the handling of warning messages from DAGs and other Airflow components.
* `DUMB_INIT_SETSID` variable configures how `dumb-init` (used by the `entrypoint`) handles signal propagation within the container, which is important for gracefully stopping processes like Celery workers. 
