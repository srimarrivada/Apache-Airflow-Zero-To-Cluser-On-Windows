# Apache-Airflow-Zero-To-Cluster-On-Windows

A hands-on guide to **Apache Airflow** on a Windows system. It starts from zero with a single-node installation of **Airflow 3.1 in WSL2** and a containerised **Airflow 2.3 setup in Docker**, and ends with a **multi-node CeleryExecutor cluster** (Redis broker + MySQL metadata database), all running on one Windows machine. Along the way it covers the Airflow CLI, creating DAGs, the REST API, integration with Apache Hadoop & Apache Spark, and Airflow best practices.

📄 Complete details are available in the PDF guide, split into two parts due to GitHub's file size limit:
- [Part 1](docs/Airflow_Overview_Setup_part1.pdf)
- [Part 2](docs/Airflow_Overview_Setup_part2.pdf)

## 🗺️ The Journey
**Zero → Install → Operate → Integrate → Cluster → Best Practices**

| Stage | Guide |
|---|---|
| 🟢 Install | [Install Airflow 3.1 using WSL on Windows](Install-Airflow-3.1-using-WSL-on-Windows.md) |
| 🟢 Install | [Install Airflow 2.3 using Docker on Windows](Install-Airflow-2.3-using-Docker-on-Windows.md) |
| 📎 Reference | [Airflow Docker Variables](Airflow-Docker-Variables.md) |
| 🔵 Operate | [Airflow CLI](Airflow-CLI.md) |
| 🔵 Operate | [Create New DAG](Create-New-DAG.md) |
| 🔵 Operate | [Airflow REST API](Airflow-REST-API.md) |
| 🟠 Integrate | [Integrate Airflow with Apache Hadoop](Integrate-Airflow-with-Apache-Hadoop.md) |
| 🟠 Integrate | [Integrate Airflow with Apache Spark](Integrate-Airflow-with-Apache-Spark.md) |
| 🔴 Cluster | [Configure Multi-Node Celery Airflow](Configure-Multi-Node-Celery-Airflow.md) |
| ⭐ Production | [Airflow Best Practices](Airflow-Best-Practices.md) |

## 📚 Topics Covered in the PDF
1. Apache Airflow Overview
2. Airflow Architecture (Single-Node & Multi-Node)
3. Airflow Process Flow
4. Airflow 2 vs 3 Architecture
5. Executor Types (Local, Celery, Kubernetes, Hybrid)
6. Airflow Terminologies (DAGs, Tasks, Operators, Deferrable Operators, Task Groups, XComs, Variables, Hooks, etc.)
7. Install Airflow in Windows (WSL2 & Docker)
8. Airflow CLI
9. Create New DAG
10. Airflow REST API
11. Integrate Airflow with Apache Hadoop
12. Integrate Airflow with Apache Spark
13. Configure Multi-Node Celery Airflow
14. Airflow Best Practices

## 🛠️ Tech Stack
- Apache Airflow 3.1 & 2.3
- Python
- WSL2 (Ubuntu)
- Docker & Docker Compose
- Celery
- Redis
- PostgreSQL
- MySQL
- Apache Hadoop
- Apache Spark

## ✅ Prerequisites
- Windows 10/11 with WSL2 enabled
- Docker Desktop (for the Docker install and the Redis broker)
- Git Bash and VS Code (used in the Docker guide; optional)
- Apache Hadoop & Apache Spark installed on Windows (for the integration guides)
- MySQL Server on Windows (for the multi-node cluster)

## 📁 Repository Structure
```
Apache-Airflow-Zero-To-Cluster-On-Windows/
├── README.md
├── LICENSE
├── docs/
│   ├── Airflow_Overview_Setup_part1.pdf
│   └── Airflow_Overview_Setup_part2.pdf
├── Install-Airflow-3.1-using-WSL-on-Windows.md
├── Install-Airflow-2.3-using-Docker-on-Windows.md
├── Airflow-Docker-Variables.md
├── Airflow-CLI.md
├── Create-New-DAG.md
├── Airflow-REST-API.md
├── Integrate-Airflow-with-Apache-Hadoop.md
├── Integrate-Airflow-with-Apache-Spark.md
├── Configure-Multi-Node-Celery-Airflow.md
└── Airflow-Best-Practices.md
```

## 📝 License
This project is licensed under the [MIT License](LICENSE).
