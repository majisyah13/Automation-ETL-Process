
## Description
This project is creating data pipeline automation, to automate the Extract, Transform, Load (ETL) process using Apache Airflow. The project runs three separate nodes to extract, transform, and load students performance factors.
Dataset: https://www.kaggle.com/datasets/lainguyn123/student-performance-factors

## Project Structure
- **dags/dags.py**: The main file that contains the DAG (Directed Acyclic Graph) definitions for the ETL process.
- **extract.py**: A script to extract data from the source and transform the extracted data.
- **load.py**: A script to load data into the target system.

## Prerequisites
Before running this project, make sure you have installed:
- Python
- Apache Airflow

You can install Apache Airflow with the following command:
```bash
pip install apache-airflow
```

## How to Use
1. **Install Airflow**: 
   - Follow the installation instructions in the [official Airflow documentation](https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html).

2. **Configure DAG**: 
   - Adjust the parameters in the `dags.py` file according to your needs, including the `path` for the extraction, transformation, and loading scripts.

3. **Run Airflow**: 
   - Start the Airflow scheduler and web server with the command:
   ```bash
   airflow scheduler
   airflow webserver
   ```

4. **Access Airflow UI**: 
   - Open a browser and access the Airflow UI at `http://localhost:8080` to monitor and manage the DAG.

5. **Schedule DAG**: 
   - The DAG will run automatically according to the specified schedule.

## Example Usage
Once the DAG is scheduled, the ETL process will run automatically. You can monitor the status of each task (extract, transform, load) through the Airflow UI.

## Results
After the ETL process is complete, student performance data will be available in the target system specified in the `load.py` script (insert into MongoDB).
