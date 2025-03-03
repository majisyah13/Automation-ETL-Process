from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

def extract(path):
    spark = SparkSession.builder \
        .appName("WriteToPostfres") \
        .getOrCreate()
    
    data = spark.read.csv(path, header=True, inferSchema=True)
    return data


def validate_data(data):
    # Ensure that int64 columns have values within the correct range (0-24 for hours columns)
    hours_columns = ["Hours_Studied", "Sleep_Hours"]
    for col_name in hours_columns:
        data = data.withColumn(
            col_name, when(col(col_name) < 0, 0).when(col(col_name) > 24, 24).otherwise(col(col_name))
        )

    # Ensure that categorical columns have values within the allowed categories
    act_columns = ["Extracurricular_Activities", "Internet_Access", "Learning_Disabilities"]
    allowed_values_act = ["Ya", "Tidak"]
    for col_name in act_columns:
        data = data.withColumn(
            col_name, when(col(col_name).isin(allowed_values_act), col(col_name)).otherwise("None")
        )

    # Ensure that score columns have values within the range 0-100
    score_columns = ["Previous_Scores", "Exam_Score"]
    for col_name in score_columns:
        data = data.withColumn(
            col_name, when(col(col_name) < 0, 0).when(col(col_name) > 100, 100).otherwise(col(col_name))
        )

    # Validate object columns with "High", "Medium", "Low" categories
    object_columns = ["Parental_Involvement", "Access_to_Resources", "Motivation_Level", "Teacher_Quality"]
    allowed_values_object = ["High", "Medium", "Low"]
    for col_name in object_columns:
        data = data.withColumn(
            col_name, when(col(col_name).isin(allowed_values_object), col(col_name)).otherwise("None")
        )

    # Validate school type column with "Private" and "Public" categories
    sch_columns = ["School_Type"]
    allowed_values_sch = ["Private", "Public"]
    for col_name in sch_columns:
        data = data.withColumn(
            col_name, when(col(col_name).isin(allowed_values_sch), col(col_name)).otherwise("None")
        )

    # Validate parental education level column with specified categories
    par_columns = ["Parental_Education_Level"]
    allowed_values_par = ["High School", "Graduate", "Post Graduate"]
    for col_name in par_columns:
        data = data.withColumn(
            col_name, when(col(col_name).isin(allowed_values_par), col(col_name)).otherwise("None")
        )

    return data

def transform(data):
    data_cleaned = validate_data(data)
    df_cleaned = data_cleaned.toPandas()
    df_cleaned.to_csv('/opt/airflow/logs/data_staging/data_cleaned.csv')
    return df_cleaned

if __name__ == "__main__":
    data_path = '/opt/airflow/dags/StudentPerformanceFactors.csv'
    data_extract = extract(data_path)
    cleaned_data = validate_data(data_extract)
    transform(cleaned_data)