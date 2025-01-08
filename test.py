from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, collect_list, to_date, lit

# Crear la sesión de Spark
spark = SparkSession.builder.appName("PreparacionDeDatos").getOrCreate()

df = spark.read.csv("asistencia.csv", header=True, inferSchema=True)

# Convertir `timestamp` a formato fecha
df = df.withColumn("fecha", to_date(col("timestamp")))

# Filtrar por la fecha actual (hoy)
fecha = "2024-11-22"
df_fecha = df.filter(col("fecha") == lit(fecha))

# Mostrar el DataFrame
df_fecha.show()

# Cálculo del número total de alumnos por estado de asistencia
estado_counts = df_fecha.groupBy("estado_asistencia").agg(count("alumno").alias("total"))
estado_counts.show()

# Lista de alumnos clasificados por estado de asistencia
alumnos_por_estado = df_fecha.groupBy("estado_asistencia").agg(collect_list("alumno").alias("alumnos"))
alumnos_por_estado.show()