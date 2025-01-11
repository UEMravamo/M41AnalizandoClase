from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, count, collect_list, avg
import json

spark = SparkSession.builder.appName("Analizando la Asistencia en Clase").getOrCreate()

df = spark.read.csv("asistencia.csv", header=True, inferSchema=True)

#Parámetros
fecha = "2024-12-01"
asignatura = "Historia"

df_filtrado_dia_asignatura = df.filter((to_date(col("timestamp")) == fecha) & (col("asignatura") == asignatura))

#Totales día actual
#Cantidad por estado asistencia
contadores_asistencia = df_filtrado_dia_asignatura.groupBy("estado_asistencia").count().collect()
#Alumnos por estado asistencia
alumnos_por_estado = df_filtrado_dia_asignatura.groupBy("estado_asistencia").agg(collect_list("alumno")).collect()

data = {
    "asignatura": "Historia",
    "totales_dia_actual": {
        **{row["estado_asistencia"]: row["count"] for row in contadores_asistencia},
        "alumnos_por_estado": {
            row["estado_asistencia"]: row["collect_list(alumno)"] for row in alumnos_por_estado
        }
    }
}

nombre_archivo = f"metricas_{fecha}_{asignatura}.json"

with open(nombre_archivo, "w") as archivo_json:
    json.dump(data, archivo_json, indent=4)

spark.stop()