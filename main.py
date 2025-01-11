from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, count, collect_list, avg, concat, lit
from pyspark.sql.functions import round
import json

spark = SparkSession.builder.appName("Analizando la Asistencia en Clase").getOrCreate()

df = spark.read.csv("asistencia.csv", header=True, inferSchema=True)

#Parámetros
fecha = "2024-12-02"
asignatura = "Historia"

df_filtrado_dia_asignatura = df.filter((to_date(col("timestamp")) == fecha) & (col("asignatura") == asignatura))

#Totales día actual
#Cantidad por estado asistencia
contadores_asistencia = df_filtrado_dia_asignatura.groupBy("estado_asistencia").count().collect()
#Alumnos por estado asistencia
alumnos_por_estado = df_filtrado_dia_asignatura.groupBy("estado_asistencia").agg(collect_list("alumno")).collect()

#Estadísticas acumuladas
#Media por estado asistencia
df_filtrado_asignatura = df.filter((col("asignatura") == asignatura) & (to_date(col("timestamp")) <= fecha))
df_columna_fecha = df_filtrado_asignatura.withColumn("fecha", to_date(col("timestamp")))

contador_asistencia = df_columna_fecha.groupBy("estado_asistencia", "fecha").count()

media_asistencia = contador_asistencia.groupBy("estado_asistencia").agg(avg("count").alias("medias"))

#Ajuste para json
media_asistencia_json = media_asistencia.withColumn("estado_asistencia", concat(lit("Media_"), col("estado_asistencia")))
media_asistencia_json = media_asistencia.withColumn("medias", round(col("medias"), 2)).collect()

data = {
    "asignatura": asignatura,
    "totales_dia_actual": {
        **{row["estado_asistencia"]: row["count"] for row in contadores_asistencia},
        "alumnos_por_estado": {
            row["estado_asistencia"]: row["collect_list(alumno)"] for row in alumnos_por_estado
        }
    },
    "estadisticas_acumuladas": {
        row["estado_asistencia"]: row["medias"] for row in media_asistencia_json
    }
}

nombre_archivo = f"metricas_{fecha}_{asignatura}.json"

with open(nombre_archivo, "w") as archivo_json:
    json.dump(data, archivo_json, indent=4)

spark.stop()