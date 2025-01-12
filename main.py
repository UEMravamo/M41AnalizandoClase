from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, count, collect_list, avg, concat, lit, stddev
from pyspark.sql.functions import round
from pyspark.sql import Window
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

#Estadísticas acumuladas
df_filtrado_asignatura = df.filter((col("asignatura") == asignatura) & (to_date(col("timestamp")) <= fecha))
df_columna_fecha = df_filtrado_asignatura.withColumn("fecha", to_date(col("timestamp")))

contador_asistencia = df_columna_fecha.groupBy("estado_asistencia", "fecha").count()
#Media por estado asistencia
media_asistencia = contador_asistencia.groupBy("estado_asistencia").agg(avg("count").alias("medias"))
#Media ajuste para json
media_asistencia_json = media_asistencia.withColumn("estado_asistencia", concat(lit("Media_"), col("estado_asistencia")))
media_asistencia_json = media_asistencia_json.withColumn("medias", round(col("medias"), 2)).collect()
#Desviacion estándar por estado asistencia
desviacion_tipica_asistencia = contador_asistencia.groupBy("estado_asistencia").agg(stddev("count").alias("desviaciones"))
#Desviacion estándar ajuste para json
desviacion_tipica_asistencia_json = desviacion_tipica_asistencia.withColumn("estado_asistencia", concat(lit("Desviacion_"), col("estado_asistencia")))
desviacion_tipica_asistencia_json = desviacion_tipica_asistencia_json.withColumn("desviaciones", round(col("desviaciones"), 2)).collect()

#Forecast Día Siguiente
window = Window.partitionBy("estado_asistencia").orderBy("fecha").rowsBetween(-2, 0)

pronostico_asistencia = contador_asistencia.withColumn("media_movil", avg("count").over(window))

pronostico_asistencia_dia_siguiente = pronostico_asistencia.filter(to_date(col("fecha")) == fecha)

data = {
    "asignatura": asignatura,
    "totales_dia_actual": {
        **{row["estado_asistencia"]: row["count"] for row in contadores_asistencia},
        "alumnos_por_estado": {
            row["estado_asistencia"]: row["collect_list(alumno)"] for row in alumnos_por_estado
        }
    },
    "estadisticas_acumuladas": {
        **{row["estado_asistencia"]: row["medias"] for row in media_asistencia_json},
        **{row["estado_asistencia"]: row["desviaciones"] for row in desviacion_tipica_asistencia_json},
    }
}

nombre_archivo = f"metricas_{fecha}_{asignatura}.json"

with open(nombre_archivo, "w") as archivo_json:
    json.dump(data, archivo_json, indent=4)

spark.stop()