import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, avg


@pytest.fixture(scope="module")
def sesion_spark():
    """Crear una sesión de Spark para pruebas."""
    return SparkSession.builder.appName("Test Analizando la Asistencia en Clase").master("local[1]").getOrCreate()


@pytest.fixture(scope="module")
def datos_prueba(sesion_spark):
    """Datos de muestra para las pruebas."""
    data = [
        {"timestamp": "2024-12-01 08:00:00", "estado_asistencia": "Presente",
         "asignatura": "Historia", "alumno": "alumno_48"},
        {"timestamp": "2024-12-01 08:30:00", "estado_asistencia": "Ausente",
         "asignatura": "Historia", "alumno": "alumno_38"},
        {"timestamp": "2024-11-30 09:00:00", "estado_asistencia": "Presente",
         "asignatura": "Historia", "alumno": "alumno_9"},
        {"timestamp": "2024-11-29 10:00:00", "estado_asistencia": "Tarde",
         "asignatura": "Historia", "alumno": "alumno_15"},
    ]
    return sesion_spark.createDataFrame(data)


def test_filtro_por_fecha_y_asignatura(sesion_spark, datos_prueba):
    """Prueba el filtrado por fecha y asignatura."""
    fecha = "2024-12-01"
    asignatura = "Historia"

    df_filtrado = datos_prueba.filter(
        (to_date(col("timestamp")) == fecha) & (col("asignatura") == asignatura)
    )

    assert df_filtrado.count() == 2


def test_agrupacion_por_estado(sesion_spark, datos_prueba):
    """Prueba la agrupación por estado de asistencia."""
    resultados = datos_prueba.groupBy("estado_asistencia").count().collect()

    estado_dict = {row["estado_asistencia"]: row["count"] for row in resultados}
    assert estado_dict["Presente"] == 2
    assert estado_dict["Ausente"] == 1
    assert estado_dict["Tarde"] == 1


def test_calculo_media_asistencia(sesion_spark, datos_prueba):
    """Prueba el cálculo de la media de asistencia."""
    df_columna_fecha = datos_prueba.withColumn("fecha", to_date(col("timestamp")))
    contador_asistencia = df_columna_fecha.groupBy(
        "estado_asistencia", "fecha"
    ).count()

    media_asistencia = contador_asistencia.groupBy("estado_asistencia").avg("count")

    resultados = {
        row["estado_asistencia"]: row["avg(count)"]
        for row in media_asistencia.collect()
    }
    assert resultados["Presente"] == 1.0
    assert resultados["Ausente"] == 1.0
    assert resultados["Tarde"] == 1.0
