# Analizando la Asistencia en Clase

Imagina que eres profesor de universidad y te has cansado de que los alumnos lleguen tarde, no asistan o simplemente aparezcan sin avisar. Para solucionar este problema, has decidido implementar un sistema de análisis basado en PySpark que registre y procese la asistencia de todas las asignaturas. Este sistema debe generar estadísticas diarias, analizar patrones de comportamiento y actualizar un forecast diario para predecir el comportamiento de asistencia de los alumnos.

## Formato del archivo de registro

El sistema procesará un archivo de registros que contiene información sobre la asistencia de los alumnos con el siguiente formato:

```
<timestamp> <asignatura> <estado_asistencia> <alumno>

2024-11-17 08:00:00 GrandesVolumenesDeDatos presente JuanPerez 
2024-11-17 08:15:00 InteligenciaArtificial tarde AnaGarcia 
2024-11-17 09:00:00 BasesDeDatos ausente CarlosLopez 
2024-11-17 10:00:00 GrandesVolumenesDeDatos tarde LuciaMartinez 
```

---

## Objetivos

1. **Procesamiento Diario**:
   - Leer y procesar los registros de asistencia para cada asignatura.
   - Calcular:
     - Número total de alumnos presentes, tarde y ausentes.
     - Lista de alumnos clasificados por estado de asistencia.

2. **Estadísticas**:
   - Calcular la media y la desviación típica de asistencia acumuladas por asignatura y estado.

3. **Forecast Diario**:
   - Generar un pronóstico actualizado diariamente del número esperado de alumnos en cada categoría para el siguiente día, basado en datos históricos (por ejemplo, media móvil).

4. **Eficiencia**:
   - Manejar grandes volúmenes de datos usando PySpark y sus capacidades distribuidas.

---

## Entregables

1. **Código fuente en PySpark**:
   - Programa principal que procese los registros y genere las métricas requeridas.
   - Función que calcule y actualice el forecast basado en datos acumulados.

2. **Resultados**:
   - Archivo con las métricas diarias y el forecast (en formato CSV o JSON).

---

## Pasos Recomendados

1. **Preparación de Datos**:
   - Crea un DataFrame de PySpark para los registros, con las siguientes columnas:
     - `timestamp` (fecha y hora).
     - `asignatura` (nombre de la asignatura).
     - `estado_asistencia` (presente, tarde, ausente).
     - `alumno` (identificador único).

2. **Cálculos de Métricas**:
   - Usa transformaciones como `groupBy` y `agg` para calcular los totales diarios.
   - Calcula la media y la desviación estándar de cada categoría usando las funciones de PySpark.

3. **Generación de Forecast**:
   - Implementa un modelo simple (como media móvil) para calcular el pronóstico de asistencia.
   - Actualiza el forecast diariamente al incorporar nuevos datos.

4. **Persistencia de Resultados**:
   - Guarda las métricas y predicciones en un archivo CSV o JSON.

---

## Ejemplo de Salida

### Archivo JSON de Métricas Diarias

```json
{
  "asignatura": "GrandesVolumenesDeDatos",
  "totales_dia_actual": {
    "presentes": 30,
    "tarde": 5,
    "ausentes": 10,
    "alumnos_por_estado": {
      "presentes": ["JuanPerez", "AnaGarcia", "CarlosLopez"],
      "tarde": ["LuciaMartinez", "PedroGomez"],
      "ausentes": ["MariaFernandez"]
    }
  },
  "estadisticas_acumuladas": {
    "media_presentes": 27,
    "desviacion_presentes": 3.2,
    "media_tarde": 6,
    "desviacion_tarde": 1.5,
    "media_ausentes": 12,
    "desviacion_ausentes": 2.1
  },
  "forecast_dia_siguiente": {
    "presentes": 28,
    "tarde": 6,
    "ausentes": 11
  }
}
```
## Bonus Points
* Pronóstico Avanzado:
Usa algoritmos como regresión lineal o ARIMA para mejorar el forecast.
* Modo en Tiempo Real:
Implementa un flujo continuo con PySpark Streaming para procesar nuevos registros mientras se generan.

---

## Requisitos

- Usa Python 3.7.
- Escribe código conforme a PEP8.
- Escribe algunas pruebas (considera usar pytest o uniitest).
- Documenta tu solución en un archivo
