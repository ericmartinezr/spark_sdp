# 🚀 Spark Declarative Pipelines: Análisis del Censo 2024


## 📌 Descripción del Proyecto

Este repositorio sirve como un entorno de aprendizaje práctico para dominar **Apache Spark Declarative Pipelines (SDP)**. Utilizando datos del Censo 2024 (`hogares_censo2024.parquet`), el proyecto demuestra cómo definir, gestionar y ejecutar flujos de trabajo de datos mediante un enfoque declarativo y nativo de Python, en lugar de la programación imperativa tradicional de Spark.

Spark Declarative Pipelines simplifica la ingeniería de datos al permitir que los desarrolladores se enfoquen en *qué* transformaciones deben aplicarse a los datos, delegando a la plataforma el *cómo* se orquesta y ejecuta el grafo de dependencias.

---

## 🏗️ Arquitectura del Pipeline (`pipeline1`)

La lógica central de este proyecto se encuentra en el archivo `pipeline1/transformations/hogares_censo2024.py`. El pipeline está estructurado en tres capas o pasos lógicos, utilizando decoradores específicos de SDP para controlar la persistencia de los datos y el comportamiento de la ejecución.

### 1. Ingesta de Datos: `@dp.materialized_view`

```python
@dp.materialized_view(name="hogares")
def hogares_censo2024() -> DataFrame:
    df = spark.read.parquet("data/hogares_censo2024.parquet")
    return df
```

**¿Por qué usar `@dp.materialized_view` en lugar de `@dp.table`?**  
En Spark Declarative Pipelines, el decorador `@dp.table` está diseñado y optimizado principalmente para **fuentes de datos en streaming** (por ejemplo, colas de Kafka, Auto Loader o flujos de Structured Streaming). Dado que nuestros datos de origen provienen de un archivo Parquet estático utilizado para pruebas y desarrollo, el uso de `@dp.table` generaría incompatibilidades al esperar un origen continuo. 

Por lo tanto, utilizamos `@dp.materialized_view`. Este decorador es ideal para lecturas por lotes (batch) utilizando operaciones estándar como `spark.read...`, asegurando que los datos se carguen y se materialicen físicamente para que las tareas posteriores puedan consultarlos de manera eficiente.

### 2. Transformación Intermedia: `@dp.temporary_view`

```python
@dp.temporary_view(name="hogares_per_region_view")
def hogares_per_region() -> DataFrame:
    df = spark.table("hogares") \
        .groupBy("region") \
        .count() \
        .withColumnRenamed("count", "num_hogares")
    return df
```

**Propósito de la Vista Temporal:**  
Una vez que se ingieren los datos crudos, realizamos agregaciones; en este caso, contar el número de hogares (`num_hogares`) por región. Al utilizar el decorador `@dp.temporary_view`, definimos un paso de transformación intermedio.

* **Eficiencia de Almacenamiento:** Las vistas temporales se evalúan de forma perezosa (lazy evaluation) y **no se persisten** físicamente en el disco. Esta es una excelente práctica para las transformaciones intermedias, ya que ahorra espacio de almacenamiento y reduce las operaciones de entrada/salida (I/O) innecesarias, manteniendo al mismo tiempo el código modular y legible para su reutilización a lo largo del pipeline.

### 3. Materialización del Resultado Final: `@dp.materialized_view`

```python
@dp.materialized_view(name="hogares_per_region")
def hogares_per_region_table() -> DataFrame:
    return spark.sql("SELECT * FROM hogares_per_region_view")
```

**Persistencia de Resultados:**  
En el paso final, consultamos la vista temporal (usando Spark SQL estándar) y empleamos nuevamente `@dp.materialized_view` para persistir los resultados agregados. Esto garantiza que el conjunto de datos analíticos final (`hogares_per_region`) se almacene físicamente como una tabla, optimizada y lista para consultas rápidas por parte de herramientas analíticas o dashboards de Business Intelligence (BI).

---

## 🛠️ Guía de Inicio: Comandos y Uso

Spark Declarative Pipelines incluye una interfaz de línea de comandos (CLI) para gestionar el ciclo de vida del pipeline.

### 1. Inicializar un Nuevo Pipeline

Para generar la estructura de directorios de un nuevo pipeline, ejecuta:

```bash
spark-pipelines init --name pipeline_name
```

*Una vez inicializado, navega al directorio creado:*

```bash
cd pipeline_name
```

### 2. Validar el Pipeline (Dry Run)

Antes de procesar datos reales, es una buena práctica validar la sintaxis del código y verificar el grafo de dependencias sin ejecutar las transformaciones (evitando así costos de procesamiento):

```bash
spark-pipelines dry-run
```

### 3. Ejecutar el Pipeline

Para correr el pipeline y procesar los datos de acuerdo a las especificaciones y la configuración del archivo `pipeline.yml`:

```bash
spark-pipelines run --spec pipeline.yml
```

---

## 📚 Referencias y Recursos Adicionales

1. [Spark Declarative Pipelines Programming Guide](https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html) - Documentación oficial de Apache Spark sobre flujos declarativos.
2. [Introducción a Spark Declarative Pipelines (YouTube)](https://www.youtube.com/watch?v=WNPYEZ7SMSM) - Video explicativo sobre el funcionamiento de SDP.

---

> **Nota:** La documentación de este proyecto fue escrita manualmente en un principio y luego mejorada con asistencia de IA para lograr un nivel profesional.
