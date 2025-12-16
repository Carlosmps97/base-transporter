# Databricks notebook source
import json
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, substring

# COMMAND ----------

def load_log_analytics_json(spark, path: str):
    """
    Lee un archivo JSON exportado desde Azure Log Analytics Workspace.
    Todas las columnas se leen como STRING para la capa Bronze.
    """
    # 1. Leer el archivo como texto completo
    file_content = spark.read.text(path, wholetext=True).collect()[0][0]
    
    # 2. Remover BOM si existe
    if file_content.startswith('\ufeff'):
        file_content = file_content[1:]
    
    # 3. Parsear JSON
    data = json.loads(file_content)
    
    # 4. Extraer tabla
    table = data['tables'][0]
    columns = table['columns']
    rows = table['rows']
    
    # 5. Construir esquema - TODAS las columnas como STRING
    col_names = [c['name'] for c in columns]
    schema = StructType([StructField(name, StringType(), True) for name in col_names])
    
    # 6. Convertir valores a string (manejar None y otros tipos)
    rows_as_strings = []
    for row in rows:
        string_row = []
        for val in row:
            if val is None:
                string_row.append(None)
            elif isinstance(val, bool):
                string_row.append(str(val).lower())  # true/false
            else:
                string_row.append(str(val))
        rows_as_strings.append(string_row)
    
    # 7. Crear DataFrame
    df = spark.createDataFrame(rows_as_strings, schema=schema)
    
    return df

# COMMAND ----------

# Configuración
storage_account_name = "adlsagentslab01"
container_name = "agentsdata"
file_path = "data/AppRequests.json"

abfss_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{file_path}"

# Cargar datos
try:
    df_requests = load_log_analytics_json(spark, abfss_path)
    df_requests = df_requests.withColumn("date_routine", substring(col("TimeGeneratedUTCMinus5"), 1, 10))
    print(f"✅ Carga exitosa: {df_requests.count()} registros, {len(df_requests.columns)} columnas")
    display(df_requests)
    df_requests.printSchema()
    
except Exception as e:
    print(f"❌ Error: {e}")

# COMMAND ----------

df_requests.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("date_routine") \
    .saveAsTable("hive_metastore.bronze.app_requests")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ---
# MAGIC ---
# MAGIC ---

# COMMAND ----------

# Configuración
storage_account_name = "adlsagentslab01"
container_name = "agentsdata"
file_path = "data/AppTraces.json"

abfss_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{file_path}"

# Cargar datos
try:
    df_traces = load_log_analytics_json(spark, abfss_path)
    df_traces = df_traces.withColumn("date_routine", substring(col("TimeGeneratedUTCMinus5"), 1, 10))
    print(f"✅ Carga exitosa: {df_traces.count()} registros, {len(df_traces.columns)} columnas")
    display(df_traces)
    df_traces.printSchema()
    
except Exception as e:
    print(f"❌ Error: {e}")

# COMMAND ----------

df_traces.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("date_routine") \
    .saveAsTable("hive_metastore.bronze.app_traces")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ---
# MAGIC ---
# MAGIC ---

# COMMAND ----------

# Configuración
storage_account_name = "adlsagentslab01"
container_name = "agentsdata"
file_path = "data/AppDependencies.json"

abfss_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{file_path}"

# Cargar datos
try:
    df_dependencies = load_log_analytics_json(spark, abfss_path)
    df_dependencies = df_dependencies.withColumn("date_routine", substring(col("TimeGeneratedUTCMinus5"), 1, 10))
    print(f"✅ Carga exitosa: {df_dependencies.count()} registros, {len(df_dependencies.columns)} columnas")
    display(df_dependencies)
    df_dependencies.printSchema()
    
except Exception as e:
    print(f"❌ Error: {e}")

# COMMAND ----------

df_dependencies.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("date_routine") \
    .saveAsTable("hive_metastore.bronze.app_dependencies")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ---
# MAGIC ---

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, TimestampType, IntegerType

# =============================================================================
# 1. CONFIGURACIÓN Y CARGA (CAPA BRONZE)
# =============================================================================

# Nombres de tus tablas en el catálogo (Ajusta si el catálogo no es hive_metastore)
TABLE_REQUESTS = "hive_metastore.bronze.app_requests"
TABLE_DEPENDENCIES = "hive_metastore.bronze.app_dependencies"
TABLE_TRACES = "hive_metastore.bronze.app_traces"

# Cargar DataFrames
df_req = spark.table(TABLE_REQUESTS)
df_dep = spark.table(TABLE_DEPENDENCIES)
df_traces = spark.table(TABLE_TRACES)

# =============================================================================
# 2. TRANSFORMACIÓN: CREACIÓN DEL "MASTER SPAN TABLE" (CAPA SILVER)
# =============================================================================
# El objetivo es unificar Requests y Dependencies para reconstruir el árbol de trazas
# y extraer los atributos que tu read_data.py usaba.

# A. Normalizar columnas para la Unión
# Requests son el "Server Side" (Entrada), Dependencies son "Client/Internal" (Salida/Internal)

# Selección de Requests
silver_req = df_req.select(
    F.col("TimeGenerated").cast(TimestampType()).alias("timestamp"),
    F.col("OperationId").alias("trace_id"),
    F.col("Id").alias("span_id"),
    F.col("ParentId").alias("parent_span_id"),
    F.col("Name").alias("name"),
    F.lit("SERVER").alias("kind"),
    F.col("DurationMs").cast(DoubleType()).alias("duration_ms"),
    F.col("Success").alias("success"),
    F.col("Properties")
)

silver_dep = df_dep.select(
    F.col("TimeGenerated").cast(TimestampType()).alias("timestamp"),
    F.col("OperationId").alias("trace_id"),
    F.col("Id").alias("span_id"),
    F.col("ParentId").alias("parent_span_id"),
    F.col("Name").alias("name"),
    F.col("DependencyType").alias("kind"),
    F.col("DurationMs").cast(DoubleType()).alias("duration_ms"),
    F.col("Success").alias("success"),
    F.col("Properties")
)

# B. Unir todo en un solo DataFrame de Spans
df_spans_raw = silver_req.unionByName(silver_dep)

# C. EXTRACCIÓN DE ATRIBUTOS (La magia que reemplaza a 'extract_attribute')
# Usamos get_json_object para sacar los campos del string JSON 'Properties'
# Nota: Azure Log Analytics a veces usa notación plana en el JSON.

df_spans = df_spans_raw.withColumn("user_id", F.get_json_object("Properties", "$['user.id']")) \
    .withColumn("thread_id", F.coalesce(
        F.get_json_object("Properties", "$['gen_ai.thread.id']"),
        F.get_json_object("Properties", "$['thread.id']"),
        F.get_json_object("Properties", "$['azure.thread_id']")
    )) \
    .withColumn("run_id", F.coalesce(
        F.get_json_object("Properties", "$['gen_ai.thread.run.id']"),
        F.get_json_object("Properties", "$['run.id']"), # A veces viene directo
        F.get_json_object("Properties", "$['azure.run_id']")
    )) \
    .withColumn("tool_name", F.get_json_object("Properties", "$['tool.name']")) \
    .withColumn("tool_input", F.get_json_object("Properties", "$['tool.input_full']")) \
    .withColumn("tool_output", F.get_json_object("Properties", "$['tool.output_full']")) \
    .withColumn("tokens_total", F.coalesce(
        F.get_json_object("Properties", "$['gen_ai.usage.total_tokens']"),
        F.get_json_object("Properties", "$['tokens.total']")
    ).cast(IntegerType())) \
    .withColumn("message_preview", F.coalesce(
        F.get_json_object("Properties", "$['message.preview']"),
        F.get_json_object("Properties", "$['agent.response.preview']")
    ))

# Cachear para rendimiento ya que consultaremos varias veces
df_spans.cache()

# =============================================================================
# 3. ANÁLISIS Y MÉTRICAS (Replicando read_data.py)
# =============================================================================

print("="*80)
print("🔍 REPORTE DE TELEMETRÍA (DATABRICKS EDITION)")
print("="*80)

# --- 1. USUARIOS ---
print("\n1. USUARIOS ACTIVOS")
df_users = df_spans.filter(F.col("name") == "POST /chat") \
    .groupBy("user_id").count().withColumnRenamed("count", "total_requests")
display(df_users)

# --- 2. CONVERSACIONES (THREADS) Y COSTOS ---
print("\n2. RESUMEN DE CONVERSACIONES (THREADS)")
# Agrupamos por thread para sumar tokens y contar interacciones
df_threads = df_spans.filter(F.col("thread_id").isNotNull()) \
    .groupBy("thread_id") \
    .agg(
        F.count(F.when(F.col("name") == "POST /chat", 1)).alias("interacciones"),
        F.sum("tokens_total").alias("tokens_consumidos"),
        F.max("timestamp").alias("ultimo_mensaje")
    ).orderBy(F.col("ultimo_mensaje").desc())

display(df_threads)

# --- 3. DETALLE DE INTERACCIONES (CHAT) ---
print("\n3. DETALLE DE INTERACCIONES (CHAT FLOW)")
# Queremos ver: Usuario -> Mensaje -> Latencia -> Tokens
df_interactions = df_spans.filter(F.col("name") == "POST /chat") \
    .select(
        "timestamp", 
        "user_id", 
        "thread_id", 
        "duration_ms", 
        "message_preview",
        "tokens_total" # Nota: A veces los tokens están en el span 'run_agent' hijo, no en el padre.
                       # Si ves nulos aquí, haremos un join con el span hijo en el paso avanzado.
    ).orderBy(F.col("timestamp").desc())

display(df_interactions)

# --- 4. EJECUCIÓN DE TOOLS (ANÁLISIS TÉCNICO) ---
print("\n4. EJECUCIÓN DE TOOLS (DETALLE TÉCNICO)")
# Filtramos solo las llamadas a tools para ver qué está pasando por debajo
df_tools = df_spans.filter(F.col("name").like("tool.%")) \
    .select(
        "timestamp",
        "trace_id",
        "name",
        "duration_ms",
        "tool_input",
        "tool_output",
        "success"
    ).orderBy(F.col("timestamp").desc())

display(df_tools)

# --- 5. LOGS DE AUDITORÍA DE NEGOCIO (Desde AppTraces) ---
print("\n5. LOGS DE AUDITORÍA (AUDIT.BUSINESS)")
# Tu read_data.py leía logs separados. En Azure están en AppTraces.
# Buscamos los logs que enviaste con tu logger 'audit.business'
# Azure guarda el JSON del log en la columna 'Message'

df_audit = df_traces.filter(F.col("Message").contains("audit.business")) \
    .select(
        F.col("TimeGenerated").alias("timestamp"),
        F.col("OperationId").alias("trace_id"),
        # Parsear el mensaje JSON completo que guardaste
        F.get_json_object("Message", "$.user_id").alias("user_id"),
        F.get_json_object("Message", "$.conversation.input").alias("input_completo"),
        F.get_json_object("Message", "$.conversation.output").alias("output_completo"),
        F.get_json_object("Message", "$.metrics.total_tokens").alias("tokens")
    ).orderBy(F.col("timestamp").desc())

display(df_audit)

# COMMAND ----------

display(silver_dep)

# COMMAND ----------

from pyspark.sql.window import Window
w = Window.partitionBy("trace_id").orderBy(F.asc("timestamp")) \
          .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

# Propagar user_id y thread_id a todos los spans del mismo trace
df_spans_enriched = df_spans \
    .withColumn("user_id_filled", F.first("user_id", ignorenulls=True).over(w)) \
    .withColumn("thread_id_filled", F.first("thread_id", ignorenulls=True).over(w))

# Mostrar resultados limpios
print("🔍 Muestra de Spans Enriquecidos (Sin Nulls en IDs clave)")
display(df_spans_enriched.select("timestamp", "name", "kind", "user_id_filled", "thread_id_filled", "tool_name"))