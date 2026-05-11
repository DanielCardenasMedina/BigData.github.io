#!/usr/bin/env python3
"""
spark_streaming_smartlogis.py
=============================
Spark Structured Streaming para SmartLogis AA4.
Consume eventos de Kafka, aplica reglas de alerta,
genera resúmenes y persiste en MongoDB.

Ejecutar desde spark-master:
  spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.mongodb.spark:mongo-spark-connector_2.12:10.2.1 \
    /scripts/spark_streaming_smartlogis.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# ============================================================
# CONFIGURACIÓN
# ============================================================
KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC_NAME = "smartlogis-events"
MONGO_URI = "mongodb://mongodb:27017/smartlogis_db"
CHECKPOINT_DIR = "/tmp/spark-checkpoints"

spark = SparkSession.builder \
    .appName("SmartLogis-Streaming") \
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
    .config("spark.mongodb.output.uri", MONGO_URI) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("SMARTLOGIS - SPARK STRUCTURED STREAMING")
print("=" * 60)

# ============================================================
# 1. LECTURA DESDE KAFKA
# ============================================================
print("\n📡 [1] Conectando a Kafka...")

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", TOPIC_NAME) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# El valor viene como bytes, convertir a string
 df_events = df_raw.selectExpr(
    "CAST(key AS STRING) as placa_vehiculo",
    "CAST(value AS STRING) as json_value",
    "timestamp as kafka_timestamp"
)

# Parsear JSON
schema_evento = StructType([
    StructField("evento_id", StringType(), True),
    StructField("tipo_evento", StringType(), True),
    StructField("placa_vehiculo", StringType(), True),
    StructField("latitud", DoubleType(), True),
    StructField("longitud", DoubleType(), True),
    StructField("velocidad_kmh", DoubleType(), True),
    StructField("timestamp", StringType(), True),
    StructField("nivel_alerta", StringType(), True),
    StructField("mensaje", StringType(), True),
    StructField("orden_id", StringType(), True),
    StructField("estado_nuevo", StringType(), True),
    StructField("desviacion_km", DoubleType(), True)
])

df_parsed = df_events.withColumn("data", F.from_json(F.col("json_value"), schema_evento)) \
    .select("placa_vehiculo", "kafka_timestamp", "data.*")

print("✅ Stream de Kafka configurado")

# ============================================================
# 2. REGLAS DE ALERTA
# ============================================================
print("\n🚨 [2] Aplicando reglas de alerta...")

# Regla 1: Velocidad excedida (>100 km/h)
df_alertas_velocidad = df_parsed.filter(
    (F.col("tipo_evento") == "speed_alert") |
    ((F.col("tipo_evento") == "gps_update") & (F.col("velocidad_kmh") > 100))
).withColumn("alerta_tipo", F.lit("VELOCIDAD_EXCEDIDA")) \
 .withColumn("alerta_descripcion",
    F.concat(F.lit("Vehículo "), F.col("placa_vehiculo"),
             F.lit(" a "), F.col("velocidad_kmh"), F.lit(" km/h")))

# Regla 2: Desviación de ruta
df_alertas_ruta = df_parsed.filter(
    F.col("tipo_evento") == "route_deviation"
).withColumn("alerta_tipo", F.lit("DESVIACION_RUTA")) \
 .withColumn("alerta_descripcion",
    F.concat(F.lit("Desviación de "), F.col("desviacion_km"), F.lit(" km detectada")))

# Unir alertas
df_alertas = df_alertas_velocidad.unionByName(df_alertas_ruta, allowMissingColumns=True) \
    .select("evento_id", "placa_vehiculo", "alerta_tipo", "alerta_descripcion",
            "latitud", "longitud", "timestamp", "nivel_alerta") \
    .withColumn("procesado_en", F.current_timestamp())

# ============================================================
# 3. RESÚMENES STREAMING
# ============================================================
print("\n📊 [3] Generando resúmenes por micro-batch...")

# Resumen 1: Eventos por tipo en ventana de 1 minuto
resumen_tipo = df_parsed \
    .withWatermark("kafka_timestamp", "2 minutes") \
    .groupBy(
        F.window("kafka_timestamp", "1 minute"),
        "tipo_evento"
    ) \
    .agg(
        F.count("*").alias("cantidad_eventos"),
        F.countDistinct("placa_vehiculo").alias("vehiculos_afectados")
    ) \
    .withColumn("ventana_inicio", F.col("window.start")) \
    .withColumn("ventana_fin", F.col("window.end")) \
    .drop("window") \
    .withColumn("procesado_en", F.current_timestamp())

# Resumen 2: Alertas por vehículo en ventana de 2 minutos
resumen_alertas = df_alertas \
    .withWatermark("procesado_en", "5 minutes") \
    .groupBy(
        F.window("procesado_en", "2 minutes"),
        "placa_vehiculo",
        "alerta_tipo"
    ) \
    .agg(F.count("*").alias("total_alertas")) \
    .withColumn("ventana_inicio", F.col("window.start")) \
    .withColumn("ventana_fin", F.col("window.end")) \
    .drop("window") \
    .withColumn("procesado_en", F.current_timestamp())

# ============================================================
# 4. SALIDAS (SINKS)
# ============================================================
print("\n💾 [4] Configurando salidas...")

# Salida 1: Todos los eventos a MongoDB
query_eventos = df_parsed.writeStream \
    .format("mongo") \
    .option("uri", MONGO_URI) \
    .option("database", "smartlogis_db") \
    .option("collection", "eventos_streaming") \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .start()

# Salida 2: Alertas a MongoDB
query_alertas = df_alertas.writeStream \
    .format("mongo") \
    .option("uri", MONGO_URI) \
    .option("database", "smartlogis_db") \
    .option("collection", "alertas_streaming") \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .start()

# Salida 3: Resumen por tipo a consola (para demo)
query_resumen_consola = resumen_tipo.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="30 seconds") \
    .start()

# Salida 4: Resumen por tipo a MongoDB
query_resumen_mongo = resumen_tipo.writeStream \
    .format("mongo") \
    .option("uri", MONGO_URI) \
    .option("database", "smartlogis_db") \
    .option("collection", "resumen_eventos") \
    .outputMode("complete") \
    .trigger(processingTime="30 seconds") \
    .start()

print("✅ Queries de streaming iniciadas")
print("\n📋 Queries activas:")
for q in spark.streams.active:
    print(f"  • {q.name}: {q.status}")

print("\n⏳ Esperando eventos... (Ctrl+C para detener)")

# Mantener activo
spark.streams.awaitAnyTermination()
