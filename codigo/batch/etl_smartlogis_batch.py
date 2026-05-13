#!/usr/bin/env python3
"""
=======================
Script ETL batch principal del ecosistema SmartLogis AA4.
Procesa datos históricos con Spark: RDD, DataFrames y Spark SQL.
Carga resultados a MongoDB.

Ejecutar desde spark-master:
  spark-submit \
    --master spark://spark-master:7077 \
    --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.1 \
    /scripts/etl_smartlogis_batch.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, BooleanType, TimestampType, DateType
)
from pyspark import SparkContext
import json

# ============================================================
# CONFIGURACIÓN
# ============================================================
HDFS_BASE = "hdfs://namenode:9000/data/smartlogis"
MONGO_URI = "mongodb://mongodb:27017/smartlogis_db"

spark = SparkSession.builder \
    .appName("SmartLogis-Batch-ETL") \
    .config("spark.mongodb.output.uri", MONGO_URI) \
    .config("spark.mongodb.input.uri", MONGO_URI) \
    .getOrCreate()

sc = spark.sparkContext

print("=" * 60)
print("SMARTLOGIS - PROCESAMIENTO BATCH")
print("=" * 60)

# ============================================================
# 1. LECTURA DE ARCHIVOS
# ============================================================
print("\n📥 [1] LECTURA DE ARCHIVOS DESDE HDFS")

# --- Esquemas explícitos (Mejor práctica Big Data) ---
schema_vehiculos = StructType([
    StructField("placa", StringType(), False),
    StructField("modelo", StringType(), True),
    StructField("capacidad_kg", IntegerType(), True),
    StructField("conductor_id", StringType(), True),
    StructField("ano_fabricacion", IntegerType(), True),
    StructField("tipo_combustible", StringType(), True),
    StructField("activo", BooleanType(), True)
])

schema_entregas = StructType([
    StructField("orden_id", StringType(), False),
    StructField("placa_vehiculo", StringType(), False),
    StructField("origen", StringType(), True),
    StructField("destino", StringType(), True),
    StructField("peso_kg", DoubleType(), True),
    StructField("fecha_entrega", StringType(), True),
    StructField("estado", StringType(), True),
    StructField("duracion_min", StringType(), True),  # String temporal para limpieza
    StructField("distancia_km", DoubleType(), True),
    StructField("cliente_id", StringType(), True)
])

# Leer CSVs
df_vehiculos = spark.read \
    .option("header", "true") \
    .schema(schema_vehiculos) \
    .csv(f"{HDFS_BASE}/vehiculos.csv")

# Leer entregas con inferencia para manejar campos vacíos
df_entregas_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(f"{HDFS_BASE}/entregas.csv")

# Leer JSONs
df_gps = spark.read.json(f"{HDFS_BASE}/gps_registros.json")
df_incidencias = spark.read.json(f"{HDFS_BASE}/incidencias.json")

print(f"  ✓ Vehículos: {df_vehiculos.count()} registros")
print(f"  ✓ Entregas: {df_entregas_raw.count()} registros")
print(f"  ✓ GPS: {df_gps.count()} registros")
print(f"  ✓ Incidencias: {df_incidencias.count()} registros")

# ============================================================
# 2. LIMPIEZA DE DATOS
# ============================================================
print("\n🧹 [2] LIMPIEZA DE DATOS")

# --- Limpieza Entregas ---
df_entregas = df_entregas_raw \
    .dropDuplicates(["orden_id"]) \
    .withColumn("fecha_entrega", F.to_timestamp("fecha_entrega", "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("duracion_min", 
        F.when(F.col("duracion_min").isNull() | (F.col("duracion_min") == ""), None)
         .otherwise(F.col("duracion_min").cast("int"))) \
    .withColumn("peso_kg", F.when(F.col("peso_kg") > 8000, None).otherwise(F.col("peso_kg"))) \
    .fillna({"estado": "Desconocido", "duracion_min": 0})

# --- Limpieza GPS ---
df_gps_clean = df_gps \
    .dropDuplicates(["registro_id"]) \
    .withColumn("timestamp", F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
    .filter((F.col("velocidad_kmh") >= 0) & (F.col("velocidad_kmh") <= 200)) \
    .filter(F.col("satelites") >= 3)

# --- Limpieza Incidencias ---
df_incidencias_clean = df_incidencias \
    .dropDuplicates(["incidencia_id"]) \
    .withColumn("fecha", F.to_timestamp("fecha", "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
    .withColumn("tiempo_resolucion_min",
        F.when(F.col("resuelta") == False, None).otherwise(F.col("tiempo_resolucion_min")))

print(f"  ✓ Entregas después de limpieza: {df_entregas.count()}")
print(f"  ✓ GPS después de limpieza: {df_gps_clean.count()}")
print(f"  ✓ Incidencias después de limpieza: {df_incidencias_clean.count()}")

# ============================================================
# 3. TRANSFORMACIONES Y CÁLCULOS
# ============================================================
print("\n🔧 [3] TRANSFORMACIONES")

# --- Calcular métricas por vehículo con Spark SQL ---
df_entregas.createOrReplaceTempView("entregas")
df_vehiculos.createOrReplaceTempView("vehiculos")

# KPIs por vehículo usando Spark SQL
kpi_vehiculos = spark.sql("""
    SELECT 
        e.placa_vehiculo,
        COUNT(*) as total_entregas,
        SUM(CASE WHEN e.estado = 'Completado' THEN 1 ELSE 0 END) as entregas_completadas,
        ROUND(SUM(CASE WHEN e.estado = 'Completado' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as tasa_exito_pct,
        ROUND(AVG(e.duracion_min), 2) as duracion_promedio_min,
        ROUND(SUM(e.distancia_km), 2) as km_totales,
        ROUND(AVG(e.peso_kg), 2) as peso_promedio_kg,
        COUNT(DISTINCT e.cliente_id) as clientes_atendidos
    FROM entregas e
    GROUP BY e.placa_vehiculo
""")

# --- Métricas GPS por vehículo ---
df_gps_clean.createOrReplaceTempView("gps")

kpi_gps = spark.sql("""
    SELECT
        placa_vehiculo,
        COUNT(*) as total_registros_gps,
        ROUND(AVG(velocidad_kmh), 2) as velocidad_promedio,
        ROUND(MAX(velocidad_kmh), 2) as velocidad_maxima,
        ROUND(MIN(precision_gps), 2) as mejor_precision,
        ROUND(AVG(satelites), 1) as satelites_promedio
    FROM gps
    GROUP BY placa_vehiculo
""")

# --- Incidencias por vehículo ---
df_incidencias_clean.createOrReplaceTempView("incidencias")

kpi_incidencias = spark.sql("""
    SELECT
        placa_vehiculo,
        COUNT(*) as total_incidencias,
        SUM(CASE WHEN severidad = 'Alta' THEN 1 ELSE 0 END) as incidencias_alta,
        SUM(CASE WHEN severidad = 'Media' THEN 1 ELSE 0 END) as incidencias_media,
        SUM(CASE WHEN severidad = 'Baja' THEN 1 ELSE 0 END) as incidencias_baja,
        SUM(CASE WHEN resuelta = true THEN 1 ELSE 0 END) as incidencias_resueltas,
        ROUND(AVG(costo_estimado_soles), 2) as costo_promedio_soles
    FROM incidencias
    GROUP BY placa_vehiculo
""")

# ============================================================
# 4. INTEGRACIÓN DE FUENTES (JOINS)
# ============================================================
print("\n🔗 [4] INTEGRACIÓN DE FUENTES")

# Reporte consolidado de flota
reporte_flota = kpi_vehiculos \
    .join(kpi_gps, "placa_vehiculo", "left") \
    .join(kpi_incidencias, "placa_vehiculo", "left") \
    .join(df_vehiculos, df_vehiculos.placa == kpi_vehiculos.placa_vehiculo, "left") \
    .select(
        kpi_vehiculos.placa_vehiculo,
        df_vehiculos.modelo,
        df_vehiculos.capacidad_kg,
        df_vehiculos.conductor_id,
        df_vehiculos.tipo_combustible,
        kpi_vehiculos.total_entregas,
        kpi_vehiculos.entregas_completadas,
        kpi_vehiculos.tasa_exito_pct,
        kpi_vehiculos.duracion_promedio_min,
        kpi_vehiculos.km_totales,
        kpi_vehiculos.peso_promedio_kg,
        kpi_vehiculos.clientes_atendidos,
        kpi_gps.total_registros_gps,
        kpi_gps.velocidad_promedio,
        kpi_gps.velocidad_maxima,
        kpi_incidencias.total_incidencias,
        kpi_incidencias.incidencias_alta,
        kpi_incidencias.incidencias_media,
        kpi_incidencias.incidencias_baja,
        kpi_incidencias.incidencias_resueltas,
        kpi_incidencias.costo_promedio_soles,
        F.current_timestamp().alias("fecha_reporte")
    ) \
    .fillna(0)

print(f"  ✓ Reporte de flota generado: {reporte_flota.count()} vehículos")

# ============================================================
# 5. PROCESAMIENTO CON RDD (Archivo LOG)
# ============================================================
print("\n📋 [5] PROCESAMIENTO RDD - EVENTOS DEL SISTEMA")

# Leer archivo log como RDD de líneas de texto
log_rdd = sc.textFile(f"{HDFS_BASE}/eventos_sistema.log")

# Parsear cada línea con map/filter
parsed_rdd = log_rdd.map(lambda line: line.split(" ", 3)) \
    .filter(lambda parts: len(parts) >= 4) \
    .map(lambda parts: {
        "timestamp": parts[0] + " " + parts[1],
        "nivel": parts[2].strip("[]"),
        "componente": parts[3].split("]")[0].strip("["),
        "mensaje": parts[3].split("]", 1)[1].strip() if "]" in parts[3] else parts[3]
    })

# Contar eventos por nivel
nivel_counts = parsed_rdd.map(lambda x: (x["nivel"], 1)).reduceByKey(lambda a, b: a + b).collect()
print(f"  ✓ Eventos por nivel: {dict(nivel_counts)}")

# Contar eventos por componente
comp_counts = parsed_rdd.map(lambda x: (x["componente"], 1)).reduceByKey(lambda a, b: a + b).collect()
print(f"  ✓ Eventos por componente: {dict(comp_counts)}")

# Filtrar solo errores
error_rdd = parsed_rdd.filter(lambda x: x["nivel"] == "ERROR")
error_list = error_rdd.take(10)
print(f"  ✓ Total errores encontrados: {error_rdd.count()}")

# Convertir RDD a DataFrame para guardar en MongoDB
df_logs = spark.createDataFrame(parsed_rdd)

# ============================================================
# 6. CARGA A MONGODB
# ============================================================
print("\n💾 [6] CARGA A MONGODB")

# Colecciones a cargar
collections = [
    (df_vehiculos, "vehiculos"),
    (df_entregas, "entregas"),
    (df_gps_clean, "rutas_gps"),
    (df_incidencias_clean, "incidencias"),
    (reporte_flota, "reporte_flota"),
    (df_logs, "eventos_sistema")
]

for df, coll_name in collections:
    df.write \
        .format("mongo") \
        .mode("overwrite") \
        .option("uri", MONGO_URI) \
        .option("database", "smartlogis_db") \
        .option("collection", coll_name) \
        .save()
    print(f"  ✓ Colección '{coll_name}' cargada ({df.count()} docs)")

# ============================================================
# 7. EXPORTAR RESULTADOS PARA VISUALIZACIÓN
# ============================================================
print("\n📊 [7] EXPORTANDO RESULTADOS PARA VISUALIZACIÓN")

# Guardar reporte como CSV para visualización
reporte_flota.write.mode("overwrite").csv(f"{HDFS_BASE}/output/reporte_flota_csv", header=True)

# Guardar KPIs resumidos como JSON
kpi_resumen = reporte_flota.select(
    "placa_vehiculo", "modelo", "total_entregas", "tasa_exito_pct",
    "km_totales", "velocidad_promedio", "total_incidencias"
).toPandas()

# Nota: En entorno real, guardarías esto. Aquí solo mostramos.
print(f"  ✓ Reporte exportado a HDFS")
print(f"  ✓ KPIs listos para visualización")

print("\n" + "=" * 60)
print("✅ ETL BATCH COMPLETADO EXITOSAMENTE")
print("=" * 60)

spark.stop()
