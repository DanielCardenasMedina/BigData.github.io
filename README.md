# 🚛 SmartLogis - Sistema de Monitoreo y Gestión Logística de Transporte

**Evaluación AA4 - Diseño de Soluciones de Big Data**  
Instituto de Educación Superior Tecnológico Privado CERTUS - 2026

## 📋 Integrantes del Grupo 9

- Daniel Cardenas Medina
- Anderson Luis Ariel Arias Garcia
- Renato Sebastian Torres Rodriguez
- Joao Sebastian Zevallos Ruiz
- Jhon David Rosas Paredes

---

## 🎯 Descripción del Proyecto

SmartLogis es un ecosistema Big Data integral para el monitoreo y optimización de operaciones logísticas de transporte. La solución procesa datos históricos mediante **Apache Spark** (batch), eventos en tiempo real mediante **Apache Kafka** y **Spark Structured Streaming**, y persiste resultados en **MongoDB**, todo desplegado en contenedores **Docker**.

### Evolución desde AA3 hacia AA4

| Componente | AA3 (Batch) | AA4 (Integral) |
|------------|-------------|----------------|
| Procesamiento | Spark Batch | Spark Batch + Streaming |
| Datos | Solo históricos | Históricos + Tiempo real |
| Streaming | No | Kafka + Spark Structured Streaming |
| MongoDB | 5 colecciones | 8+ colecciones (incluye streaming) |
| GitHub | No | Sí, con evidencia colaborativa |
| Visualización | No | 3 gráficos + 5 KPIs |

---

## 🏗️ Arquitectura

```
┌─────────────────────────────────────────────────────────────┐
│                    CAPA DE INGESTA                           │
│  [vehiculos.csv] [entregas.csv] [gps_registros.json]       │
│  [incidencias.json] [eventos_sistema.log]                  │
└──────────────────────┬──────────────────────────────────────┘
                       │ Carga a HDFS
                       ▼
┌─────────────────────────────────────────────────────────────┐
│              HADOOP HDFS (Almacenamiento)                    │
│         hdfs://namenode:9000/data/smartlogis/              │
└──────────────────────┬──────────────────────────────────────┘
                       │ Lectura distribuida
                       ▼
┌─────────────────────────────────────────────────────────────┐
│           APACHE SPARK (Procesamiento Batch)               │
│   RDD (logs) │ DataFrame (CSV/JSON) │ Spark SQL (KPIs)     │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│              APACHE KAFKA (Streaming en tiempo real)       │
│   Topic: smartlogis-events                                  │
│   Eventos: gps_update, speed_alert, route_deviation,       │
│            delivery_status                                  │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│        SPARK STRUCTURED STREAMING (Procesamiento RT)       │
│   Micro-batches │ Alertas │ Resúmenes por ventana          │
└──────────────────────┬──────────────────────────────────────┘
                       │ Escritura
                       ▼
┌─────────────────────────────────────────────────────────────┐
│              MONGODB (Base de Datos Documental)            │
│   smartlogis_db:                                            │
│   ├── vehiculos       ├── entregas      ├── rutas_gps       │
│   ├── incidencias     ├── reporte_flota ├── eventos_sistema │
│   ├── eventos_streaming  ├── alertas_streaming              │
│   └── resumen_eventos                                     │
└──────────────────────┬──────────────────────────────────────┘
                       │ Consultas y reportes
                       ▼
┌─────────────────────────────────────────────────────────────┐
│              VISUALIZACIÓN Y DASHBOARDS                      │
│   Gráficos matplotlib │ KPIs │ Interpretaciones            │
└─────────────────────────────────────────────────────────────┘
```

---

## 📁 Estructura del Proyecto

```
smartlogis_aa4/
├── data/                          # Archivos de entrada (5 archivos, 3 formatos)
│   ├── vehiculos.csv
│   ├── entregas.csv              # Archivo principal (12,000 registros)
│   ├── gps_registros.json
│   ├── incidencias.json
│   └── eventos_sistema.log
├── codigo/
│   ├── generar_datos.py          # Generador de datos sintéticos
│   ├── batch/
│   │   ├── carga_hdfs.sh         # Script de carga a HDFS
│   │   └── etl_smartlogis_batch.py   # ETL batch con Spark
│   ├── streaming/
│   │   ├── kafka_producer_gps.py     # Productor de eventos Kafka
│   │   └── spark_streaming_smartlogis.py  # Consumer Spark Streaming
│   └── visualizacion/
│       └── visualizacion_smartlogis.py    # Gráficos y KPIs
├── docker/
│   └── docker-compose.yml        # Orquestación de contenedores
├── docs/
│   └── visualizaciones/          # Gráficos generados
└── README.md
```

---

## 🚀 Guía de Ejecución

### Paso 1: Levantar el ecosistema

```bash
cd docker
docker-compose up -d
```

Servicios disponibles:
- **HDFS Web UI**: http://localhost:9870
- **Spark Master UI**: http://localhost:8080
- **Kafka UI**: http://localhost:8085
- **MongoDB Express**: http://localhost:8081 (admin/admin123)

### Paso 2: Generar datos

```bash
cd codigo
python generar_datos.py
```

### Paso 3: Cargar datos a HDFS

```bash
# Desde el contenedor namenode
docker exec -it namenode bash /scripts/batch/carga_hdfs.sh
```

### Paso 4: Ejecutar ETL Batch

```bash
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.1 \
  /scripts/batch/etl_smartlogis_batch.py
```

### Paso 5: Iniciar Streaming

**Terminal 1 - Productor Kafka:**
```bash
docker exec -it spark-master python /scripts/streaming/kafka_producer_gps.py
```

**Terminal 2 - Consumer Spark Streaming:**
```bash
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.mongodb.spark:mongo-spark-connector_2.12:10.2.1 \
  /scripts/streaming/spark_streaming_smartlogis.py
```

### Paso 6: Generar visualizaciones

```bash
docker exec -it spark-master python /scripts/visualizacion/visualizacion_smartlogis.py
```

---

## 📊 Datos de Entrada

| Archivo | Formato | Registros | Fuente | Uso |
|---------|---------|-----------|--------|-----|
| entregas.csv | CSV | 12,000 | Python/Faker | **Archivo principal** - Órdenes de entrega |
| vehiculos.csv | CSV | 15 | Simulado | Datos maestros de flota |
| gps_registros.json | JSON | 8,000 | IA/Python | Registros GPS por vehículo |
| incidencias.json | JSON | 500 | Simulado | Eventos críticos |
| eventos_sistema.log | LOG | 2,000 | Simulado | Logs del sistema de despacho |
| **Eventos streaming** | JSON/Kafka | 2,000 | Simulado | Eventos GPS en tiempo real |

---

## 🔔 Eventos de Streaming (4 tipos)

| Tipo | Descripción | Frecuencia |
|------|-------------|------------|
| `gps_update` | Actualización de posición GPS | 50% |
| `speed_alert` | Alerta de velocidad excedida (>100 km/h) | 20% |
| `route_deviation` | Desviación de ruta planeada | 15% |
| `delivery_status` | Cambio de estado de entrega | 15% |

### Reglas de Alerta
1. **VELOCIDAD_EXCEDIDA**: Velocidad > 100 km/h
2. **DESVIACION_RUTA**: Desviación > 2 km de ruta planeada

### Resúmenes Streaming
1. Eventos por tipo en ventana de 1 minuto
2. Alertas por vehículo en ventana de 2 minutos

---

## 📈 KPIs Generados

1. **Total Entregas**: 12,000
2. **Tasa de Éxito Promedio**: ~85%
3. **Total Incidencias**: 500
4. **KM Totales Recorridos**: ~150,000 km
5. **Velocidad Promedio de Flota**: ~65 km/h

---

## 🛠️ Tecnologías Utilizadas

| Tecnología | Versión | Uso |
|------------|---------|-----|
| Apache Hadoop (HDFS) | 3.2.1 | Almacenamiento distribuido |
| Apache Spark | 3.4.1 | Procesamiento batch y streaming |
| Apache Kafka | 7.5.0 | Streaming de eventos en tiempo real |
| Zookeeper | 7.5.0 | Coordinación de Kafka |
| MongoDB | 6.0 | Base de datos documental |
| Docker Compose | 3.8 | Orquestación de contenedores |
| Python | 3.9+ | Lenguaje de desarrollo |
| PyMongo | 4.x | Conector MongoDB |
| Matplotlib/Seaborn | 3.x | Visualización |

---

## 📜 Licencia

Proyecto académico - Instituto CERTUS 2026
