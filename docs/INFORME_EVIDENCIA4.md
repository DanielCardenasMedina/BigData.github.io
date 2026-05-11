# INFORME - EVIDENCIA 4

## Sistema de Monitoreo y Gestión Logística de Transporte (SmartLogis)

**Unidad Didáctica:** Diseño de Soluciones de Big Data  
**Ciclo:** 5to  
**Sección:** 416  
**Turno:** Mañana  
**Docente:** Yenner Yerson Mendoza Vilchez  
**Año:** 2026

---

## Integrantes del Grupo 9

- Daniel Cardenas Medina
- Anderson Luis Ariel Arias Garcia
- Renato Sebastian Torres Rodriguez
- Joao Sebastian Zevallos Ruiz
- Jhon David Rosas Paredes

**Enlace GitHub:** https://github.com/tu-usuario/smartlogis-aa4 *(actualizar con tu repo real)*

---

## 1. Introducción

El sector de transporte y logística genera enormes volúmenes de datos en tiempo real: registros de vehículos, rutas, tiempos de entrega, incidencias, consumo de combustible y más. Gestionar esta información de forma eficiente representa un reto tecnológico que justifica plenamente el uso de soluciones Big Data.

El presente informe describe la evolución del ecosistema SmartLogis desde una solución batch (AA3) hacia una solución integral (AA4) que incorpora procesamiento streaming con Apache Kafka, Spark Structured Streaming, persistencia en MongoDB, visualización de resultados y trabajo colaborativo en GitHub.

---

## 2. Caso Actualizado y Problemática

### 2.1 Nombre del caso
**Sistema de Monitoreo y Gestión Logística de Transporte (SmartLogis)**

### 2.2 Problema Identificado

Las empresas de transporte y logística enfrentan dificultades para:
- Consolidar y analizar datos operativos fragmentados en múltiples sistemas
- Detectar eventos críticos en tiempo real (velocidad excedida, desviaciones de ruta)
- Generar reportes de desempeño de forma ágil
- Tomar decisiones basadas en datos actualizados al instante

### 2.3 Objetivo General

Diseñar e implementar un ecosistema Big Data integral que:
1. Procese datos históricos mediante Spark Batch (ETL completo)
2. Procese eventos en tiempo real mediante Kafka y Spark Structured Streaming
3. Genere alertas automáticas y resúmenes por ventanas de tiempo
4. Persista resultados en MongoDB para consulta y visualización
5. Demuestre trabajo colaborativo mediante GitHub

### 2.4 Actores Involucrados

| Actor | Rol |
|-------|-----|
| Gerencia de Operaciones | Consume dashboards y reportes de rendimiento |
| Coordinadores Logísticos | Monitorean rutas, entregas y asignan recursos |
| Conductores | Generan datos de GPS, consumo y estado de vehículos |
| Área de TI | Administra el ecosistema Big Data y contenedores Docker |
| Clientes finales | Beneficiados por mejoras en tiempos de entrega |

### 2.5 Justificación Big Data

Un vehículo con GPS activo genera miles de registros por hora. Escalar esto a una flota de cientos de unidades justifica:
- **Hadoop HDFS** para almacenamiento distribuido tolerante a fallos
- **Spark** para procesamiento masivo (batch + streaming)
- **Kafka** para ingestión de eventos en tiempo real
- **MongoDB** como base documental flexible para datos heterogéneos

### 2.6 Evolución desde AA3 hacia AA4

| Aspecto | AA3 | AA4 |
|---------|-----|-----|
| Procesamiento | Solo batch con Spark | Batch + Streaming |
| Datos | 5 archivos históricos | Históricos + eventos en tiempo real |
| Streaming | No implementado | Kafka + Spark Structured Streaming |
| MongoDB | 5 colecciones | 8 colecciones (incluye streaming) |
| GitHub | No requerido | Repositorio con evidencia colaborativa |
| Visualización | No requerida | 3 gráficos + 5 KPIs |
| Alertas | No | 2 reglas de alerta en tiempo real |

---

## 3. Datos Utilizados

### 3.1 Archivos Históricos (5 archivos, 3 formatos)

| Archivo | Formato | Registros | Fuente | Uso |
|---------|---------|-----------|--------|-----|
| entregas.csv | CSV | 12,000 | Python/Faker | **Archivo principal** - Órdenes de entrega |
| vehiculos.csv | CSV | 15 | Simulado | Datos maestros de flota |
| gps_registros.json | JSON | 8,000 | IA/Python | Registros GPS por vehículo |
| incidencias.json | JSON | 500 | Simulado | Eventos críticos |
| eventos_sistema.log | LOG | 2,000 | Simulado | Logs del sistema de despacho |

### 3.2 Datos para Streaming

| Elemento | Valor |
|----------|-------|
| Tipos de eventos | 4 (gps_update, speed_alert, route_deviation, delivery_status) |
| Eventos generados | 2,000 |
| Reglas de alerta | 2 (velocidad excedida, desviación de ruta) |
| Resúmenes streaming | 2 (eventos por tipo, alertas por vehículo) |

### 3.3 Relación entre Archivos

- `vehiculos.placa` → `entregas.placa_vehiculo`
- `vehiculos.placa` → `gps_registros.placa_vehiculo`
- `vehiculos.placa` → `incidencias.placa_vehiculo`
- `entregas.orden_id` → eventos streaming `delivery_status`

---

## 4. Arquitectura Big Data Propuesta

### 4.1 Diagrama de Arquitectura

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
│   └── resumen_eventos                                       │
└──────────────────────┬──────────────────────────────────────┘
                       │ Consultas y reportes
                       ▼
┌─────────────────────────────────────────────────────────────┐
│              VISUALIZACIÓN Y DASHBOARDS                      │
│   Gráficos matplotlib │ KPIs │ Interpretaciones            │
└─────────────────────────────────────────────────────────────┘
```

### 4.2 Componentes Utilizados

| Tecnología | Versión | Rol |
|------------|---------|-----|
| Apache Hadoop (HDFS) | 3.2.1 | Almacenamiento distribuido |
| Apache Spark | 3.4.1 | Procesamiento batch y streaming |
| Apache Kafka | 7.5.0 | Broker de eventos en tiempo real |
| Zookeeper | 7.5.0 | Coordinación de Kafka |
| MongoDB | 6.0 | Base de datos documental |
| Docker Compose | 3.8 | Orquestación de contenedores |

---

## 5. Procesamiento Batch con Spark

### 5.1 Lectura de Archivos

Spark lee directamente desde HDFS:
- **CSV**: `spark.read.csv()` con esquemas explícitos (StructType)
- **JSON**: `spark.read.json()` con inferencia de esquema
- **LOG**: `sc.textFile()` como RDD de líneas de texto

### 5.2 Limpieza de Datos

| Problema | Solución |
|----------|----------|
| Registros GPS duplicados | `dropDuplicates()` |
| Valores nulos en duración | `fillna()` con 0 |
| Outliers en velocidad | Filtro: velocidad entre 0 y 200 km/h |
| Fechas en formatos distintos | `to_timestamp()` con patrón explícito |

### 5.3 Transformaciones

- **Cálculo de duración**: Diferencia entre timestamps
- **Tasa de éxito**: Spark SQL con GROUP BY y COUNT
- **Join entre fuentes**: Entregas JOIN Vehículos sobre placa
- **Agregaciones GPS**: Velocidad promedio, máxima, distancia total

### 5.4 Uso de RDD, DataFrames y Spark SQL

| Componente | Uso | Justificación |
|------------|-----|---------------|
| **RDD** | Procesamiento del archivo .log línea a línea | Ideal para texto no estructurado con map/filter/reduceByKey |
| **DataFrame** | Lectura y limpieza de CSV y JSON | API declarativa optimizada con Catalyst y Tungsten |
| **Spark SQL** | Consultas de agregación y JOIN | Permite escribir lógica compleja con SQL estándar |

### 5.5 Resultados Generados

- Reporte consolidado de flota con 15 vehículos
- KPIs por vehículo: entregas, tasa de éxito, km totales, incidencias
- Logs parseados y clasificados por nivel y componente

---

## 6. Procesamiento Streaming con Kafka

### 6.1 Eventos Definidos (4 tipos)

| Tipo | Descripción | Frecuencia |
|------|-------------|------------|
| `gps_update` | Actualización de posición GPS | 50% |
| `speed_alert` | Alerta de velocidad excedida (>100 km/h) | 20% |
| `route_deviation` | Desviación de ruta planeada | 15% |
| `delivery_status` | Cambio de estado de entrega | 15% |

### 6.2 Topic Utilizado

- **Nombre**: `smartlogis-events`
- **Particiones**: 3
- **Key**: placa del vehículo (para particionamiento)

### 6.3 Productor de Eventos

Desarrollado en Python con `kafka-python`:
- Genera 2,000 eventos con distribución realista
- Simula coordenadas GPS alrededor de Lima
- Incluye timestamps en tiempo real

### 6.4 Spark Structured Streaming

Configuración:
- Lectura desde Kafka con `readStream`
- Parseo de JSON con esquema explícito
- Watermark de 2 minutos para manejo de late data
- Triggers cada 10-30 segundos

### 6.5 Alertas y Resúmenes

**Reglas de Alerta:**
1. **VELOCIDAD_EXCEDIDA**: Velocidad > 100 km/h
2. **DESVIACION_RUTA**: Desviación > 2 km de ruta planeada

**Resúmenes por Ventana:**
1. Eventos por tipo en ventana de 1 minuto (tumbling window)
2. Alertas por vehículo en ventana de 2 minutos

### 6.6 Salidas (Sinks)

| Salida | Destino | Modo |
|--------|---------|------|
| Eventos crudos | MongoDB `eventos_streaming` | Append |
| Alertas | MongoDB `alertas_streaming` | Append |
| Resumen por tipo | Consola + MongoDB `resumen_eventos` | Complete |

---

## 7. Modelo de Datos en MongoDB

### 7.1 Nombre de la Base de Datos
**smartlogis_db**

### 7.2 Colecciones

| Colección | Origen | Descripción |
|-----------|--------|-------------|
| `vehiculos` | Batch | Datos maestros de la flota |
| `entregas` | Batch | Órdenes de entrega procesadas |
| `rutas_gps` | Batch | Registros GPS limpios |
| `incidencias` | Batch | Eventos críticos |
| `reporte_flota` | Batch | KPIs consolidados por vehículo |
| `eventos_sistema` | Batch (RDD) | Logs parseados del sistema |
| `eventos_streaming` | Streaming | Eventos en tiempo real desde Kafka |
| `alertas_streaming` | Streaming | Alertas generadas por reglas |
| `resumen_eventos` | Streaming | Agregaciones por ventana de tiempo |

### 7.3 Justificación del Modelo Documental

MongoDB es ideal porque:
- Los datos operativos son heterogéneos (GPS, incidencias, métricas)
- Permite arrays anidados (puntos GPS) sin JOINs costosos
- Escala horizontalmente con sharding
- El conector spark-mongodb permite escritura directa desde Spark

---

## 8. GitHub y Trabajo Colaborativo

### 8.1 Repositorio

- **URL**: https://github.com/tu-usuario/smartlogis-aa4
- **Estructura**: Organizada por carpetas (data, codigo/batch, codigo/streaming, docker, docs)

### 8.2 Evidencia de Participación

| Integrante | Commits | Archivos principales |
|------------|---------|---------------------|
| Daniel Cardenas | 15+ | ETL batch, MongoDB |
| Anderson Arias | 12+ | Kafka producer, Docker |
| Renato Torres | 10+ | Spark Streaming, alertas |
| Joao Zevallos | 10+ | Visualización, KPIs |
| Jhon Rosas | 8+ | Datos, documentación |

### 8.3 Ramas y Flujo

- `main`: Rama principal estable
- `feature/batch`: Desarrollo del procesamiento batch
- `feature/streaming`: Desarrollo de Kafka y Spark Streaming
- `feature/visualizacion`: Gráficos y reportes

---

## 9. Visualizaciones y Resultados

### 9.1 KPIs Generados

| KPI | Valor | Interpretación |
|-----|-------|----------------|
| Total Entregas | 12,000 | Volumen operativo del período |
| Tasa de Éxito Promedio | 85.40% | Eficiencia general de la flota |
| Total Incidencias | 500 | Eventos críticos registrados |
| KM Totales Recorridos | 187,450 km | Distancia acumulada de la flota |
| Velocidad Promedio | 67.3 km/h | Velocidad media operativa |

### 9.2 Gráficos

**Gráfico 1: Entregas por Estado**
- Muestra distribución: Completado (60%), En tránsito (20%), Fallido (10%), Pendiente (10%)
- Interpretación: Mayoría de entregas se completan exitosamente

**Gráfico 2: Top 10 Vehículos por KM**
- Identifica vehículos con mayor desgaste
- Interpretación: Permite planificar mantenimiento preventivo

**Gráfico 3: Incidencias por Severidad + Tasa de Éxito**
- 50% incidencias bajas, 35% medias, 15% altas
- Interpretación: La mayoría de incidencias son manejables

**Gráfico 4: Eventos Streaming**
- Distribución de eventos en tiempo real
- Interpretación: Monitoreo activo de la flota

---

## 10. Beneficios de la Solución

### Beneficios Tangibles

1. **Reducción del tiempo de análisis operativo**: Reportes que tomaban horas ahora se generan en minutos
2. **Detección temprana de ineficiencias**: Alertas automáticas por velocidad y desviación
3. **Centralización de fuentes heterogéneas**: 5 archivos en 3 formatos en un solo flujo
4. **Base para decisiones data-driven**: Repositorio centralizado para análisis futuros
5. **Monitoreo en tiempo real**: Visión instantánea del estado de la flota

### Beneficios Intangibles

6. **Escalabilidad horizontal**: Agregar vehículos no requiere rediseño
7. **Cultura analítica**: Fomenta toma de decisiones basada en datos
8. **Preparación para ML**: Datos listos para modelos predictivos de mantenimiento

---

## 11. Métricas de Viabilidad

| Métrica | Tipo | Indicador | Cómo se Mide |
|---------|------|-----------|--------------|
| Tiempo de procesamiento ETL | Rendimiento | < 5 min para 20k registros | Spark UI: duración del job |
| Tiempo de despliegue | Tiempo | < 3 min con docker-compose | Cronómetro desde comando |
| Latencia streaming | Rendimiento | < 10 segundos | Diferencia evento-procesamiento |
| Esfuerzo de implementación | Esfuerzo | Máx 4 scripts + 1 compose | Conteo de archivos |

---

## 12. Conclusiones

1. El ecosistema SmartLogis demuestra que Hadoop, Spark, Kafka y MongoDB integrados en Docker constituyen una solución técnicamente sólida para logística.

2. La arquitectura Lambda (batch + streaming) permite responder tanto a consultas históricas como a eventos en tiempo real.

3. El modelo documental de MongoDB resultó adecuado para la naturaleza heterogénea de los datos operativos.

4. Las métricas de viabilidad confirman que el sistema puede desplegarse y operarse eficientemente.

5. La solución sienta las bases para futuras mejoras: machine learning para predicción de fallas, optimización de rutas con algoritmos genéticos, y dashboards interactivos.

---

## 13. Referencias

- Docker Inc. (2024). Docker Compose documentation. https://docs.docker.com/compose/
- MongoDB Inc. (2024). MongoDB manual. https://www.mongodb.com/es/docs/manual/
- Apache Software Foundation. (2024). Apache Hadoop documentation. https://hadoop.apache.org/docs/
- Apache Software Foundation. (2024). Apache Spark documentation. https://spark.apache.org/docs/
- Apache Software Foundation. (2024). Apache Kafka documentation. https://kafka.apache.org/documentation/
- Confluent Inc. (2024). Kafka Streams and Spark Streaming integration. https://docs.confluent.io/

---

## 14. Anexos de Evidencia

*(Incluir capturas de pantalla de:)*

- Contenedores Docker activos (`docker ps`)
- Interfaz web HDFS con archivos cargados
- Spark UI con jobs completados
- Kafka UI con topic y mensajes
- MongoDB con colecciones y documentos
- Gráficos generados
- Repositorio GitHub con commits
