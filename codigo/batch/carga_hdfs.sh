#!/bin/bash
# ============================================================
# carga_hdfs.sh
# ============
# Script para cargar archivos de datos al HDFS de Hadoop.
# Ejecutar dentro del contenedor namenode o desde host con docker exec.
# ============================================================

echo "=========================================="
echo "SMARTLOGIS - CARGA DE DATOS A HDFS"
echo "=========================================="

HDFS_BASE="hdfs://namenode:9000/data/smartlogis"
LOCAL_DATA="/data"

# Crear directorio en HDFS si no existe
echo "📁 Creando directorio en HDFS..."
hdfs dfs -mkdir -p ${HDFS_BASE}

# Cargar archivos
echo "📤 Cargando archivos..."

hdfs dfs -put -f ${LOCAL_DATA}/vehiculos.csv ${HDFS_BASE}/
echo "  ✓ vehiculos.csv"

hdfs dfs -put -f ${LOCAL_DATA}/entregas.csv ${HDFS_BASE}/
echo "  ✓ entregas.csv (archivo principal - 12,000 registros)"

hdfs dfs -put -f ${LOCAL_DATA}/gps_registros.json ${HDFS_BASE}/
echo "  ✓ gps_registros.json"

hdfs dfs -put -f ${LOCAL_DATA}/incidencias.json ${HDFS_BASE}/
echo "  ✓ incidencias.json"

hdfs dfs -put -f ${LOCAL_DATA}/eventos_sistema.log ${HDFS_BASE}/
echo "  ✓ eventos_sistema.log"

# Verificar
echo ""
echo "📋 Verificando archivos en HDFS:"
hdfs dfs -ls ${HDFS_BASE}

echo ""
echo "=========================================="
echo "✅ Carga completada"
echo "=========================================="
