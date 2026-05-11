#!/usr/bin/env python3
"""
visualizacion_smartlogis.py
============================
Genera visualizaciones y KPIs para SmartLogis AA4.
Requiere: pymongo, pandas, matplotlib, seaborn

Ejecutar: python visualizacion_smartlogis.py
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pymongo import MongoClient
import os
from datetime import datetime

# ============================================================
# CONFIGURACIÓN
# ============================================================
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017/")
DB_NAME = "smartlogis_db"
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "docs", "visualizaciones")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Estilo
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)
plt.rcParams['font.size'] = 10

# ============================================================
# CONEXIÓN A MONGODB
# ============================================================
client = MongoClient(MONGO_URI)
db = client[DB_NAME]

print("=" * 60)
print("SMARTLOGIS - VISUALIZACIÓN DE RESULTADOS")
print("=" * 60)

# ============================================================
# CARGAR DATOS
# ============================================================
print("\n📥 Cargando datos desde MongoDB...")

# Reporte de flota
df_flota = pd.DataFrame(list(db.reporte_flota.find()))
# Entregas
df_entregas = pd.DataFrame(list(db.entregas.find()))
# Incidencias
df_incidencias = pd.DataFrame(list(db.incidencias.find()))
# Eventos streaming (si existen)
try:
    df_eventos = pd.DataFrame(list(db.eventos_streaming.find().limit(1000)))
except:
    df_eventos = pd.DataFrame()

print(f"  ✓ Reporte flota: {len(df_flota)} registros")
print(f"  ✓ Entregas: {len(df_entregas)} registros")
print(f"  ✓ Incidencias: {len(df_incidencias)} registros")
print(f"  ✓ Eventos streaming: {len(df_eventos)} registros")

# ============================================================
# KPIs (5 mínimos requeridos)
# ============================================================
print("\n📊 CALCULANDO KPIs")

kpis = {}

# KPI 1: Total de entregas realizadas
kpis["Total Entregas"] = len(df_entregas)

# KPI 2: Tasa de éxito promedio
tasa_exito_promedio = df_flota["tasa_exito_pct"].mean() if "tasa_exito_pct" in df_flota.columns else 0
kpis["Tasa Éxito Promedio"] = f"{tasa_exito_promedio:.2f}%"

# KPI 3: Total de incidencias registradas
kpis["Total Incidencias"] = len(df_incidencias)

# KPI 4: Kilómetros totales recorridos
km_totales = df_flota["km_totales"].sum() if "km_totales" in df_flota.columns else 0
kpis["KM Totales Recorridos"] = f"{km_totales:,.0f} km"

# KPI 5: Velocidad promedio de la flota
vel_promedio = df_flota["velocidad_promedio"].mean() if "velocidad_promedio" in df_flota.columns else 0
kpis["Velocidad Promedio"] = f"{vel_promedio:.1f} km/h"

print("\n📈 KPIs Generados:")
for k, v in kpis.items():
    print(f"  • {k}: {v}")

# Guardar KPIs
with open(os.path.join(OUTPUT_DIR, "kpis.txt"), "w", encoding="utf-8") as f:
    f.write("KPIs - SMARTLOGIS AA4\n")
    f.write("=" * 40 + "\n")
    f.write(f"Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
    for k, v in kpis.items():
        f.write(f"{k}: {v}\n")

# ============================================================
# GRÁFICO 1: Entregas por Estado (Barras)
# ============================================================
print("\n📊 Generando Gráfico 1: Entregas por Estado...")

if "estado" in df_entregas.columns:
    estados = df_entregas["estado"].value_counts()

    fig, ax = plt.subplots(figsize=(10, 6))
    colores = ["#2ecc71", "#f39c12", "#e74c3c", "#3498db"]
    bars = ax.bar(estados.index, estados.values, color=colores[:len(estados)])
    ax.set_title("Distribución de Entregas por Estado", fontsize=14, fontweight='bold')
    ax.set_xlabel("Estado de Entrega")
    ax.set_ylabel("Cantidad")

    # Agregar valores sobre las barras
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height):,}',
                ha='center', va='bottom', fontsize=11)

    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "grafico1_entregas_estado.png"), dpi=150, bbox_inches='tight')
    plt.close()
    print("  ✅ grafico1_entregas_estado.png guardado")

# ============================================================
# GRÁFICO 2: Top 10 Vehículos por Kilómetros Recorridos (Horizontal)
# ============================================================
print("\n📊 Generando Gráfico 2: Top 10 Vehículos por KM...")

if "km_totales" in df_flota.columns and "placa_vehiculo" in df_flota.columns:
    top_km = df_flota.nlargest(10, "km_totales")[["placa_vehiculo", "km_totales"]]

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.barh(top_km["placa_vehiculo"], top_km["km_totales"], color="#3498db")
    ax.set_title("Top 10 Vehículos por Kilómetros Recorridos", fontsize=14, fontweight='bold')
    ax.set_xlabel("Kilómetros")
    ax.invert_yaxis()

    for bar in bars:
        width = bar.get_width()
        ax.text(width, bar.get_y() + bar.get_height()/2.,
                f'{int(width):,}',
                ha='left', va='center', fontsize=10)

    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "grafico2_top_km.png"), dpi=150, bbox_inches='tight')
    plt.close()
    print("  ✅ grafico2_top_km.png guardado")

# ============================================================
# GRÁFICO 3: Incidencias por Severidad (Pastel) + Tasa de Éxito
# ============================================================
print("\n📊 Generando Gráfico 3: Incidencias por Severidad...")

if "severidad" in df_incidencias.columns:
    severidad = df_incidencias["severidad"].value_counts()

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    # Pastel - Incidencias por severidad
    colores_pastel = ["#e74c3c", "#f39c12", "#2ecc71"]
    wedges, texts, autotexts = ax1.pie(
        severidad.values, labels=severidad.index, autopct='%1.1f%%',
        colors=colores_pastel[:len(severidad)], startangle=90
    )
    ax1.set_title("Incidencias por Nivel de Severidad", fontsize=13, fontweight='bold')

    # Barras - Tasa de éxito por vehículo (top 10)
    if "tasa_exito_pct" in df_flota.columns:
        top_exito = df_flota.nlargest(10, "tasa_exito_pct")[["placa_vehiculo", "tasa_exito_pct"]]
        bars = ax2.bar(top_exito["placa_vehiculo"], top_exito["tasa_exito_pct"], color="#2ecc71")
        ax2.set_title("Top 10 Vehículos - Tasa de Éxito (%)", fontsize=13, fontweight='bold')
        ax2.set_ylabel("Tasa de Éxito (%)")
        ax2.set_xlabel("Vehículo")
        ax2.tick_params(axis='x', rotation=45)

        for bar in bars:
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height,
                    f'{height:.1f}%',
                    ha='center', va='bottom', fontsize=9)

    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "grafico3_incidencias_exito.png"), dpi=150, bbox_inches='tight')
    plt.close()
    print("  ✅ grafico3_incidencias_exito.png guardado")

# ============================================================
# GRÁFICO EXTRA: Eventos Streaming (si hay datos)
# ============================================================
if len(df_eventos) > 0 and "tipo_evento" in df_eventos.columns:
    print("\n📊 Generando Gráfico Extra: Eventos Streaming...")

    eventos_tipo = df_eventos["tipo_evento"].value_counts()

    fig, ax = plt.subplots(figsize=(10, 6))
    colores = ["#9b59b6", "#e74c3c", "#f39c12", "#3498db"]
    bars = ax.bar(eventos_tipo.index, eventos_tipo.values, color=colores[:len(eventos_tipo)])
    ax.set_title("Eventos en Tiempo Real por Tipo", fontsize=14, fontweight='bold')
    ax.set_xlabel("Tipo de Evento")
    ax.set_ylabel("Cantidad")
    ax.tick_params(axis='x', rotation=15)

    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height):,}',
                ha='center', va='bottom', fontsize=11)

    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "grafico4_eventos_streaming.png"), dpi=150, bbox_inches='tight')
    plt.close()
    print("  ✅ grafico4_eventos_streaming.png guardado")

# ============================================================
# REPORTE FINAL
# ============================================================
print("\n" + "=" * 60)
print("✅ VISUALIZACIONES GENERADAS")
print("=" * 60)
print(f"📁 Guardadas en: {OUTPUT_DIR}")
for f in os.listdir(OUTPUT_DIR):
    print(f"  • {f}")

client.close()
