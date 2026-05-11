#!/usr/bin/env python3
"""
generar_datos.py
================
Genera los 5 archivos de entrada para SmartLogis AA4:
- vehiculos.csv      (maestros de flota)
- entregas.csv      (10,000+ registros - ARCHIVO PRINCIPAL)
- gps_registros.json (registros GPS)
- incidencias.json  (eventos críticos)
- eventos_sistema.log (logs del sistema)

Ejecutar: python generar_datos.py
"""

import csv
import json
import random
import os
from datetime import datetime, timedelta
from faker import Faker

fake = Faker('es_PE')
Faker.seed(42)
random.seed(42)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ============================================================
# DATOS BASE
# ============================================================
PLACAS = ["ABC-001", "ABC-002", "XYZ-101", "XYZ-102", "LMN-050",
            "LMN-051", "QRS-200", "QRS-201", "TUV-300", "TUV-301",
            "DEF-400", "DEF-401", "GHI-500", "GHI-501", "JKL-600"]

MODELOS = ["Volvo FH 2022", "Mercedes Actros 2021", "Scania R450 2020",
             "MAN TGX 2022", "DAF XF 2021", "Iveco Stralis 2020",
             "Renault T 2022", "Freightliner Cascadia 2021"]

CONDUCTORES = [f"DRV-{i:03d}" for i in range(1, 21)]

CLIENTES = [f"CLI-{i:03d}" for i in range(1, 51)]

RUTAS = [
    ("Lima Centro", "Callao", 15), ("Lima Centro", "Miraflores", 8),
    ("Lima Centro", "San Isidro", 10), ("Lima Centro", "Surco", 12),
    ("Lima Centro", "Comas", 18), ("Lima Centro", "San Juan de Lurigancho", 22),
    ("Lima Centro", "Villa El Salvador", 25), ("Lima Centro", "Ate", 20),
    ("San Isidro", "La Molina", 15), ("Miraflores", "Barranco", 5),
    ("Miraflores", "Chorrillos", 8), ("Callao", "Bellavista", 6),
    ("Lima Centro", "Cusco", 1200), ("Lima Centro", "Arequipa", 1000),
    ("Lima Centro", "Trujillo", 550), ("Lima Centro", "Chiclayo", 650),
    ("Lima Centro", "Piura", 850), ("Lima Centro", "Iquitos", 1200),
    ("Lima Centro", "Huancayo", 300), ("Lima Centro", "Tacna", 1300)
]

TIPOS_INCIDENCIA = ["Avería mecánica", "Llanta pinchada", "Accidente leve",
                    "Retraso por tráfico", "Multa de tránsito", "Falla de GPS",
                    "Sobrecarga", "Combustible bajo", "Pérdida de carga"]

SEVERIDADES = ["Baja", "Media", "Alta"]

ESTADOS_ENTREGA = ["Completado", "En tránsito", "Fallido", "Pendiente"]

# ============================================================
# 1. VEHICULOS.CSV (15 registros)
# ============================================================
print("Generando vehiculos.csv...")
with open(os.path.join(OUTPUT_DIR, "vehiculos.csv"), "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["placa", "modelo", "capacidad_kg", "conductor_id",
                     "ano_fabricacion", "tipo_combustible", "activo"])
    for placa in PLACAS:
        writer.writerow([
            placa,
            random.choice(MODELOS),
            random.randint(2000, 8000),
            random.choice(CONDUCTORES),
            random.randint(2019, 2024),
            random.choice(["Diesel", "GNV", "Gasolina", "Eléctrico"]),
            random.choice([True, True, True, False])  # 75% activos
        ])

# ============================================================
# 2. ENTREGAS.CSV (12,000 registros - ARCHIVO PRINCIPAL)
# ============================================================
print("Generando entregas.csv (12,000 registros)...")
with open(os.path.join(OUTPUT_DIR, "entregas.csv"), "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["orden_id", "placa_vehiculo", "origen", "destino",
                     "peso_kg", "fecha_entrega", "estado", "duracion_min",
                     "distancia_km", "cliente_id"])

    for i in range(1, 12001):
        origen, destino, distancia = random.choice(RUTAS)
        estado = random.choices(ESTADOS_ENTREGA, weights=[60, 20, 10, 10])[0]

        # Fecha entre enero 2025 y mayo 2026
        fecha = datetime(2025, 1, 1) + timedelta(
            days=random.randint(0, 500),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )

        # Duración basada en distancia + variación
        duracion = int(distancia * random.uniform(2, 5)) if estado != "Pendiente" else None

        writer.writerow([
            f"ORD-{i:05d}",
            random.choice(PLACAS),
            origen,
            destino,
            round(random.uniform(50, 5000), 2),
            fecha.strftime("%Y-%m-%d %H:%M:%S"),
            estado,
            duracion if duracion else "",
            round(distancia * random.uniform(0.9, 1.1), 2),
            random.choice(CLIENTES)
        ])

# ============================================================
# 3. GPS_REGISTROS.JSON (8,000 registros)
# ============================================================
print("Generando gps_registros.json...")
gps_data = []
for i in range(8000):
    placa = random.choice(PLACAS)
    # Coordenadas alrededor de Lima
    lat = round(random.uniform(-12.1, -11.8), 6)
    lon = round(random.uniform(-77.1, -76.9), 6)

    fecha = datetime(2025, 1, 1) + timedelta(
        days=random.randint(0, 500),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )

    gps_data.append({
        "registro_id": f"GPS-{i+1:05d}",
        "placa_vehiculo": placa,
        "latitud": lat,
        "longitud": lon,
        "velocidad_kmh": round(random.uniform(0, 120), 1),
        "timestamp": fecha.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "satelites": random.randint(3, 12),
        "precision_gps": round(random.uniform(1.0, 10.0), 2)
    })

with open(os.path.join(OUTPUT_DIR, "gps_registros.json"), "w", encoding="utf-8") as f:
    json.dump(gps_data, f, indent=2, ensure_ascii=False)

# ============================================================
# 4. INCIDENCIAS.JSON (500 registros)
# ============================================================
print("Generando incidencias.json...")
incidencias = []
for i in range(500):
    fecha = datetime(2025, 1, 1) + timedelta(
        days=random.randint(0, 500),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )

    severidad = random.choices(SEVERIDADES, weights=[50, 35, 15])[0]
    resuelta = random.choices([True, False], weights=[70, 30])[0]

    incidencias.append({
        "incidencia_id": f"INC-{i+1:04d}",
        "placa_vehiculo": random.choice(PLACAS),
        "tipo": random.choice(TIPOS_INCIDENCIA),
        "severidad": severidad,
        "fecha": fecha.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "descripcion": f"Evento registrado en ruta. Requiere atención.",
        "costo_estimado_soles": round(random.uniform(100, 5000), 2),
        "resuelta": resuelta,
        "tiempo_resolucion_min": random.randint(30, 600) if resuelta else None
    })

with open(os.path.join(OUTPUT_DIR, "incidencias.json"), "w", encoding="utf-8") as f:
    json.dump(incidencias, f, indent=2, ensure_ascii=False)

# ============================================================
# 5. EVENTOS_SISTEMA.LOG (2,000 líneas)
# ============================================================
print("Generando eventos_sistema.log...")
log_levels = ["INFO", "WARN", "ERROR", "DEBUG"]
components = ["AUTH", "SCHEDULER", "GPS_TRACKER", "DELIVERY", "MONITOR", "DB_SYNC"]
messages = [
    "Usuario autenticado correctamente",
    "Asignación de ruta completada",
    "GPS sync generado exitosamente",
    "Conexión a MongoDB establecida",
    "Entrega registrada en sistema",
    "Alerta de velocidad excedida",
    "Error de conexión con sensor",
    "Backup diario completado",
    "Vehículo fuera de ruta detectado",
    "Orden procesada correctamente"
]

with open(os.path.join(OUTPUT_DIR, "eventos_sistema.log"), "w", encoding="utf-8") as f:
    for i in range(2000):
        fecha = datetime(2025, 1, 1) + timedelta(
            days=random.randint(0, 500),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )
        level = random.choices(log_levels, weights=[60, 20, 10, 10])[0]
        comp = random.choice(components)
        msg = random.choice(messages)
        f.write(f"{fecha.strftime('%Y-%m-%d %H:%M:%S')} [{level}] [{comp}] {msg}\n")

print("\n✅ Todos los archivos generados correctamente:")
for fname in os.listdir(OUTPUT_DIR):
    fpath = os.path.join(OUTPUT_DIR, fname)
    size = os.path.getsize(fpath)
    print(f"  📄 {fname:<25} ({size:,} bytes)")
