#!/usr/bin/env python3
"""
_producer_gps.py
=====================
Productor de eventos Kafka para SmartLogis AA4.
Simula eventos GPS en tiempo real de vehículos de la flota.

Tipos de eventos:
  1. gps_update     - Actualización de posición GPS
  2. speed_alert    - Alerta de velocidad excedida (>100 km/h)
  3. route_deviation - Desviación de ruta
  4. delivery_status - Cambio de estado de entrega

Ejecutar: python kafka_producer_gps.py
"""

import json
import random
import time
import os
from datetime import datetime
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

# ============================================================
# CONFIGURACIÓN
# ============================================================
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_NAME = "smartlogis-events"
NUM_EVENTS = int(os.getenv("NUM_EVENTS", 2000))  # 1,000 a 3,000 eventos

PLACAS = ["ABC-001", "ABC-002", "XYZ-101", "XYZ-102", "LMN-050",
            "LMN-051", "QRS-200", "QRS-201", "TUV-300", "TUV-301",
            "DEF-400", "DEF-401", "GHI-500", "GHI-501", "JKL-600"]

RUTAS = [
    {"origen": "Lima Centro", "destino": "Callao", "distancia": 15},
    {"origen": "Lima Centro", "destino": "Miraflores", "distancia": 8},
    {"origen": "Lima Centro", "destino": "San Isidro", "distancia": 10},
    {"origen": "Lima Centro", "destino": "Surco", "distancia": 12},
    {"origen": "San Isidro", "destino": "La Molina", "distancia": 15},
    {"origen": "Miraflores", "destino": "Barranco", "distancia": 5},
    {"origen": "Callao", "destino": "Bellavista", "distancia": 6},
]

# ============================================================
# CREAR TOPIC SI NO EXISTE
# ============================================================
def crear_topic():
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            client_id='smartlogis-admin'
        )
        topic_list = [NewTopic(name=TOPIC_NAME, num_partitions=3, replication_factor=1)]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print(f"✅ Topic '{TOPIC_NAME}' creado")
        admin_client.close()
    except Exception as e:
        print(f"ℹ️  Topic '{TOPIC_NAME}' ya existe o error: {e}")

# ============================================================
# GENERADORES DE EVENTOS
# ============================================================
def generar_evento_gps(placa):
    """Evento tipo 1: Actualización GPS"""
    return {
        "evento_id": f"EV-GPS-{random.randint(10000,99999)}",
        "tipo_evento": "gps_update",
        "placa_vehiculo": placa,
        "latitud": round(random.uniform(-12.1, -11.8), 6),
        "longitud": round(random.uniform(-77.1, -76.9), 6),
        "velocidad_kmh": round(random.uniform(20, 110), 1),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "satelites": random.randint(3, 12),
        "precision_gps": round(random.uniform(1.0, 10.0), 2)
    }

def generar_alerta_velocidad(placa):
    """Evento tipo 2: Alerta de velocidad excedida"""
    return {
        "evento_id": f"EV-SPD-{random.randint(10000,99999)}",
        "tipo_evento": "speed_alert",
        "placa_vehiculo": placa,
        "latitud": round(random.uniform(-12.1, -11.8), 6),
        "longitud": round(random.uniform(-77.1, -76.9), 6),
        "velocidad_kmh": round(random.uniform(100, 160), 1),
        "velocidad_limite": 100,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "nivel_alerta": "ALTA" if random.random() > 0.7 else "MEDIA",
        "mensaje": f"Vehículo {placa} excedió límite de velocidad"
    }

def generar_desviacion_ruta(placa):
    """Evento tipo 3: Desviación de ruta"""
    ruta = random.choice(RUTAS)
    return {
        "evento_id": f"EV-DEV-{random.randint(10000,99999)}",
        "tipo_evento": "route_deviation",
        "placa_vehiculo": placa,
        "latitud": round(random.uniform(-12.5, -11.5), 6),
        "longitud": round(random.uniform(-77.5, -76.5), 6),
        "ruta_planeada_origen": ruta["origen"],
        "ruta_planeada_destino": ruta["destino"],
        "desviacion_km": round(random.uniform(2, 15), 2),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "nivel_alerta": "ALTA",
        "mensaje": f"Vehículo {placa} desviado de ruta planeada"
    }

def generar_cambio_entrega(placa):
    """Evento tipo 4: Cambio de estado de entrega"""
    estados = ["Pendiente", "En tránsito", "Completado", "Fallido"]
    estado_anterior = random.choice(estados)
    estado_nuevo = random.choice([e for e in estados if e != estado_anterior])
    return {
        "evento_id": f"EV-DEL-{random.randint(10000,99999)}",
        "tipo_evento": "delivery_status",
        "placa_vehiculo": placa,
        "orden_id": f"ORD-{random.randint(1,12000):05d}",
        "estado_anterior": estado_anterior,
        "estado_nuevo": estado_nuevo,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "nivel_alerta": "BAJA" if estado_nuevo == "Completado" else "MEDIA",
        "mensaje": f"Entrega cambió de {estado_anterior} a {estado_nuevo}"
    }

# ============================================================
# PRODUCTOR PRINCIPAL
# ============================================================
def main():
    print("=" * 60)
    print("SMARTLOGIS - PRODUCTOR DE EVENTOS KAFKA")
    print("=" * 60)
    print(f"Broker: {KAFKA_BOOTSTRAP}")
    print(f"Topic: {TOPIC_NAME}")
    print(f"Eventos a generar: {NUM_EVENTS}")
    print("=" * 60)

    crear_topic()

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        acks='all',
        retries=3
    )

    generadores = [
        generar_evento_gps,
        generar_alerta_velocidad,
        generar_desviacion_ruta,
        generar_cambio_entrega
    ]

    pesos = [50, 20, 15, 15]  # 50% GPS, 20% velocidad, 15% desviación, 15% entrega

    print("\n🚀 Enviando eventos...")
    contadores = {"gps_update": 0, "speed_alert": 0, "route_deviation": 0, "delivery_status": 0}

    for i in range(NUM_EVENTS):
        placa = random.choice(PLACAS)
        generador = random.choices(generadores, weights=pesos)[0]
        evento = generador(placa)
        contadores[evento["tipo_evento"]] += 1

        # Enviar con placa como key para particionamiento
        producer.send(
            TOPIC_NAME,
            key=placa,
            value=evento
        )

        if (i + 1) % 100 == 0:
            print(f"  📤 {i+1}/{NUM_EVENTS} eventos enviados...")

        time.sleep(0.01)  # Pequeña pausa para simular flujo real

    producer.flush()
    producer.close()

    print("\n✅ Todos los eventos enviados")
    print("\n📊 Resumen de eventos:")
    for tipo, cantidad in contadores.items():
        print(f"  • {tipo}: {cantidad}")

if __name__ == "__main__":
    main()
