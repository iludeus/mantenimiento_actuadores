import os
import requests
from collections import deque
from math import sqrt
from statistics import mean, pstdev

from fastapi import FastAPI, HTTPException

app = FastAPI(title="Servicio de Análisis")

# En docker compose, "historial_ui" es el hostname del contenedor
HISTORY_URL = os.getenv("HISTORY_URL", "http://historial_ui:8003")

# Ventana para métricas (muestras recientes). Fácil de explicar al profe.
N_VENTANA = int(os.getenv("N_VENTANA", "8"))

# Buffers por (machine_id, actuator_id)
buffers = {}  # key: (machine_id, actuator_id) -> deque(maxlen=N_VENTANA)


# -------------------------
# Utilidades matemáticas
# -------------------------
def rms(valores):
    """RMS simple (para vibración)."""
    if not valores:
        return 0.0
    return sqrt(sum(x * x for x in valores) / len(valores))


def obtener_buffer(machine_id: str, actuator_id: str):
    key = (machine_id, actuator_id)
    if key not in buffers:
        buffers[key] = deque(maxlen=N_VENTANA)
    return buffers[key]


# -------------------------
# Clasificación de estado
# -------------------------
def evaluar_estado(metrics):
    """
    Umbrales elegidos (simulación realista + fácil de explicar):
      - Temperatura (°C): normal <55, warning 55-70, critical >=70
      - Vibración RMS:    normal <0.30, warning 0.30-0.65, critical >=0.65
      - RPM: warning si <400 o >1800; critical si <200 o >2200

    Devuelve: (state, reasons)
    """
    razones = []

    temp = float(metrics["temp_mean"])
    vib = float(metrics["vib_rms"])
    rpm = float(metrics["rpm_mean"])

    # Temperatura
    if temp >= 70:
        razones.append("temp_critica")
    elif temp >= 55:
        razones.append("temp_alta")

    # Vibración
    if vib >= 0.65:
        razones.append("vib_critica")
    elif vib >= 0.30:
        razones.append("vib_alta")

    # RPM (puede estar rara por carga o control)
    if rpm < 200:
        razones.append("rpm_critica_baja")
    elif rpm < 400:
        razones.append("rpm_baja")

    if rpm > 2200:
        razones.append("rpm_critica_alta")
    elif rpm > 1800:
        razones.append("rpm_alta")

    # Estado global por severidad
    if any("critica" in r for r in razones):
        return "critical", razones
    if razones:
        return "warning", razones
    return "normal", razones


# -------------------------
# Endpoints
# -------------------------
@app.get("/api/v1/health")
def health():
    return {
        "ok": True,
        "servicio": "analisis",
        "history_url": HISTORY_URL,
        "ventana": N_VENTANA,
        "buffers_activos": len(buffers),
    }


@app.post("/api/v1/ingest")
def ingest(muestra: dict):
    """
    Entrada esperada (por adquisición):
    {
      "ts": "...Z",
      "machine_id": "arm_01",
      "actuator_id": "hombro",
      "motor_temp_c": 50.0,
      "motor_rpm": 1100,
      "motor_vibration_rms": 0.2
    }
    """

    # 1) Validación básica para que no pase 500 por KeyError
    required = [
        "ts",
        "machine_id",
        "actuator_id",
        "motor_temp_c",
        "motor_rpm",
        "motor_vibration_rms",
    ]
    missing = [k for k in required if k not in muestra]
    if missing:
        raise HTTPException(status_code=422, detail=f"Faltan campos: {missing}")

    # 2) Extraer y normalizar tipos
    try:
        ts = str(muestra["ts"])
        machine_id = str(muestra["machine_id"])
        actuator_id = str(muestra["actuator_id"])

        temp_c = float(muestra["motor_temp_c"])
        rpm = float(muestra["motor_rpm"])
        vib = float(muestra["motor_vibration_rms"])
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Datos inválidos: {e}")

    # 3) Guardar muestra en historial (SQLite)
    payload_muestra = {
        "ts": ts,
        "machine_id": machine_id,
        "actuator_id": actuator_id,
        "motor_temp_c": temp_c,
        "motor_rpm": rpm,
        "motor_vibration_rms": vib,
    }

    try:
        r = requests.post(f"{HISTORY_URL}/api/v1/samples", json=payload_muestra, timeout=5)
        r.raise_for_status()
    except Exception as e:
        # Error comunicando con historial
        print(f"[analisis] Error enviando sample a historial: {e}")
        raise HTTPException(status_code=502, detail=f"No pude guardar muestra en historial: {e}")

    # 4) Actualizar buffer (ventana)
    buf = obtener_buffer(machine_id, actuator_id)
    buf.append(payload_muestra)

    temps = [float(x["motor_temp_c"]) for x in buf]
    rpms = [float(x["motor_rpm"]) for x in buf]
    vibs = [float(x["motor_vibration_rms"]) for x in buf]

    # 5) Calcular métricas (promedio, desviación, RMS)
    metrics = {
        "temp_mean": mean(temps),
        "temp_std": pstdev(temps) if len(temps) > 1 else 0.0,
        "rpm_mean": mean(rpms),
        "rpm_std": pstdev(rpms) if len(rpms) > 1 else 0.0,
        "vib_rms": rms(vibs),
        "n_ventana": len(buf),
    }

    # 6) Estado + razones
    state, reasons = evaluar_estado(metrics)

    diagnostico = {
        "ts": ts,
        "machine_id": machine_id,
        "actuator_id": actuator_id,
        "state": state,
        "reasons": reasons,
        "metrics": metrics,
    }

    # 7) Guardar diagnóstico en historial
    try:
        r2 = requests.post(f"{HISTORY_URL}/api/v1/diagnostics", json=diagnostico, timeout=5)
        r2.raise_for_status()
    except Exception as e:
        print(f"[analisis] Error enviando diagnóstico a historial: {e}")
        raise HTTPException(status_code=502, detail=f"No pude guardar diagnóstico en historial: {e}")

    return {
        "accepted": True,
        "state": state,
        "reasons": reasons,
        "metrics": metrics,
    }
