import os
import requests
from fastapi import FastAPI, HTTPException

app = FastAPI(title="Servicio de Análisis")

HISTORY_URL = os.getenv("HISTORY_URL", "http://historial_ui:8003")

def evaluar_estado(temp_c: float, rpm: float, vib: float):
    razones = []

    # Temperatura
    if temp_c >= 70:
        razones.append("temp_critica")
    elif temp_c >= 55:
        razones.append("temp_alta")

    # Vibración
    if vib >= 0.65:
        razones.append("vib_critica")
    elif vib >= 0.30:
        razones.append("vib_alta")

    # RPM
    if rpm < 200:
        razones.append("rpm_critica_baja")
    elif rpm < 400:
        razones.append("rpm_baja")

    if rpm > 2200:
        razones.append("rpm_critica_alta")
    elif rpm > 1800:
        razones.append("rpm_alta")

    if any("critica" in r for r in razones):
        return "critical", razones
    if razones:
        return "warning", razones
    return "normal", razones


@app.get("/api/v1/health")
def health():
    return {"ok": True, "servicio": "analisis"}


@app.post("/api/v1/ingest")
def ingest(muestra: dict):
    required = ["ts", "machine_id", "actuator_id", "motor_temp_c", "motor_rpm", "motor_vibration_rms"]
    missing = [k for k in required if k not in muestra]
    if missing:
        raise HTTPException(status_code=422, detail=f"Faltan campos: {missing}")

    try:
        ts = str(muestra["ts"])
        machine_id = str(muestra["machine_id"])
        actuator_id = str(muestra["actuator_id"])
        temp_c = float(muestra["motor_temp_c"])
        rpm = float(muestra["motor_rpm"])
        vib = float(muestra["motor_vibration_rms"])
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Datos inválidos: {e}")

    # 1) Guardar muestra en historial
    payload_muestra = {
        "ts": ts,
        "machine_id": machine_id,
        "actuator_id": actuator_id,
        "motor_temp_c": temp_c,
        "motor_rpm": rpm,
        "motor_vibration_rms": vib,
    }

    try:
        requests.post(f"{HISTORY_URL}/api/v1/samples", json=payload_muestra, timeout=5).raise_for_status()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"No pude guardar muestra en historial: {e}")

    # 2) Diagnóstico simple por umbrales
    state, reasons = evaluar_estado(temp_c, rpm, vib)

    # Mantengo nombres "mean/rms" para compatibilidad con UI existente
    metrics = {
        "temp_mean": temp_c,
        "rpm_mean": rpm,
        "vib_rms": vib,
        "n_ventana": 1,
    }

    diagnostico = {
        "ts": ts,
        "machine_id": machine_id,
        "actuator_id": actuator_id,
        "state": state,
        "reasons": reasons,
        "metrics": metrics,
    }

    try:
        requests.post(f"{HISTORY_URL}/api/v1/diagnostics", json=diagnostico, timeout=5).raise_for_status()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"No pude guardar diagnóstico en historial: {e}")

    return {"accepted": True, "state": state, "reasons": reasons, "metrics": metrics}
