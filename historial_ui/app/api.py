from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Optional
from app.db import obtener_conexion

app = FastAPI(title="Servicio de Historial (API)")

class Muestra(BaseModel):
    ts: str
    machine_id: str
    actuator_id: str
    motor_temp_c: float
    motor_rpm: float
    motor_vibration_rms: float


class Diagnostico(BaseModel):
    ts: str
    machine_id: str
    actuator_id: str
    state: str
    reasons: List[str] = []
    metrics: dict = {}


@app.get("/api/v1/health")
def health():
    return {"ok": True, "servicio": "historial_ui"}

@app.post("/api/v1/samples")
def guardar_muestra(m: Muestra):
    con = obtener_conexion()
    con.execute(
        "INSERT INTO muestras (ts, machine_id, actuator_id, motor_temp_c, motor_rpm, motor_vibration_rms) VALUES (?,?,?,?,?,?)",
        (m.ts, m.machine_id, m.actuator_id, m.motor_temp_c, m.motor_rpm, m.motor_vibration_rms)
    )

    con.commit()
    con.close()
    return {"stored": True}

@app.post("/api/v1/diagnostics")
def guardar_diagnostico(d: Diagnostico):
    con = obtener_conexion()
    razones = ",".join(d.reasons)

    temp_mean = float(d.metrics.get("temp_mean", 0))
    temp_std  = float(d.metrics.get("temp_std", 0))
    rpm_mean  = float(d.metrics.get("rpm_mean", 0))
    rpm_std   = float(d.metrics.get("rpm_std", 0))
    vib_rms   = float(d.metrics.get("vib_rms", 0))

    con.execute(
        "INSERT INTO diagnosticos (ts, machine_id, actuator_id, estado, razones, temp_mean, temp_std, rpm_mean, rpm_std, vib_rms) "
        "VALUES (?,?,?,?,?,?,?,?,?,?)",
        (d.ts, d.machine_id, d.actuator_id, d.state, razones, temp_mean, temp_std, rpm_mean, rpm_std, vib_rms)
    )
    con.commit()
    con.close()
    return {"stored": True}

@app.get("/api/v1/latest")
def latest(machine_id: str, actuator_id: str):
    con = obtener_conexion()
    cur = con.execute(
        "SELECT ts, machine_id, actuator_id, estado, razones, temp_mean, temp_std, rpm_mean, rpm_std, vib_rms "
        "FROM diagnosticos WHERE machine_id=? AND actuator_id=? ORDER BY id DESC LIMIT 1",
        (machine_id, actuator_id)
    )
    row = cur.fetchone()
    con.close()

    if not row:
        return {"latest": None}

    ts, mid, aid, estado, razones, temp_mean, temp_std, rpm_mean, rpm_std, vib_rms = row
    return {
        "latest": {
            "ts": ts,
            "machine_id": mid,
            "actuator_id": aid,
            "state": estado,
            "reasons": razones.split(",") if razones else [],
            "metrics": {
                "temp_mean": temp_mean,
                "temp_std": temp_std,
                "rpm_mean": rpm_mean,
                "rpm_std": rpm_std,
                "vib_rms": vib_rms
            }
        }
    }

@app.get("/api/v1/diagnostics")
def diagnostics(machine_id: str, actuator_id: str, limite: int = 50):
    con = obtener_conexion()
    cur = con.execute(
        "SELECT ts, machine_id, actuator_id, estado, razones, temp_mean, temp_std, rpm_mean, rpm_std, vib_rms "
        "FROM diagnosticos WHERE machine_id=? AND actuator_id=? ORDER BY id DESC LIMIT ?",
        (machine_id, actuator_id, limite)
    )
    rows = cur.fetchall()
    con.close()

    items = []
    for ts, mid, aid, estado, razones, temp_mean, temp_std, rpm_mean, rpm_std, vib_rms in rows:
        items.append({
            "ts": ts,
            "machine_id": mid,
            "actuator_id": aid,
            "state": estado,
            "reasons": razones.split(",") if razones else [],
            "metrics": {
                "temp_mean": temp_mean,
                "temp_std": temp_std,
                "rpm_mean": rpm_mean,
                "rpm_std": rpm_std,
                "vib_rms": vib_rms
            }
        })

    items.reverse()
    return {"items": items}

@app.get("/api/v1/samples")
def samples(machine_id: str, actuator_id: str, limite: int = 200):
    con = obtener_conexion()
    cur = con.execute(
        "SELECT ts, machine_id, actuator_id, motor_temp_c, motor_rpm, motor_vibration_rms "
        "FROM muestras WHERE machine_id=? AND actuator_id=? ORDER BY id DESC LIMIT ?",
        (machine_id, actuator_id, limite)
    )
    rows = cur.fetchall()
    con.close()

    items = []
    for ts, mid, aid, t, rpm, v in rows:
        items.append({
            "ts": ts,
            "machine_id": mid,
            "actuator_id": aid,
            "motor_temp_c": t,
            "motor_rpm": rpm,
            "motor_vibration_rms": v
        })

    items.reverse()
    return {"items": items}

