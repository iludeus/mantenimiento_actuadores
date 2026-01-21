import os
import csv
import time
import threading
import requests
from fastapi import FastAPI, HTTPException

app = FastAPI(title="Servicio de Adquisición")

ANALYSIS_URL = os.getenv("ANALYSIS_URL", "http://analisis:8002")
RUTA_CSV = os.getenv("RUTA_CSV", "/datos/actuator_data.csv")
INTERVALO_SEG = float(os.getenv("INTERVALO_SEG", "1.0"))

estado = {
    "corriendo": False,
    "pausado": False,
    "enviado_total": 0,
}

def enviar_muestra(muestra: dict):
    r = requests.post(f"{ANALYSIS_URL}/api/v1/ingest", json=muestra, timeout=5)
    r.raise_for_status()

def reproductor_csv():
    """
    Lee el CSV y envía filas en loop infinito.
    Pause detiene temporalmente el envío.
    """
    try:
        while estado["corriendo"]:
            with open(RUTA_CSV, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if not estado["corriendo"]:
                        break

                    while estado["pausado"] and estado["corriendo"]:
                        time.sleep(0.2)

                    if not estado["corriendo"]:
                        break

                    muestra = {
                        "ts": row["ts"],
                        "machine_id": row["machine_id"],
                        "actuator_id": row["actuator_id"],
                        "motor_temp_c": float(row["motor_temp_c"]),
                        "motor_rpm": float(row["motor_rpm"]),
                        "motor_vibration_rms": float(row["motor_vibration_rms"]),
                    }

                    try:
                        enviar_muestra(muestra)
                        estado["enviado_total"] += 1
                    except Exception as e:
                        print(f"[adquisicion] Error enviando muestra: {e}")

                    time.sleep(INTERVALO_SEG)

            # al terminar el CSV, vuelve a empezar automáticamente (loop demo)
    finally:
        estado["corriendo"] = False
        estado["pausado"] = False


@app.get("/api/v1/health")
def health():
    return {"ok": True, "servicio": "adquisicion", **estado}


@app.post("/api/v1/control/start")
def start():
    if estado["corriendo"]:
        return {"ok": True, "msg": "Ya estaba corriendo"}

    estado["corriendo"] = True
    estado["pausado"] = False

    t = threading.Thread(target=reproductor_csv, daemon=True)
    t.start()

    return {"ok": True, "msg": "Demo iniciada (leyendo CSV)"}


@app.post("/api/v1/control/pause")
def pause_toggle():
    """
    Toggle: si está corriendo, pausa/reanuda.
    Sirve como botón único en la UI.
    """
    if not estado["corriendo"]:
        return {"ok": False, "msg": "No está corriendo. Usa START primero."}

    estado["pausado"] = not estado["pausado"]
    return {"ok": True, "pausado": estado["pausado"], "msg": "Pausado" if estado["pausado"] else "Reanudado"}
