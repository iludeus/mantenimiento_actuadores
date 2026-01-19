import os
import csv
import time
import threading
import requests
from fastapi import FastAPI

app = FastAPI(title="Servicio de Adquisición")

ANALYSIS_URL = os.getenv("ANALYSIS_URL", "http://analisis:8002")
RUTA_CSV = os.getenv("RUTA_CSV", "/datos/actuator_data.csv")

# Intervalo entre envíos (segundos). Lo puedes cambiar desde UI con /control/intervalo
INTERVALO_SEG = float(os.getenv("INTERVALO_SEG", "3"))

estado = {
    "corriendo": False,
    "pausado": False,
    "detener": False,
    "enviado_total": 0,
}

def enviar_muestra(muestra: dict):
    r = requests.post(f"{ANALYSIS_URL}/api/v1/ingest", json=muestra, timeout=5)
    r.raise_for_status()

def hilo_reproductor():
    """
    Lee el CSV y envía muestras en LOOP infinito.
    Cuando llega al final del archivo, vuelve al inicio.
    Respeta pause/resume/stop.
    """
    global INTERVALO_SEG
    try:
        while True:
            if estado["detener"]:
                break

            with open(RUTA_CSV, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if estado["detener"]:
                        break

                    while estado["pausado"] and not estado["detener"]:
                        time.sleep(0.2)

                    if estado["detener"]:
                        break

                    # CSV esperado:
                    # ts,machine_id,actuator_id,motor_temp_c,motor_rpm,motor_vibration_rms
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
                        # estilo estudiante: imprimir y seguir
                        print(f"[adquisicion] Error enviando muestra: {e}")

                    time.sleep(float(INTERVALO_SEG))

    finally:
        estado["corriendo"] = False
        estado["pausado"] = False
        estado["detener"] = False

@app.get("/api/v1/health")
def health():
    return {
        "ok": True,
        "servicio": "adquisicion",
        **estado,
        "ruta_csv": RUTA_CSV,
        "analysis_url": ANALYSIS_URL,
        "intervalo_seg": INTERVALO_SEG,
    }

@app.post("/api/v1/control/start")
def start():
    if estado["corriendo"]:
        return {"ok": True, "msg": "Ya estaba corriendo", "intervalo_seg": INTERVALO_SEG}

    estado["corriendo"] = True
    estado["pausado"] = False
    estado["detener"] = False

    t = threading.Thread(target=hilo_reproductor, daemon=True)
    t.start()

    return {"ok": True, "msg": "Reproduccion iniciada", "intervalo_seg": INTERVALO_SEG}

@app.post("/api/v1/control/pause")
def pause():
    if not estado["corriendo"]:
        return {"ok": True, "msg": "No estaba corriendo (nada que pausar)"}
    estado["pausado"] = True
    return {"ok": True, "msg": "Pausado"}

@app.post("/api/v1/control/resume")
def resume():
    if not estado["corriendo"]:
        return {"ok": True, "msg": "No estaba corriendo (nada que reanudar)"}
    estado["pausado"] = False
    return {"ok": True, "msg": "Reanudado"}

@app.post("/api/v1/control/stop")
def stop():
    if not estado["corriendo"]:
        return {"ok": True, "msg": "No estaba corriendo (nada que detener)"}
    estado["detener"] = True
    estado["pausado"] = False
    return {"ok": True, "msg": "Deteniendo adquisicion"}

@app.post("/api/v1/control/intervalo")
def set_intervalo(segundos: float):
    """
    Cambia el intervalo mientras corre.
    """
    global INTERVALO_SEG
    if segundos < 0.2:
        segundos = 0.2
    if segundos > 30:
        segundos = 30
    INTERVALO_SEG = float(segundos)
    return {"ok": True, "intervalo_seg": INTERVALO_SEG}
