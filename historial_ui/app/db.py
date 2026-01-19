import sqlite3
from pathlib import Path

RUTA_DB = Path("/data/app.db")

def obtener_conexion():
    RUTA_DB.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(RUTA_DB)

    con.execute("""
        CREATE TABLE IF NOT EXISTS muestras (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT,
            machine_id TEXT,
            actuator_id TEXT,
            motor_temp_c REAL,
            motor_rpm REAL,
            motor_vibration_rms REAL
)
""")

    con.execute("""
        CREATE TABLE IF NOT EXISTS diagnosticos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT,
            machine_id TEXT,
            actuator_id TEXT,
            estado TEXT,
            razones TEXT,
            temp_mean REAL,
            temp_std REAL,
            rpm_mean REAL,
            rpm_std REAL,
            vib_rms REAL
        )
        """)


    con.commit()
    return con
