import time
import requests
import pandas as pd
import streamlit as st

# =========================
# Configuración fija (NO editable por usuario)
# =========================
API_URL = "http://historial_ui:8003"       # API historial (FastAPI)
ADQ_URL = "http://adquisicion:8001"       # Adquisición (FastAPI)

AUTO_UI = True
INTERVALO_UI_SEG = 1  # fijo 1s

LIMITE_SAMPLES = 120
LIMITE_DIAG = 80

ACTUADORES = ["base", "hombro", "codo"]

# =========================
# Helpers HTTP
# =========================
def safe_get(url, params=None, timeout=6):
    try:
        r = requests.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json(), None
    except Exception as e:
        return None, str(e)

def safe_post(url, params=None, json=None, timeout=6):
    try:
        r = requests.post(url, params=params, json=json, timeout=timeout)
        r.raise_for_status()
        return r.json(), None
    except Exception as e:
        return None, str(e)

# =========================
# Helpers UI (chips / colores)
# =========================
def severidad_estado(state: str) -> int:
    s = (state or "").lower()
    if s == "critical":
        return 3
    if s == "warning":
        return 2
    if s == "normal":
        return 1
    return 0

def color_estado(state: str) -> str:
    s = (state or "").lower()
    if s == "critical":
        return "#e53935"   # rojo
    if s == "warning":
        return "#fb8c00"   # naranjo
    if s == "normal":
        return "#43a047"   # verde
    return "#616161"       # gris

def emoji_estado(state: str) -> str:
    s = (state or "").lower()
    if s == "critical":
        return "🔴"
    if s == "warning":
        return "🟠"
    if s == "normal":
        return "🟢"
    return "⚪"

def chip(texto: str, color: str):
    st.markdown(
        f"""
        <span style="
            display:inline-block;
            padding:6px 10px;
            margin:2px 8px 6px 0;
            border-radius:999px;
            background:{color};
            color:white;
            font-size:13px;
            font-weight:700;">
            {texto}
        </span>
        """,
        unsafe_allow_html=True
    )

def badge(texto: str, color: str):
    st.markdown(
        f"""
        <div style="
            display:inline-block;
            padding:10px 14px;
            border-radius:14px;
            background:{color};
            color:white;
            font-weight:800;
            letter-spacing:0.2px;">
            {texto}
        </div>
        """,
        unsafe_allow_html=True
    )

def fmt_num(x, nd=1, suf=""):
    if x is None:
        return "-"
    try:
        return f"{float(x):.{nd}f}{suf}"
    except Exception:
        return "-"

def estado_por_metrica(temp, rpm, vib):
    # TEMP (°C)
    if temp is None:
        st_temp = "unknown"
    elif temp >= 70:
        st_temp = "critical"
    elif temp >= 55:
        st_temp = "warning"
    else:
        st_temp = "normal"

    # VIB (RMS)
    if vib is None:
        st_vib = "unknown"
    elif vib >= 0.65:
        st_vib = "critical"
    elif vib >= 0.30:
        st_vib = "warning"
    else:
        st_vib = "normal"

    # RPM (baja)
    if rpm is None:
        st_rpm = "unknown"
    elif rpm < 200:
        st_rpm = "critical"
    elif rpm < 400:
        st_rpm = "warning"
    else:
        st_rpm = "normal"

    return st_temp, st_rpm, st_vib

def max_estado(a, b):
    return a if severidad_estado(a) >= severidad_estado(b) else b

# =========================
# Estilos (sin cambiar todo el diseño)
# =========================
st.set_page_config(page_title="Monitoreo de actuadores", layout="wide")

st.markdown(
    """
    <style>
      .block-container {padding-top: 1.0rem;}
      div[data-testid="stMetricValue"] {font-size: 1.7rem;}
      div[data-testid="stMetricLabel"] {font-size: 0.95rem;}
      .tarjeta {
        background: rgba(255,255,255,0.03);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 18px;
        padding: 16px 16px 10px 16px;
      }
      .subtarjeta {
        background: rgba(0,0,0,0.15);
        border: 1px solid rgba(255,255,255,0.06);
        border-radius: 14px;
        padding: 12px;
      }
      .mini {
        font-size: 12px;
        opacity: 0.85;
      }
    </style>
    """,
    unsafe_allow_html=True
)

# =========================
# Encabezado
# =========================
st.title("Monitoreo de actuadores (simple)")
st.caption("Actualización fija: adquisición = 1s, UI = 1s. Actuadores: base / hombro / codo.")

machine_id = st.text_input("ID del brazo (machine_id)", value="arm_01")

# =========================
# Control (bonito, sin JSON crudo)
# =========================
st.subheader("Control de adquisición")

cA, cB = st.columns([1.2, 3.0])

with cA:
    # Solo mostramos estado resumido (sin JSON crudo)
    j_adq, err_adq = safe_get(f"{ADQ_URL}/api/v1/health")
    if err_adq:
        chip("Adquisición: NO conectada", color_estado("critical"))
        st.write(f"⚠️ {err_adq}")
        adq_corriendo = False
    else:
        adq_corriendo = bool(j_adq.get("corriendo", False))
        adq_pausado = bool(j_adq.get("pausado", False))
        if adq_corriendo and not adq_pausado:
            chip("Adquisición: corriendo", color_estado("normal"))
        elif adq_corriendo and adq_pausado:
            chip("Adquisición: pausada", color_estado("warning"))
        else:
            chip("Adquisición: detenida", "#616161")

        st.caption(f"Enviados: {j_adq.get('enviado_total', 0)} | intervalo: {j_adq.get('intervalo_seg', 1)}s")

    j_hist, err_hist = safe_get(f"{API_URL}/api/v1/health")
    if err_hist:
        chip("Historial: NO conectado", color_estado("critical"))
        st.write(f"⚠️ {err_hist}")
    else:
        chip("Historial: conectado", color_estado("normal"))

with cB:
    b1, b2, b3, b4, b5 = st.columns([1, 1, 1, 1, 1.3])

    with b1:
        if st.button("▶ Start", width="stretch"):
            res, err = safe_post(f"{ADQ_URL}/api/v1/control/start")
            if err:
                st.error(f"Start falló: {err}")
            else:
                st.success(res.get("msg", "Start OK"))

    with b2:
        if st.button("⏸ Pause", width="stretch"):
            res, err = safe_post(f"{ADQ_URL}/api/v1/control/pause")
            if err:
                st.error(f"Pause falló: {err}")
            else:
                st.info(res.get("msg", "Pause OK"))

    with b3:
        if st.button("⏵ Resume", width="stretch"):
            res, err = safe_post(f"{ADQ_URL}/api/v1/control/resume")
            if err:
                st.error(f"Resume falló: {err}")
            else:
                st.info(res.get("msg", "Resume OK"))

    with b4:
        if st.button("⏹ Stop", width="stretch"):
            res, err = safe_post(f"{ADQ_URL}/api/v1/control/stop")
            if err:
                st.error(f"Stop falló: {err}")
            else:
                st.warning(res.get("msg", "Stop OK"))

    with b5:
        if st.button("🛑 DETENER ROBOT", type="primary", width="stretch"):
            res, err = safe_post(f"{ADQ_URL}/api/v1/control/stop")
            if err:
                st.error(f"Detener falló: {err}")
            else:
                st.error("⚠️ Robot detenido (simulación). Se detuvo la adquisición.")

st.divider()

# =========================
# Estado actual (por actuador) - estilo "bonito"
# =========================
st.subheader("Estado actual (por actuador)")

latest_por_actuador = {}
estado_global = "normal"
act_criticos = []
sin_datos = []

for act in ACTUADORES:
    j, err = safe_get(f"{API_URL}/api/v1/latest", params={"machine_id": machine_id, "actuator_id": act})
    if err:
        latest_por_actuador[act] = {"state": "unknown", "error": err}
        sin_datos.append(act)
        estado_global = max_estado(estado_global, "warning")
        continue

    latest = j.get("latest") if isinstance(j, dict) and "latest" in j else j
    if not isinstance(latest, dict):
        latest_por_actuador[act] = {"state": "unknown", "ts": "-", "reasons": [], "metrics": {}, "actuator_id": act}
        sin_datos.append(act)
        estado_global = max_estado(estado_global, "warning")
        continue

    latest_por_actuador[act] = latest
    st_act = latest.get("state", "normal")
    estado_global = max_estado(estado_global, st_act)
    if (st_act or "").lower() == "critical":
        act_criticos.append(act)

# Header estado global
colG1, colG2 = st.columns([1.2, 3.0])
with colG1:
    chip(f"Estado general: {estado_global.upper()} {emoji_estado(estado_global)}", color_estado(estado_global))

with colG2:
    if act_criticos:
        st.error(f"🚨 Recomendación: DETENER ROBOT. Actuadores críticos: {', '.join(act_criticos)}")
    elif sin_datos:
        st.info(f"⏳ Aún sin datos para: {', '.join(sin_datos)}. Presiona Start y espera unos segundos.")
    else:
        st.success("✅ Operación aceptable (sin condición crítica).")

# Tarjetas por actuador
cols = st.columns(3)

for i, act in enumerate(ACTUADORES):
    with cols[i]:
        st.markdown(f"<div class='tarjeta'>", unsafe_allow_html=True)
        st.markdown(f"### {act}")

        data = latest_por_actuador.get(act, {})
        if "error" in data:
            chip("SIN DATOS", "#616161")
            st.write(data["error"])
            st.markdown("</div>", unsafe_allow_html=True)
            continue

        estado = data.get("state", "unknown")
        ts = ""

        # metrics
        metrics = data.get("metrics", {}) or {}
        temp = metrics.get("temp_mean", None)
        rpm = metrics.get("rpm_mean", None)
        vib = metrics.get("vib_rms", None)

        st_temp, st_rpm, st_vib = estado_por_metrica(temp, rpm, vib)

        # etiqueta principal estado
        chip(f"{estado.upper()}", color_estado(estado))

        # línea "último ts"
        ts = data.get("ts", "-")
        st.markdown(f"<span class='mini'>Último ts: {ts}</span>", unsafe_allow_html=True)

        # métricas grandes
        m1, m2, m3 = st.columns(3)
        with m1:
            st.metric("Temp media (°C)", fmt_num(temp, 1, ""), delta=None)
        with m2:
            st.metric("RPM media", fmt_num(rpm, 0, ""), delta=None)
        with m3:
            st.metric("Vib RMS", fmt_num(vib, 3, ""), delta=None)

        # chips por métrica
        chip(f"TEMP: {fmt_num(temp, 1, '°C')}", color_estado(st_temp))
        chip(f"RPM: {fmt_num(rpm, 0, ' rpm')}", color_estado(st_rpm))
        chip(f"VIB: {fmt_num(vib, 3, '')}", color_estado(st_vib))

        # razones
        reasons = data.get("reasons", []) or []
        if reasons:
            st.markdown(
                f"<div class='subtarjeta'><b>Razones</b>: {', '.join(reasons)}</div>",
                unsafe_allow_html=True
            )
        else:
            st.markdown(
                f"<div class='subtarjeta'><b>Razones</b>: Funcionamiento correcto</div>",
                unsafe_allow_html=True
            )

        st.markdown("</div>", unsafe_allow_html=True)

st.divider()

# =========================
# Gráficos sobrepuestos (por métrica) - base/hombro/codo juntos
# =========================
st.subheader("Señales (muestras) — gráficos sobrepuestos")

def cargar_samples_por_actuador(act):
    j, err = safe_get(
        f"{API_URL}/api/v1/samples",
        params={"machine_id": machine_id, "actuator_id": act, "limite": LIMITE_SAMPLES},
        timeout=8
    )
    if err:
        return None, err
    items = (j or {}).get("items", [])
    df = pd.DataFrame(items)
    if df.empty:
        return pd.DataFrame(), None
    df["ts"] = pd.to_datetime(df["ts"])
    df = df.sort_values("ts")
    return df, None

dfs = {}
errores = []

for act in ACTUADORES:
    df, err = cargar_samples_por_actuador(act)
    if err:
        errores.append(f"{act}: {err}")
        continue
    dfs[act] = df

if errores:
    st.warning("Algunos actuadores no pudieron cargar samples:\n\n" + "\n".join(errores))

# Unificar en un DF "largo" para gráficas sobrepuestas
def build_overlay(metric_col: str):
    frames = []
    for act, df in dfs.items():
        if df is None or df.empty:
            continue
        tmp = df[["ts", metric_col]].copy()
        tmp["actuator_id"] = act
        frames.append(tmp)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    return out

overlay_temp = build_overlay("motor_temp_c")
overlay_rpm = build_overlay("motor_rpm")
overlay_vib = build_overlay("motor_vibration_rms")

# Para line_chart: pivot index=ts columns=actuator_id values=metric
def plot_overlay(df_long, title, height=260):
    if df_long.empty:
        st.info(f"No hay datos para {title}.")
        return
    dfp = df_long.pivot_table(index="ts", columns="actuator_id", values=df_long.columns[1], aggfunc="last")
    dfp = dfp.sort_index()
    st.write(f"**{title}**")
    st.line_chart(dfp, height=height)

c1, c2, c3 = st.columns(3)
with c1:
    plot_overlay(overlay_temp, "Temperatura (°C)", height=250)
with c2:
    plot_overlay(overlay_rpm, "RPM", height=250)
with c3:
    plot_overlay(overlay_vib, "Vibración RMS", height=250)

st.divider()

# =========================
# Diagnósticos (igual que antes, pero limpio)
# =========================
st.subheader("Historial de diagnósticos (por actuador)")
act_diag = st.selectbox("Actuador", ACTUADORES, index=1)

j, err = safe_get(
    f"{API_URL}/api/v1/diagnostics",
    params={"machine_id": machine_id, "actuator_id": act_diag, "limite": LIMITE_DIAG},
    timeout=8
)

if err:
    st.error(f"No pude leer diagnostics: {err}")
else:
    items = (j or {}).get("items", [])
    dfd = pd.DataFrame(items)
    if dfd.empty:
        st.info("Aún no hay diagnósticos.")
    else:
        dfd["ts"] = pd.to_datetime(dfd["ts"])
        dfd = dfd.sort_values("ts", ascending=False).head(40)
        dfd["ts"] = dfd["ts"].dt.strftime("%H:%M:%S")
        dfd["reasons"] = dfd["reasons"].apply(lambda x: ", ".join(x) if isinstance(x, list) else str(x))
        st.dataframe(dfd[["ts", "machine_id", "actuator_id", "state", "reasons"]], width="stretch", height=320)

# =========================
# Auto-refresh fijo 1s
# =========================
if AUTO_UI:
    time.sleep(INTERVALO_UI_SEG)
    st.rerun()
