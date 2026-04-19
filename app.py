import io
import sqlite3
from datetime import datetime, date, time, timedelta

import pandas as pd
import plotly.express as px
import streamlit as st

DB_NAME = "production.db"
DT_FORMAT = "%Y-%m-%d %H:%M"
DEFAULT_DOWNTIME_CATEGORIES = [
    "Breakdown",
    "Planned Maintenance",
    "Operational",
    "Administrative",
]
DEFAULT_OEE_IMPACTS = ["Availability", "Utilization", "Both"]


def get_conn():
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def deduction_multiplier_from_percent(percent):
    return round(1 - (float(percent or 0) / 100.0), 6)


def parse_dt_input(value):
    value = str(value).strip()
    if not value:
        raise ValueError("Datetime is required.")
    for fmt in ["%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"]:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            pass
    raise ValueError("Use datetime like 2026-04-17 03:29")


def fmt_dt(dt_obj):
    return dt_obj.strftime(DT_FORMAT)


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        '''CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            role TEXT NOT NULL CHECK(role IN ('admin','operator')),
            active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )'''
    )
    cur.execute(
        '''CREATE TABLE IF NOT EXISTS materials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            bulk_density REAL NOT NULL DEFAULT 1.0,
            recirculation_factor REAL NOT NULL DEFAULT 1.0,
            active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )'''
    )
    cur.execute(
        '''CREATE TABLE IF NOT EXISTS feeding_equipment (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            bucket_volume REAL NOT NULL DEFAULT 1.0,
            active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )'''
    )
    cur.execute(
        '''CREATE TABLE IF NOT EXISTS machines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            machine_type TEXT NOT NULL CHECK(machine_type IN ('plant','mobile')),
            area TEXT NOT NULL DEFAULT 'Plant',
            display_order INTEGER NOT NULL DEFAULT 0,
            active INTEGER NOT NULL DEFAULT 1,
            apply_recirculation INTEGER NOT NULL DEFAULT 1,
            deduction_percent REAL NOT NULL DEFAULT 0,
            current_totalizer_start INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )'''
    )
    cur.execute(
        '''CREATE TABLE IF NOT EXISTS machine_deduction_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            machine_id INTEGER NOT NULL,
            deduction_percent REAL NOT NULL,
            deduction_multiplier REAL NOT NULL,
            effective_from TEXT NOT NULL,
            changed_by TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(machine_id) REFERENCES machines(id)
        )'''
    )
    cur.execute(
        '''CREATE TABLE IF NOT EXISTS production (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            production_date TEXT NOT NULL,
            shift TEXT NOT NULL,
            hour_label TEXT NOT NULL,
            hour_index INTEGER NOT NULL DEFAULT 0,
            machine_id INTEGER NOT NULL,
            material_id INTEGER,
            equipment_id INTEGER,
            loads REAL NOT NULL DEFAULT 0,
            ton_per_load REAL NOT NULL DEFAULT 0,
            input_tons REAL NOT NULL DEFAULT 0,
            output_tons REAL NOT NULL DEFAULT 0,
            recirculation_factor REAL NOT NULL DEFAULT 1.0,
            deduction_percent REAL NOT NULL DEFAULT 0,
            deduction_multiplier REAL NOT NULL DEFAULT 1.0,
            current_totalizer INTEGER,
            previous_totalizer INTEGER,
            comments TEXT,
            created_by TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(production_date, shift, hour_label, machine_id)
        )'''
    )
    cur.execute(
        '''CREATE TABLE IF NOT EXISTS downtime_reason_master (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            machine_id INTEGER NOT NULL,
            category TEXT NOT NULL,
            equipment TEXT NOT NULL,
            reason TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(machine_id, category, equipment, reason)
        )'''
    )
    cur.execute(
        '''CREATE TABLE IF NOT EXISTS downtime (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            machine_id INTEGER NOT NULL,
            stop_datetime TEXT NOT NULL,
            start_datetime TEXT,
            category TEXT,
            equipment TEXT,
            cause TEXT NOT NULL,
            comments TEXT,
            is_open INTEGER NOT NULL DEFAULT 1,
            created_by TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )'''
    )

    existing_dt_cols = [r[1] for r in cur.execute("PRAGMA table_info(downtime)").fetchall()]
    if 'category' not in existing_dt_cols:
        cur.execute("ALTER TABLE downtime ADD COLUMN category TEXT")
    if 'equipment' not in existing_dt_cols:
        cur.execute("ALTER TABLE downtime ADD COLUMN equipment TEXT")

    cur.execute("SELECT COUNT(*) FROM users")
    if cur.fetchone()[0] == 0:
        cur.executemany(
            "INSERT INTO users(username,password,role) VALUES(?,?,?)",
            [("admin", "admin123", "admin"), ("operator", "operator123", "operator")],
        )

    cur.execute("SELECT COUNT(*) FROM materials")
    if cur.fetchone()[0] == 0:
        cur.executemany(
            "INSERT INTO materials(name,bulk_density,recirculation_factor,active) VALUES(?,?,?,1)",
            [("DSO 1", 1.80, 1.00), ("DSO 2", 1.90, 0.75), ("Crusher Run", 1.65, 1.00)],
        )

    cur.execute("SELECT COUNT(*) FROM feeding_equipment")
    if cur.fetchone()[0] == 0:
        cur.executemany(
            "INSERT INTO feeding_equipment(name,bucket_volume,active) VALUES(?,?,1)",
            [("Excavator 1", 5.0), ("Loader 1", 3.5)],
        )

    cur.execute("SELECT COUNT(*) FROM machines")
    if cur.fetchone()[0] == 0:
        seed = [
            ("Metso", "plant", "Plant", 1, 1, 2.5, 0),
            ("Mobile Crusher", "mobile", "North Pit", 2, 1, 0.0, 0),
            ("Finlay 2", "mobile", "North Pit", 3, 1, 0.0, 0),
        ]
        for name, mtype, area, display_order, apply_recirc, ded, totalizer_start in seed:
            cur.execute(
                "INSERT INTO machines(name,machine_type,area,display_order,active,apply_recirculation,deduction_percent,current_totalizer_start) VALUES(?,?,?,?,1,?,?,?)",
                (name, mtype, area, display_order, apply_recirc, ded, totalizer_start),
            )
            mid = cur.lastrowid
            cur.execute(
                "INSERT INTO machine_deduction_history(machine_id,deduction_percent,deduction_multiplier,effective_from,changed_by) VALUES(?,?,?,?,?)",
                (mid, ded, deduction_multiplier_from_percent(ded), date.today().isoformat(), "system"),
            )

    seed_downtime_master(cur)
    conn.commit()
    conn.close()


def seed_downtime_master(cur):
    machines = cur.execute("SELECT id,name FROM machines WHERE active=1").fetchall()
    for machine in machines:
        machine_id = machine[0]
        existing = cur.execute("SELECT COUNT(*) FROM downtime_reason_master WHERE machine_id=?", (machine_id,)).fetchone()[0]
        if existing == 0:
            defaults = [
                (machine_id, "Breakdown", machine[1], "Mechanical"),
                (machine_id, "Breakdown", machine[1], "Electrical"),
                (machine_id, "Breakdown", machine[1], "Hydraulic"),
                (machine_id, "Planned Maintenance", machine[1], "Scheduled Service"),
                (machine_id, "Operational", machine[1], "No Feed"),
                (machine_id, "Operational", machine[1], "Waiting Loader"),
                (machine_id, "Administrative", machine[1], "Shift Change"),
                (machine_id, "Administrative", machine[1], "Safety Meeting"),
            ]
            cur.executemany(
                "INSERT OR IGNORE INTO downtime_reason_master(machine_id,category,equipment,reason,active) VALUES(?,?,?,?,1)",
                defaults,
            )


def read_df(query, params=None):
    conn = get_conn()
    df = pd.read_sql_query(query, conn, params=params or [])
    conn.close()
    return df


def execute(query, params=None):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(query, params or [])
    conn.commit()
    lastrowid = cur.lastrowid
    conn.close()
    return lastrowid


def ensure_runtime_migrations():
    conn = get_conn()
    conn.execute("""CREATE TABLE IF NOT EXISTS downtime_equipment_master(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        machine_id INTEGER NOT NULL,
        equipment_name TEXT NOT NULL,
        active INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(machine_id, equipment_name),
        FOREIGN KEY(machine_id) REFERENCES machines(id)
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS downtime_reason_assignment(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        reason_id INTEGER NOT NULL,
        machine_id INTEGER NOT NULL,
        downtime_equipment_id INTEGER NOT NULL,
        active INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(reason_id, machine_id, downtime_equipment_id),
        FOREIGN KEY(reason_id) REFERENCES downtime_reason_master(id),
        FOREIGN KEY(machine_id) REFERENCES machines(id),
        FOREIGN KEY(downtime_equipment_id) REFERENCES downtime_equipment_master(id)
    )""")
    cols = [r[1] for r in conn.execute("PRAGMA table_info(downtime_reason_master)").fetchall()]
    if cols:
        if 'oee_impact' not in cols:
            conn.execute("ALTER TABLE downtime_reason_master ADD COLUMN oee_impact TEXT NOT NULL DEFAULT 'Availability'")
    else:
        conn.execute("""CREATE TABLE IF NOT EXISTS downtime_reason_master(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category TEXT NOT NULL,
            reason TEXT NOT NULL,
            oee_impact TEXT NOT NULL DEFAULT 'Availability',
            active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(category, reason)
        )""")
    conn.commit()
    conn.close()


def authenticate(username, password):
    df = read_df("SELECT * FROM users WHERE username=? AND password=? AND active=1", [username, password])
    if df.empty:
        return None
    return df.iloc[0].to_dict()


def production_hour_options():
    labels = []
    base = datetime.combine(date.today(), time(6, 0))
    for i in range(24):
        a = base + timedelta(hours=i)
        b = a + timedelta(hours=1)
        labels.append(f"{a.strftime('%H:%M')}-{b.strftime('%H:%M')}")
    return labels


def hour_to_index(hour_label):
    return production_hour_options().index(hour_label)


def get_shift_from_hour_label(hour_label):
    start_hour = int(hour_label.split(":")[0])
    return "Day" if 6 <= start_hour < 18 else "Night"


def get_production_day_start(selected_date):
    return datetime.combine(selected_date, time(6, 0))


def get_hour_window(selected_date, hour_label):
    idx = hour_to_index(hour_label)
    start = get_production_day_start(selected_date) + timedelta(hours=idx)
    end = start + timedelta(hours=1)
    return start, end


def export_bytes(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="data", index=False)
    return output.getvalue()


def add_refresh_button():
    if st.button("🔄 Refresh page"):
        st.rerun()


def init_page_state():
    defaults = {"prod_date": date.today(), "prod_hour": production_hour_options()[0]}
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value
    if "bulk_prod_defaults" not in st.session_state:
        st.session_state.bulk_prod_defaults = {}


def get_previous_totalizer(machine_id, production_date, hour_index):
    df = read_df(
        """SELECT current_totalizer FROM production
           WHERE machine_id=? AND (
               production_date < ? OR (production_date = ? AND hour_index < ?)
           )
           AND current_totalizer IS NOT NULL
           ORDER BY production_date DESC, hour_index DESC LIMIT 1""",
        [machine_id, production_date, production_date, hour_index],
    )
    if not df.empty and pd.notna(df.iloc[0]["current_totalizer"]):
        return int(df.iloc[0]["current_totalizer"])
    machine_df = read_df("SELECT current_totalizer_start FROM machines WHERE id=?", [machine_id])
    return int(machine_df.iloc[0]["current_totalizer_start"]) if not machine_df.empty else 0


def get_existing_production(machine_id, production_date, shift, hour_label):
    df = read_df("SELECT * FROM production WHERE machine_id=? AND production_date=? AND shift=? AND hour_label=?", [machine_id, production_date, shift, hour_label])
    return None if df.empty else df.iloc[0].to_dict()


def save_or_replace_production(payload, replace=False):
    existing = get_existing_production(payload["machine_id"], payload["production_date"], payload["shift"], payload["hour_label"])
    if existing and not replace:
        return "exists"
    if existing and replace:
        execute("DELETE FROM production WHERE id=?", [existing["id"]])
    execute(
        '''INSERT INTO production(
            production_date,shift,hour_label,hour_index,machine_id,material_id,equipment_id,
            loads,ton_per_load,input_tons,output_tons,recirculation_factor,
            deduction_percent,deduction_multiplier,current_totalizer,previous_totalizer,comments,created_by
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
        [payload["production_date"], payload["shift"], payload["hour_label"], payload["hour_index"], payload["machine_id"], payload["material_id"], payload["equipment_id"], payload["loads"], payload["ton_per_load"], payload["input_tons"], payload["output_tons"], payload["recirculation_factor"], payload["deduction_percent"], payload["deduction_multiplier"], payload["current_totalizer"], payload["previous_totalizer"], payload["comments"], payload["created_by"]],
    )
    return "saved"


def get_hourly_production(machine_id, selected_date, hour_label):
    df = read_df("SELECT output_tons, comments FROM production WHERE machine_id=? AND production_date=? AND hour_label=?", [machine_id, selected_date.isoformat(), hour_label])
    if df.empty:
        return 0.0, ""
    row = df.iloc[0]
    return float(row["output_tons"] or 0), "" if pd.isna(row["comments"]) else str(row["comments"])


def get_cumulative_production(machine_id, selected_date, hour_label):
    idx = hour_to_index(hour_label)
    df = read_df("SELECT SUM(output_tons) AS total FROM production WHERE machine_id=? AND production_date=? AND hour_index<=?", [machine_id, selected_date.isoformat(), idx])
    if df.empty or pd.isna(df.iloc[0]["total"]):
        return 0.0
    return float(df.iloc[0]["total"])


def overlap_minutes(a_start, a_end, b_start, b_end):
    start = max(a_start, b_start)
    end = min(a_end, b_end)
    return max(0.0, (end - start).total_seconds() / 60.0)


def get_downtime_for_hour(machine_id, selected_date, hour_label):
    hour_start, hour_end = get_hour_window(selected_date, hour_label)
    df = read_df("SELECT * FROM downtime WHERE machine_id=?", [machine_id])
    total_minutes = 0.0
    reasons = []
    status = "On"
    for _, row in df.iterrows():
        dt_stop = datetime.fromisoformat(row["stop_datetime"])
        dt_start = datetime.fromisoformat(row["start_datetime"]) if pd.notna(row["start_datetime"]) and row["start_datetime"] else None
        effective_end = dt_start if dt_start else hour_end
        mins = overlap_minutes(dt_stop, effective_end, hour_start, hour_end)
        if mins > 0:
            total_minutes += mins
            parts = [str(row.get("category") or ""), str(row.get("equipment") or ""), str(row.get("cause") or "")]
            parts = [p for p in parts if p and p != 'None']
            reason_text = " / ".join(parts)
            if pd.notna(row["comments"]) and str(row["comments"]).strip():
                reason_text = f"{reason_text} - {row['comments']}"
            reasons.append(reason_text)
        if dt_stop < hour_end and (dt_start is None or dt_start > hour_start):
            status = "Off"
    return round(total_minutes, 1), " | ".join(reasons), status


def build_home_summary(selected_date, selected_hour):
    machines = read_df("SELECT * FROM machines WHERE active=1 ORDER BY display_order,name")
    rows = []
    for _, machine in machines.iterrows():
        hourly_prod, prod_comment = get_hourly_production(int(machine["id"]), selected_date, selected_hour)
        cumulative_prod = get_cumulative_production(int(machine["id"]), selected_date, selected_hour)
        dt_minutes, dt_reason, status = get_downtime_for_hour(int(machine["id"]), selected_date, selected_hour)
        rows.append({"Machine": machine["name"], "Type": machine["machine_type"], "Current Hour Production": round(hourly_prod, 2), "Cumulative Production": round(cumulative_prod, 2), "Downtime Minutes": round(dt_minutes, 1), "Status": status, "Downtime Reason": dt_reason, "Comments": prod_comment})
    return pd.DataFrame(rows)


def category_chart_df(selected_date, machine_type):
    hours = production_hour_options()
    machines = read_df("SELECT id,name FROM machines WHERE active=1 AND machine_type=? ORDER BY display_order,name", [machine_type])
    prod = read_df(
        '''SELECT p.hour_label,p.hour_index,m.name AS machine,SUM(p.output_tons) AS output_tons
           FROM production p JOIN machines m ON p.machine_id=m.id
           WHERE p.production_date=? AND m.machine_type=?
           GROUP BY p.hour_label,p.hour_index,m.name
           ORDER BY p.hour_index,m.name''',
        [selected_date.isoformat(), machine_type],
    )
    rows = []
    for _, m in machines.iterrows():
        for idx, hr in enumerate(hours):
            match = prod[(prod['machine'] == m['name']) & (prod['hour_label'] == hr)]
            val = 0.0 if match.empty else float(match.iloc[0]['output_tons'] or 0)
            rows.append({'hour_label': hr, 'hour_index': idx, 'machine': m['name'], 'machine_type': machine_type, 'output_tons': val})
    return pd.DataFrame(rows)


def production_totals_for_period(day_value=None, month_value=None, year_value=None):
    query = "SELECT COALESCE(SUM(output_tons),0) AS total FROM production WHERE 1=1"
    params = []
    if day_value is not None:
        query += " AND production_date=?"
        params.append(day_value.isoformat() if hasattr(day_value, 'isoformat') else str(day_value))
    if month_value is not None:
        query += " AND substr(production_date,1,7)=?"
        params.append(month_value)
    if year_value is not None:
        query += " AND substr(production_date,1,4)=?"
        params.append(str(year_value))
    df = read_df(query, params)
    return 0.0 if df.empty else float(df.iloc[0]['total'] or 0)


def get_downtime_master(machine_id, category=None, equipment=None):
    query = """
        SELECT a.id AS assignment_id,
               r.id AS reason_id,
               r.category,
               e.id AS equipment_id,
               e.equipment_name AS equipment,
               r.reason,
               r.oee_impact
        FROM downtime_reason_assignment a
        JOIN downtime_reason_master r ON a.reason_id = r.id
        JOIN downtime_equipment_master e ON a.downtime_equipment_id = e.id
        WHERE a.machine_id=?
          AND a.active=1
          AND r.active=1
          AND e.active=1
    """
    params = [machine_id]
    if category:
        query += " AND r.category=?"
        params.append(category)
    if equipment:
        query += " AND e.equipment_name=?"
        params.append(equipment)
    query += " ORDER BY r.category, e.equipment_name, r.reason"
    return read_df(query, params)


def home_page():
    st.title("🏭 Production Dashboard")
    add_refresh_button()
    c1, c2 = st.columns(2)
    with c1:
        selected_date = st.date_input("Production day", value=date.today(), key="home_date")
    with c2:
        selected_hour = st.selectbox("Selected hour", production_hour_options(), key="home_hour")

    today_total = production_totals_for_period(day_value=selected_date)
    month_total = production_totals_for_period(month_value=selected_date.strftime("%Y-%m"))
    year_total = production_totals_for_period(year_value=selected_date.year)

    k1, k2, k3 = st.columns(3)
    k1.metric("Day production", f"{today_total:,.2f} t")
    k2.metric("Month production", f"{month_total:,.2f} t")
    k3.metric("Year production", f"{year_total:,.2f} t")

    plant_df = category_chart_df(selected_date, "plant")
    mobile_df = category_chart_df(selected_date, "mobile")
    if not plant_df.empty:
        st.subheader("Plant production by hour")
        fig_plant = px.bar(plant_df, x="hour_label", y="output_tons", color="machine", barmode="group", text="output_tons")
        fig_plant.update_traces(texttemplate='%{text:.0f}', textposition='outside')
        fig_plant.update_yaxes(rangemode="tozero", title_text="Production")
        fig_plant.update_xaxes(title_text="Hour", categoryorder='array', categoryarray=production_hour_options())
        st.plotly_chart(fig_plant, width="stretch")
    if not mobile_df.empty:
        st.subheader("Mobile production by hour")
        fig_mobile = px.bar(mobile_df, x="hour_label", y="output_tons", color="machine", barmode="group", text="output_tons")
        fig_mobile.update_traces(texttemplate='%{text:.0f}', textposition='outside')
        fig_mobile.update_yaxes(rangemode="tozero", title_text="Production")
        fig_mobile.update_xaxes(title_text="Hour", categoryorder='array', categoryarray=production_hour_options())
        st.plotly_chart(fig_mobile, width="stretch")


def production_page(user):
    st.title("🟢 Production Entry")
    add_refresh_button()
    machines = read_df("SELECT * FROM machines WHERE active=1 ORDER BY display_order,name")
    materials = read_df("SELECT * FROM materials WHERE active=1 ORDER BY name")
    equipment = read_df("SELECT * FROM feeding_equipment WHERE active=1 ORDER BY name")
    if machines.empty:
        st.warning("Please load machines in Settings first.")
        return

    top1, top2 = st.columns(2)
    with top1:
        production_date = st.date_input("Production date", value=st.session_state.prod_date, key="bulk_prod_date")
    with top2:
        hour_label = st.selectbox("Hour", production_hour_options(), index=production_hour_options().index(st.session_state.prod_hour), key="bulk_prod_hour")

    st.session_state.prod_date = production_date
    st.session_state.prod_hour = hour_label
    shift = get_shift_from_hour_label(hour_label)
    hour_index = hour_to_index(hour_label)
    st.caption("One row per machine. Plant: enter totalizer. Mobile: choose feeding equipment and enter buckets. Submit once for the full hour.")

    material_names = materials["name"].tolist() if not materials.empty else []
    equipment_names = equipment["name"].tolist() if not equipment.empty else []

    for _, machine in machines.iterrows():
        machine_id = int(machine["id"])
        machine_name = machine["name"]
        machine_type = machine["machine_type"]
        existing = get_existing_production(machine_id, production_date.isoformat(), shift, hour_label)
        if machine_id not in st.session_state.bulk_prod_defaults:
            st.session_state.bulk_prod_defaults[machine_id] = {}
        defaults = st.session_state.bulk_prod_defaults[machine_id]
        if existing:
            defaults["comments"] = existing.get("comments") or defaults.get("comments", "")
            if machine_type == "plant":
                defaults["totalizer"] = int(existing.get("current_totalizer") or 0)
            else:
                defaults["loads"] = float(existing.get("loads") or 0)
                if existing.get("equipment_id"):
                    eq_df = equipment[equipment["id"] == int(existing.get("equipment_id"))]
                    if not eq_df.empty:
                        defaults["equipment"] = eq_df.iloc[0]["name"]
                if existing.get("material_id"):
                    mat_df = materials[materials["id"] == int(existing.get("material_id"))]
                    if not mat_df.empty:
                        defaults["material"] = mat_df.iloc[0]["name"]

        with st.container(border=True):
            st.markdown(f"**{machine_name}** ({machine_type})")
            cols = st.columns([1.2, 1.2, 1.2, 1.4, 1.4, 1.8])
            if machine_type == "plant":
                previous_totalizer = get_previous_totalizer(machine_id, production_date.isoformat(), hour_index)
                totalizer_key = f"bulk_totalizer_{machine_id}"
                if totalizer_key not in st.session_state:
                    st.session_state[totalizer_key] = int(defaults.get("totalizer", previous_totalizer))
                cols[0].number_input("Totalizer", min_value=0, step=1, key=totalizer_key)
                cols[1].text_input("Previous", value=str(previous_totalizer), disabled=True, key=f"bulk_prev_{machine_id}")
                est_output = int(st.session_state[totalizer_key]) - int(previous_totalizer)
                cols[2].text_input("Production", value=f"{max(est_output, 0):,.0f}", disabled=True, key=f"bulk_est_{machine_id}")
                cols[3].write("")
                cols[4].write("")
            else:
                mat_key = f"bulk_material_{machine_id}"
                eq_key = f"bulk_equipment_{machine_id}"
                loads_key = f"bulk_loads_{machine_id}"
                if material_names and mat_key not in st.session_state:
                    st.session_state[mat_key] = defaults.get("material", material_names[0])
                if equipment_names and eq_key not in st.session_state:
                    st.session_state[eq_key] = defaults.get("equipment", equipment_names[0])
                if loads_key not in st.session_state:
                    st.session_state[loads_key] = float(defaults.get("loads", 0.0))
                cols[0].selectbox("Material", material_names, key=mat_key)
                cols[1].selectbox("Equipment", equipment_names, key=eq_key)
                cols[2].number_input("Buckets", min_value=0.0, step=1.0, key=loads_key)
                selected_material = materials.loc[materials["name"] == st.session_state[mat_key]].iloc[0]
                selected_equipment = equipment.loc[equipment["name"] == st.session_state[eq_key]].iloc[0]
                preview = float(st.session_state[loads_key]) * float(selected_equipment["bucket_volume"]) * float(selected_material["bulk_density"]) * deduction_multiplier_from_percent(float(machine["deduction_percent"]))
                cols[3].text_input("Production", value=f"{preview:,.2f}", disabled=True, key=f"bulk_est_{machine_id}")
                cols[4].text_input("Deduction %", value=f"{float(machine['deduction_percent']):.1f}", disabled=True, key=f"bulk_ded_{machine_id}")
            comments_key = f"bulk_comments_{machine_id}"
            if comments_key not in st.session_state:
                st.session_state[comments_key] = defaults.get("comments", "")
            cols[5].text_input("Comments", key=comments_key)

    if st.button("Submit production for selected hour", type="primary"):
        errors = []
        saved_count = 0
        for _, machine in machines.iterrows():
            machine_id = int(machine["id"])
            machine_name = machine["name"]
            machine_type = machine["machine_type"]
            comments = st.session_state.get(f"bulk_comments_{machine_id}", "")
            payload = {
                "production_date": production_date.isoformat(), "shift": shift, "hour_label": hour_label, "hour_index": hour_index,
                "machine_id": machine_id, "material_id": None, "equipment_id": None, "loads": 0.0, "ton_per_load": 0.0,
                "input_tons": 0.0, "output_tons": 0.0, "recirculation_factor": 1.0, "deduction_percent": 0.0,
                "deduction_multiplier": 1.0, "current_totalizer": None, "previous_totalizer": None,
                "comments": comments, "created_by": user["username"],
            }
            try:
                if machine_type == "plant":
                    current_totalizer = int(st.session_state.get(f"bulk_totalizer_{machine_id}", 0))
                    previous_totalizer = get_previous_totalizer(machine_id, production_date.isoformat(), hour_index)
                    output_tons = current_totalizer - int(previous_totalizer)
                    if output_tons < 0:
                        errors.append(f"{machine_name}: totalizer is smaller than previous totalizer.")
                        continue
                    payload.update({"input_tons": float(output_tons), "output_tons": float(output_tons), "current_totalizer": int(current_totalizer), "previous_totalizer": int(previous_totalizer)})
                else:
                    material_name = st.session_state.get(f"bulk_material_{machine_id}")
                    equipment_name = st.session_state.get(f"bulk_equipment_{machine_id}")
                    loads = float(st.session_state.get(f"bulk_loads_{machine_id}", 0.0))
                    material = materials.loc[materials["name"] == material_name].iloc[0]
                    feed = equipment.loc[equipment["name"] == equipment_name].iloc[0]
                    bulk_density = float(material["bulk_density"])
                    bucket_volume = float(feed["bucket_volume"])
                    deduction_percent = float(machine["deduction_percent"])
                    deduction_multiplier = deduction_multiplier_from_percent(deduction_percent)
                    input_tons = loads * bucket_volume * bulk_density
                    output_tons = round(input_tons * deduction_multiplier, 3)
                    payload.update({"material_id": int(material["id"]), "equipment_id": int(feed["id"]), "loads": loads, "ton_per_load": round(bucket_volume * bulk_density, 3), "input_tons": input_tons, "output_tons": output_tons, "deduction_percent": deduction_percent, "deduction_multiplier": deduction_multiplier})
                save_or_replace_production(payload, replace=True)
                saved_count += 1
            except Exception as e:
                errors.append(f"{machine_name}: {e}")
        if errors:
            for err in errors:
                st.error(err)
        if saved_count:
            st.success(f"Saved production for {saved_count} machines for {production_date.isoformat()} {hour_label}.")
            st.rerun()


def downtime_page(user):
    ensure_runtime_migrations()
    st.title("🔴 Downtime Board")
    add_refresh_button()
    machines = read_df("SELECT * FROM machines WHERE active=1 ORDER BY display_order,name")
    open_dt = read_df("SELECT d.*, m.name AS machine_name FROM downtime d JOIN machines m ON d.machine_id=m.id WHERE d.is_open=1 ORDER BY m.display_order, m.name")
    if machines.empty:
        st.warning("Please add machines in Settings first.")
        return
    st.caption("Choose category, equipment and reason per machine. Use the date picker for date and type the time manually with keyboard.")

    for _, machine in machines.iterrows():
        machine_id = int(machine["id"])
        machine_name = machine["name"]
        open_row = open_dt.loc[open_dt["machine_id"] == machine_id]
        has_open = not open_row.empty
        master = get_downtime_master(machine_id)
        if not master.empty:
            master = master.drop_duplicates(subset=["category", "equipment", "reason"]).reset_index(drop=True)
        if master.empty:
            with st.container(border=True):
                st.markdown(f"**{machine_name}**")
                st.info("No downtime assignments configured yet for this machine.")
            continue
        categories = sorted(master['category'].dropna().unique().tolist())
        cat_key = f"dt_cat_{machine_id}"
        eq_key = f"dt_eq_{machine_id}"
        reason_key = f"dt_reason_{machine_id}"
        stop_date_key = f"stop_date_{machine_id}"
        stop_time_key = f"stop_time_{machine_id}"
        start_date_key = f"start_date_{machine_id}"
        start_time_key = f"start_time_{machine_id}"
        detail_key = f"detail_{machine_id}"
        now_dt = datetime.now().replace(second=0, microsecond=0)
        if cat_key not in st.session_state or st.session_state[cat_key] not in categories:
            st.session_state[cat_key] = categories[0]
        if stop_date_key not in st.session_state:
            st.session_state[stop_date_key] = now_dt.date()
        if stop_time_key not in st.session_state:
            st.session_state[stop_time_key] = now_dt.strftime('%H:%M')
        if start_date_key not in st.session_state:
            st.session_state[start_date_key] = now_dt.date()
        if start_time_key not in st.session_state:
            st.session_state[start_time_key] = now_dt.strftime('%H:%M')
        if detail_key not in st.session_state:
            st.session_state[detail_key] = ""
        filtered_eq = get_downtime_master(machine_id, category=st.session_state[cat_key])
        if not filtered_eq.empty:
            filtered_eq = filtered_eq.drop_duplicates(subset=["equipment"]).reset_index(drop=True)
        eq_options = sorted(filtered_eq['equipment'].dropna().unique().tolist())
        if eq_key not in st.session_state or st.session_state[eq_key] not in eq_options:
            st.session_state[eq_key] = eq_options[0]
        filtered_reason = get_downtime_master(machine_id, category=st.session_state[cat_key], equipment=st.session_state[eq_key])
        reason_options = sorted(filtered_reason['reason'].dropna().unique().tolist())
        if reason_key not in st.session_state or st.session_state[reason_key] not in reason_options:
            st.session_state[reason_key] = reason_options[0]
        with st.container(border=True):
            st.markdown(f"**{machine_name}**")
            row1 = st.columns([0.8, 1.1, 1.2, 1.2, 1.0, 0.8, 0.8])
            row1[0].write("Off" if has_open else "On")
            row1[1].selectbox("Category", categories, key=cat_key)
            filtered_eq = get_downtime_master(machine_id, category=st.session_state[cat_key])
            if not filtered_eq.empty:
                filtered_eq = filtered_eq.drop_duplicates(subset=["equipment"]).reset_index(drop=True)
            eq_options = sorted(filtered_eq['equipment'].dropna().unique().tolist())
            if st.session_state[eq_key] not in eq_options:
                st.session_state[eq_key] = eq_options[0]
            row1[2].selectbox("Equipment", eq_options, key=eq_key)
            filtered_reason = get_downtime_master(machine_id, category=st.session_state[cat_key], equipment=st.session_state[eq_key])
            if not filtered_reason.empty:
                filtered_reason = filtered_reason.drop_duplicates(subset=["reason"]).reset_index(drop=True)
            reason_options = sorted(filtered_reason['reason'].dropna().unique().tolist())
            if st.session_state[reason_key] not in reason_options:
                st.session_state[reason_key] = reason_options[0]
            row1[3].selectbox("Reason", reason_options, key=reason_key)
            row1[4].date_input("Stop date", key=stop_date_key, format="YYYY-MM-DD")
            row1[5].text_input("Stop time", key=stop_time_key, placeholder="HH:MM")
            stop_clicked = row1[6].button("Stop", key=f"stopbtn_{machine_id}")
            if has_open:
                open_item = open_row.iloc[0]
                open_stop_dt = datetime.fromisoformat(str(open_item["stop_datetime"]))
                st.caption(f"Open since {str(open_item['stop_datetime']).replace('T', ' ')}")
                st.session_state[start_date_key] = open_stop_dt.date()
            else:
                st.caption("No open downtime")
            row2 = st.columns([1.0, 0.8, 2.4, 0.8])
            row2[0].date_input("Start date", key=start_date_key, format="YYYY-MM-DD")
            row2[1].text_input("Start time", key=start_time_key, placeholder="HH:MM")
            row2[2].text_input("Detail / comments", key=detail_key)
            start_clicked = row2[3].button("Start", key=f"resume_{machine_id}")
            if stop_clicked:
                if has_open:
                    st.warning(f"{machine_name} already has an open downtime. Close it with Start first.")
                else:
                    try:
                        stop_dt = parse_dt_input(f"{st.session_state[stop_date_key]} {st.session_state[stop_time_key]}")
                    except ValueError as e:
                        st.error(f"{machine_name}: {e}")
                    else:
                        execute("INSERT INTO downtime(machine_id,stop_datetime,category,equipment,cause,comments,is_open,created_by) VALUES(?,?,?,?,?,?,1,?)", [machine_id, stop_dt.isoformat(timespec='minutes'), st.session_state[cat_key], st.session_state[eq_key], st.session_state[reason_key], st.session_state.get(detail_key, ''), user["username"]])
                        st.success(f"Downtime started for {machine_name}.")
                        st.rerun()
            if start_clicked:
                if not has_open:
                    st.warning(f"{machine_name} has no open downtime to close.")
                else:
                    try:
                        start_dt = parse_dt_input(f"{st.session_state[start_date_key]} {st.session_state[start_time_key]}")
                        open_stop_dt = datetime.fromisoformat(str(open_item["stop_datetime"]))
                    except ValueError as e:
                        st.error(f"{machine_name}: {e}")
                    else:
                        if start_dt <= open_stop_dt:
                            st.error(f"{machine_name}: Start must be after Stop.")
                        else:
                            execute("UPDATE downtime SET start_datetime=?, comments=COALESCE(NULLIF(?,''),comments), is_open=0 WHERE id=?", [start_dt.isoformat(timespec='minutes'), st.session_state.get(detail_key, ''), int(open_item["id"])])
                            st.success(f"Downtime closed for {machine_name}.")
                            st.rerun()
    history = read_df("""SELECT d.id,m.name AS machine,d.stop_datetime,d.start_datetime,d.category,d.equipment,d.cause,d.comments,
                  CASE WHEN d.start_datetime IS NULL THEN NULL ELSE ROUND((julianday(d.start_datetime)-julianday(d.stop_datetime))*24*60,1) END AS duration_minutes,
                  d.is_open,d.created_by
           FROM downtime d JOIN machines m ON d.machine_id=m.id ORDER BY d.stop_datetime DESC""")
    if not history.empty:
        st.subheader("Downtime history")
        st.dataframe(history, width="stretch")

def analysis_page():
    st.title("📊 Analysis")
    add_refresh_button()
    top1, top2 = st.columns(2)
    with top1:
        selected_date = st.date_input("Production day", value=date.today(), key="analysis_date")
    with top2:
        selected_hour = st.selectbox("Hour", production_hour_options(), key="analysis_hour")
    summary = build_home_summary(selected_date, selected_hour)
    st.subheader("Hourly report")
    st.dataframe(summary, width="stretch", hide_index=True)

    st.subheader("Exports")
    machine_df = read_df("SELECT name FROM machines WHERE active=1 ORDER BY display_order,name")
    machine_names = machine_df['name'].tolist() if not machine_df.empty else []
    f1, f2, f3 = st.columns(3)
    with f1:
        start_date = st.date_input("Start date", value=date.today(), key="export_start")
    with f2:
        end_date = st.date_input("End date", value=date.today(), key="export_end")
    with f3:
        selected_machines = st.multiselect("Machines", machine_names, default=machine_names, key="export_machines")

    prod_query = '''SELECT p.production_date,p.shift,p.hour_label,m.name AS machine,m.machine_type,mat.name AS material,
                           f.name AS feeding_equipment,p.loads,p.ton_per_load,p.input_tons,p.output_tons,
                           p.previous_totalizer,p.current_totalizer,p.comments,p.created_by,p.created_at
                    FROM production p
                    JOIN machines m ON p.machine_id=m.id
                    LEFT JOIN materials mat ON p.material_id=mat.id
                    LEFT JOIN feeding_equipment f ON p.equipment_id=f.id
                    WHERE p.production_date>=? AND p.production_date<=?'''
    down_query = '''SELECT substr(d.stop_datetime,1,10) AS production_date,m.name AS machine,m.machine_type,
                           d.stop_datetime,d.start_datetime,d.category,d.equipment,d.cause,d.comments,
                           CASE WHEN d.start_datetime IS NULL THEN NULL
                                ELSE ROUND((julianday(d.start_datetime)-julianday(d.stop_datetime))*24*60,1)
                           END AS duration_minutes,
                           d.is_open,d.created_by,d.created_at
                    FROM downtime d
                    JOIN machines m ON d.machine_id=m.id
                    WHERE substr(d.stop_datetime,1,10) >= ? AND substr(d.stop_datetime,1,10) <= ?'''
    prod_params = [start_date.isoformat(), end_date.isoformat()]
    down_params = [start_date.isoformat(), end_date.isoformat()]
    if selected_machines:
        placeholders = ",".join(["?" for _ in selected_machines])
        prod_query += f" AND m.name IN ({placeholders})"
        down_query += f" AND m.name IN ({placeholders})"
        prod_params.extend(selected_machines)
        down_params.extend(selected_machines)
    prod_query += " ORDER BY p.production_date,p.hour_index,m.display_order,m.name"
    down_query += " ORDER BY d.stop_datetime DESC"
    prod_export = read_df(prod_query, prod_params)
    down_export = read_df(down_query, down_params)

    c1, c2 = st.columns(2)
    with c1:
        if not prod_export.empty:
            st.download_button("Export production to Excel", data=export_bytes(prod_export), file_name=f"production_export_{start_date.isoformat()}_{end_date.isoformat()}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            st.dataframe(prod_export, width="stretch")
        else:
            st.info("No production data for the selected filters.")
    with c2:
        if not down_export.empty:
            st.download_button("Export downtime to Excel", data=export_bytes(down_export), file_name=f"downtime_export_{start_date.isoformat()}_{end_date.isoformat()}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            st.dataframe(down_export, width="stretch")
        else:
            st.info("No downtime data for the selected filters.")


def settings_page(user):
    ensure_runtime_migrations()
    if user["role"] != "admin":
        st.warning("Only admin can access Settings.")
        return
    st.title("⚙️ Settings")
    add_refresh_button()
    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(["Machines", "Materials", "Feeding Equipment", "Downtime Equipment", "Downtime Reasons", "Downtime Assignments", "Users"])

    with tab1:
        st.subheader("Machines")
        machines = read_df("SELECT * FROM machines ORDER BY display_order,name")
        st.dataframe(machines, width="stretch", hide_index=True)

        st.markdown("### Add machine")
        model_options = ["None"] + machines["name"].tolist() if not machines.empty else ["None"]
        with st.form("add_machine_form"):
            c1, c2, c3, c4, c5 = st.columns(5)
            with c1:
                add_name = st.text_input("Machine name")
                add_type = st.selectbox("Machine type", ["plant", "mobile"], key="add_machine_type")
            with c2:
                add_area = st.text_input("Area", value="Plant")
                add_order = st.number_input("Display order", min_value=1, step=1, value=max(len(machines) + 1, 1))
            with c3:
                add_apply = st.checkbox("Apply recirculation", value=True)
                add_ded = st.number_input("Deduction %", min_value=0.0, max_value=100.0, step=0.1, value=0.0, key="add_machine_ded")
            with c4:
                add_totalizer = st.number_input("Starting totalizer", min_value=0, step=1, value=0)
                add_active = st.checkbox("Active", value=True, key="add_machine_active")
            with c5:
                copy_model = st.selectbox("Copy downtime setup from", model_options, key="copy_machine_model")
            add_machine = st.form_submit_button("Add machine")
        if add_machine:
            if not add_name.strip():
                st.warning("Machine name is required.")
            else:
                existing = read_df("SELECT id FROM machines WHERE LOWER(name)=LOWER(?)", [add_name.strip()])
                if not existing.empty:
                    st.warning("Machine already exists.")
                else:
                    new_id = execute("INSERT INTO machines(name,machine_type,area,display_order,active,apply_recirculation,deduction_percent,current_totalizer_start) VALUES(?,?,?,?,?,?,?,?)", [add_name.strip(), add_type, add_area.strip(), int(add_order), 1 if add_active else 0, 1 if add_apply else 0, float(add_ded), int(add_totalizer)])
                    execute("INSERT INTO machine_deduction_history(machine_id,deduction_percent,deduction_multiplier,effective_from,changed_by) VALUES(?,?,?,?,?)", [new_id, float(add_ded), deduction_multiplier_from_percent(add_ded), date.today().isoformat(), user["username"]])
                    if copy_model != "None":
                        model_id = int(machines[machines["name"] == copy_model].iloc[0]["id"])
                        model_equipment = read_df("SELECT equipment_name, active FROM downtime_equipment_master WHERE machine_id=?", [model_id])
                        for _, eq in model_equipment.iterrows():
                            execute("INSERT OR IGNORE INTO downtime_equipment_master(machine_id,equipment_name,active) VALUES(?,?,?)", [new_id, eq["equipment_name"], int(eq["active"])])
                        new_equipment = read_df("SELECT id,equipment_name FROM downtime_equipment_master WHERE machine_id=?", [new_id])
                        model_assign = read_df("""
                            SELECT e.equipment_name, a.reason_id, a.active
                            FROM downtime_reason_assignment a
                            JOIN downtime_equipment_master e ON a.downtime_equipment_id=e.id
                            WHERE a.machine_id=?
                        """, [model_id])
                        for _, ass in model_assign.iterrows():
                            match = new_equipment[new_equipment["equipment_name"] == ass["equipment_name"]]
                            if not match.empty:
                                execute("INSERT OR IGNORE INTO downtime_reason_assignment(reason_id,machine_id,downtime_equipment_id,active) VALUES(?,?,?,?)", [int(ass["reason_id"]), new_id, int(match.iloc[0]["id"]), int(ass["active"])])
                    st.success("Machine added.")
                    st.rerun()

        if not machines.empty:
            st.markdown("### Edit machine")
            selected = st.selectbox("Select machine to edit", machines["name"].tolist(), key="edit_machine_pick")
            row = machines[machines["name"] == selected].iloc[0]
            with st.form("edit_machine_form"):
                c1, c2, c3, c4 = st.columns(4)
                with c1:
                    name = st.text_input("Machine name", value=row["name"])
                    machine_type = st.selectbox("Machine type", ["plant", "mobile"], index=0 if row["machine_type"] == "plant" else 1, key="edit_machine_type")
                with c2:
                    area = st.text_input("Area", value=row["area"])
                    display_order = st.number_input("Display order", min_value=1, step=1, value=int(row["display_order"]))
                with c3:
                    apply_recirculation = st.checkbox("Apply recirculation", value=bool(row["apply_recirculation"]))
                    deduction_percent = st.number_input("Deduction %", min_value=0.0, max_value=100.0, step=0.1, value=float(row["deduction_percent"]), key="edit_machine_ded")
                with c4:
                    totalizer_start = st.number_input("Starting totalizer", min_value=0, step=1, value=int(row["current_totalizer_start"]))
                    active = st.checkbox("Active", value=bool(row["active"]), key="edit_machine_active")
                b1, b2 = st.columns(2)
                save = b1.form_submit_button("Save machine changes")
                delete = b2.form_submit_button("Delete machine")
            if save:
                execute("UPDATE machines SET name=?, machine_type=?, area=?, display_order=?, active=?, apply_recirculation=?, deduction_percent=?, current_totalizer_start=? WHERE id=?", [name.strip(), machine_type, area.strip(), int(display_order), 1 if active else 0, 1 if apply_recirculation else 0, float(deduction_percent), int(totalizer_start), int(row["id"])])
                execute("INSERT INTO machine_deduction_history(machine_id,deduction_percent,deduction_multiplier,effective_from,changed_by) VALUES(?,?,?,?,?)", [int(row["id"]), float(deduction_percent), deduction_multiplier_from_percent(deduction_percent), date.today().isoformat(), user["username"]])
                st.success("Machine updated.")
                st.rerun()
            if delete:
                execute("DELETE FROM downtime WHERE machine_id=?", [int(row["id"])])
                execute("DELETE FROM downtime_reason_assignment WHERE machine_id=?", [int(row["id"])])
                execute("DELETE FROM downtime_equipment_master WHERE machine_id=?", [int(row["id"])])
                execute("DELETE FROM production WHERE machine_id=?", [int(row["id"])])
                execute("DELETE FROM machine_deduction_history WHERE machine_id=?", [int(row["id"])])
                execute("DELETE FROM machines WHERE id=?", [int(row["id"])])
                st.success("Machine deleted.")
                st.rerun()

    with tab2:
        st.subheader("Materials")
        mats = read_df("SELECT * FROM materials ORDER BY name")
        st.dataframe(mats, width="stretch", hide_index=True)

        st.markdown("### Add material")
        with st.form("add_mat_form"):
            c1, c2, c3, c4 = st.columns(4)
            with c1:
                add_name = st.text_input("Material name")
            with c2:
                add_bd = st.number_input("Bulk density", min_value=0.0, step=0.01, value=1.0, key="add_mat_bd")
            with c3:
                add_rf = st.number_input("Recirculation factor", min_value=0.0, step=0.01, value=1.0, key="add_mat_rf")
            with c4:
                add_active = st.checkbox("Active", value=True, key="add_mat_active")
            add_material = st.form_submit_button("Add material")
        if add_material:
            if not add_name.strip():
                st.warning("Material name is required.")
            else:
                existing = read_df("SELECT id FROM materials WHERE LOWER(name)=LOWER(?)", [add_name.strip()])
                if not existing.empty:
                    st.warning("Material already exists.")
                else:
                    execute("INSERT INTO materials(name,bulk_density,recirculation_factor,active) VALUES(?,?,?,?)", [add_name.strip(), float(add_bd), float(add_rf), 1 if add_active else 0])
                    st.success("Material added.")
                    st.rerun()

        if not mats.empty:
            st.markdown("### Edit material")
            selected = st.selectbox("Select material to edit", mats["name"].tolist(), key="edit_mat_pick")
            row = mats[mats["name"] == selected].iloc[0]
            with st.form("edit_mat_form"):
                c1, c2, c3, c4 = st.columns(4)
                with c1:
                    name = st.text_input("Material name", value=row["name"])
                with c2:
                    bulk_density = st.number_input("Bulk density", min_value=0.0, step=0.01, value=float(row["bulk_density"]))
                with c3:
                    recirculation_factor = st.number_input("Recirculation factor", min_value=0.0, step=0.01, value=float(row["recirculation_factor"]))
                with c4:
                    active = st.checkbox("Active", value=bool(row["active"]), key="edit_mat_active")
                b1, b2 = st.columns(2)
                save = b1.form_submit_button("Save material changes")
                delete = b2.form_submit_button("Delete material")
            if save:
                execute("UPDATE materials SET name=?, bulk_density=?, recirculation_factor=?, active=? WHERE id=?", [name.strip(), float(bulk_density), float(recirculation_factor), 1 if active else 0, int(row["id"])])
                st.success("Material updated.")
                st.rerun()
            if delete:
                execute("DELETE FROM materials WHERE id=?", [int(row["id"])])
                st.success("Material deleted.")
                st.rerun()

    with tab3:
        st.subheader("Feeding Equipment")
        equip = read_df("SELECT * FROM feeding_equipment ORDER BY name")
        st.dataframe(equip, width="stretch", hide_index=True)

        st.markdown("### Add feeding equipment")
        with st.form("add_eq_form"):
            c1, c2, c3 = st.columns(3)
            with c1:
                add_name = st.text_input("Equipment name")
            with c2:
                add_bucket = st.number_input("Bucket volume", min_value=0.0, step=0.1, value=1.0, key="add_eq_bucket")
            with c3:
                add_active = st.checkbox("Active", value=True, key="add_eq_active")
            add_equipment = st.form_submit_button("Add equipment")
        if add_equipment:
            if not add_name.strip():
                st.warning("Equipment name is required.")
            else:
                existing = read_df("SELECT id FROM feeding_equipment WHERE LOWER(name)=LOWER(?)", [add_name.strip()])
                if not existing.empty:
                    st.warning("Equipment already exists.")
                else:
                    execute("INSERT INTO feeding_equipment(name,bucket_volume,active) VALUES(?,?,?)", [add_name.strip(), float(add_bucket), 1 if add_active else 0])
                    st.success("Equipment added.")
                    st.rerun()

        if not equip.empty:
            st.markdown("### Edit feeding equipment")
            selected = st.selectbox("Select equipment to edit", equip["name"].tolist(), key="edit_eq_pick")
            row = equip[equip["name"] == selected].iloc[0]
            with st.form("edit_eq_form"):
                c1, c2, c3 = st.columns(3)
                with c1:
                    name = st.text_input("Equipment name", value=row["name"])
                with c2:
                    bucket_volume = st.number_input("Bucket volume", min_value=0.0, step=0.1, value=float(row["bucket_volume"]))
                with c3:
                    active = st.checkbox("Active", value=bool(row["active"]), key="edit_eq_active")
                b1, b2 = st.columns(2)
                save = b1.form_submit_button("Save equipment changes")
                delete = b2.form_submit_button("Delete equipment")
            if save:
                execute("UPDATE feeding_equipment SET name=?, bucket_volume=?, active=? WHERE id=?", [name.strip(), float(bucket_volume), 1 if active else 0, int(row["id"])])
                st.success("Equipment updated.")
                st.rerun()
            if delete:
                execute("DELETE FROM feeding_equipment WHERE id=?", [int(row["id"])])
                st.success("Equipment deleted.")
                st.rerun()

    with tab4:
        st.subheader("Downtime Equipment")
        machine_df = read_df("SELECT id,name FROM machines ORDER BY display_order,name")
        if machine_df.empty:
            st.info("Add machines first.")
        else:
            selected_machine = st.selectbox("Machine", machine_df["name"].tolist(), key="dt_equipment_machine")
            machine_id = int(machine_df[machine_df["name"] == selected_machine].iloc[0]["id"])
            dt_equip = read_df("SELECT id,equipment_name,active,created_at FROM downtime_equipment_master WHERE machine_id=? ORDER BY equipment_name", [machine_id])
            st.dataframe(dt_equip, width="stretch", hide_index=True)

            st.markdown("### Add downtime equipment")
            with st.form("add_dt_equipment_form"):
                c1, c2 = st.columns(2)
                with c1:
                    eq_name = st.text_input("Equipment name", placeholder="Conveyor, Crusher, Screen...")
                with c2:
                    eq_active = st.checkbox("Active", value=True)
                add_dt_eq = st.form_submit_button("Add downtime equipment")
            if add_dt_eq:
                if not eq_name.strip():
                    st.warning("Equipment name is required.")
                else:
                    execute("INSERT OR IGNORE INTO downtime_equipment_master(machine_id,equipment_name,active) VALUES(?,?,?)", [machine_id, eq_name.strip(), 1 if eq_active else 0])
                    st.success("Downtime equipment added.")
                    st.rerun()

            if not dt_equip.empty:
                st.markdown("### Edit downtime equipment")
                picked_eq = st.selectbox("Select downtime equipment to edit", dt_equip["equipment_name"].tolist(), key="edit_dt_eq_pick")
                row = dt_equip[dt_equip["equipment_name"] == picked_eq].iloc[0]
                with st.form("edit_dt_equipment_form"):
                    c1, c2 = st.columns(2)
                    with c1:
                        edit_name = st.text_input("Equipment name", value=row["equipment_name"])
                    with c2:
                        edit_active = st.checkbox("Active", value=bool(row["active"]), key="edit_dt_eq_active")
                    b1, b2 = st.columns(2)
                    save_eq = b1.form_submit_button("Save downtime equipment")
                    delete_eq = b2.form_submit_button("Delete downtime equipment")
                if save_eq:
                    execute("UPDATE downtime_equipment_master SET equipment_name=?, active=? WHERE id=?", [edit_name.strip(), 1 if edit_active else 0, int(row["id"])] )
                    st.success("Downtime equipment updated.")
                    st.rerun()
                if delete_eq:
                    execute("DELETE FROM downtime_reason_assignment WHERE downtime_equipment_id=?", [int(row["id"])])
                    execute("DELETE FROM downtime_equipment_master WHERE id=?", [int(row["id"])])
                    st.success("Downtime equipment deleted.")
                    st.rerun()

    with tab5:
        st.subheader("Downtime Reasons")
        reasons = read_df("SELECT id,category,reason,oee_impact,active,created_at FROM downtime_reason_master ORDER BY category,reason")
        st.dataframe(reasons, width="stretch", hide_index=True)

        st.markdown("### Add downtime reason")
        with st.form("add_dt_reason_form"):
            c1, c2, c3, c4 = st.columns(4)
            with c1:
                category = st.selectbox("Category", DEFAULT_DOWNTIME_CATEGORIES, key="add_reason_category")
            with c2:
                reason = st.text_input("Reason")
            with c3:
                oee_impact = st.selectbox("OEE impact", DEFAULT_OEE_IMPACTS, key="add_reason_oee")
            with c4:
                active = st.checkbox("Active", value=True)
            add_reason = st.form_submit_button("Add downtime reason")
        if add_reason:
            if not reason.strip():
                st.warning("Reason is required.")
            else:
                execute("INSERT OR IGNORE INTO downtime_reason_master(category,reason,oee_impact,active) VALUES(?,?,?,?)", [category, reason.strip(), oee_impact, 1 if active else 0])
                st.success("Downtime reason added.")
                st.rerun()

        if not reasons.empty:
            st.markdown("### Edit downtime reason")
            labels = reasons.apply(lambda r: f"{r['category']} | {r['reason']}", axis=1).tolist()
            picked_reason = st.selectbox("Select downtime reason to edit", labels, key="edit_reason_pick")
            row = reasons.iloc[labels.index(picked_reason)]
            with st.form("edit_dt_reason_form"):
                c1, c2, c3, c4 = st.columns(4)
                with c1:
                    edit_category = st.selectbox("Category", DEFAULT_DOWNTIME_CATEGORIES, index=DEFAULT_DOWNTIME_CATEGORIES.index(row["category"]) if row["category"] in DEFAULT_DOWNTIME_CATEGORIES else 0, key="edit_reason_category")
                with c2:
                    edit_reason = st.text_input("Reason", value=row["reason"])
                with c3:
                    edit_oee = st.selectbox("OEE impact", DEFAULT_OEE_IMPACTS, index=DEFAULT_OEE_IMPACTS.index(row["oee_impact"]) if row["oee_impact"] in DEFAULT_OEE_IMPACTS else 0, key="edit_reason_oee")
                with c4:
                    edit_active = st.checkbox("Active", value=bool(row["active"]), key="edit_reason_active")
                b1, b2 = st.columns(2)
                save_reason = b1.form_submit_button("Save downtime reason")
                delete_reason = b2.form_submit_button("Delete downtime reason")
            if save_reason:
                execute("UPDATE downtime_reason_master SET category=?, reason=?, oee_impact=?, active=? WHERE id=?", [edit_category, edit_reason.strip(), edit_oee, 1 if edit_active else 0, int(row["id"])] )
                st.success("Downtime reason updated.")
                st.rerun()
            if delete_reason:
                execute("DELETE FROM downtime_reason_assignment WHERE reason_id=?", [int(row["id"])])
                execute("DELETE FROM downtime_reason_master WHERE id=?", [int(row["id"])])
                st.success("Downtime reason deleted.")
                st.rerun()

    with tab6:
        st.subheader("Downtime Assignments")
        reasons_ref = read_df("SELECT id,category,reason,oee_impact FROM downtime_reason_master WHERE active=1 ORDER BY category,reason")
        machines_ref = read_df("SELECT id,name FROM machines WHERE active=1 ORDER BY display_order,name")
        if reasons_ref.empty or machines_ref.empty:
            st.info("You need active machines and downtime reasons first.")
        else:
            picked_reason_label = st.selectbox(
                "Downtime reason",
                [f"{r['category']} | {r['reason']} | {r['oee_impact']}" for _, r in reasons_ref.iterrows()],
                key="assign_reason_all_machines"
            )
            labels = [f"{r['category']} | {r['reason']} | {r['oee_impact']}" for _, r in reasons_ref.iterrows()]
            picked_reason_row = reasons_ref.iloc[labels.index(picked_reason_label)]
            reason_id = int(picked_reason_row["id"])
            summary = read_df("""
                SELECT m.name AS machine, e.equipment_name, a.active
                FROM downtime_reason_assignment a
                JOIN machines m ON a.machine_id=m.id
                JOIN downtime_equipment_master e ON a.downtime_equipment_id=e.id
                WHERE a.reason_id=?
                ORDER BY m.name, e.equipment_name
            """, [reason_id])
            if not summary.empty:
                st.dataframe(summary, width="stretch", hide_index=True)
            st.markdown("### Assignment matrix by machine")
            for _, machine in machines_ref.iterrows():
                machine_id = int(machine["id"])
                equipment_df = read_df("SELECT id,equipment_name FROM downtime_equipment_master WHERE machine_id=? AND active=1 ORDER BY equipment_name", [machine_id])
                with st.container(border=True):
                    st.markdown(f"**{machine['name']}**")
                    if equipment_df.empty:
                        st.caption("No downtime equipment configured for this machine.")
                    else:
                        active_assignments = set(read_df("SELECT downtime_equipment_id FROM downtime_reason_assignment WHERE machine_id=? AND reason_id=? AND active=1", [machine_id, reason_id])["downtime_equipment_id"].tolist())
                        matrix_df = equipment_df.copy()
                        matrix_df["Assign"] = matrix_df["id"].apply(lambda x: x in active_assignments)
                        edited = st.data_editor(
                            matrix_df[["equipment_name", "Assign"]],
                            width="stretch",
                            hide_index=True,
                            disabled=["equipment_name"],
                            key=f"assign_editor_{reason_id}_{machine_id}"
                        )
                        if st.button(f"Save {machine['name']}", key=f"save_assign_{reason_id}_{machine_id}"):
                            for _, eq_row in equipment_df.iterrows():
                                eq_id = int(eq_row["id"])
                                eq_name = eq_row["equipment_name"]
                                new_state = bool(edited.loc[edited["equipment_name"] == eq_name, "Assign"].iloc[0])
                                if new_state:
                                    execute("INSERT OR IGNORE INTO downtime_reason_assignment(reason_id,machine_id,downtime_equipment_id,active) VALUES(?,?,?,1)", [reason_id, machine_id, eq_id])
                                    execute("UPDATE downtime_reason_assignment SET active=1 WHERE reason_id=? AND machine_id=? AND downtime_equipment_id=?", [reason_id, machine_id, eq_id])
                                else:
                                    execute("UPDATE downtime_reason_assignment SET active=0 WHERE reason_id=? AND machine_id=? AND downtime_equipment_id=?", [reason_id, machine_id, eq_id])
                            st.success(f"Assignments updated for {machine['name']}.")
                            st.rerun()

    with tab7:
        st.subheader("Users")
        users = read_df("SELECT id, username, role, active, created_at FROM users ORDER BY username")
        st.dataframe(users, width="stretch", hide_index=True)


def login_screen():
    st.title("Production Data App")
    with st.form("login_form"):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        submitted = st.form_submit_button("Login")
    if submitted:
        user = authenticate(username.strip(), password)
        if user:
            st.session_state.user = user
            st.rerun()
        st.error("Invalid username or password")


def main():
    init_db()
    st.set_page_config(page_title="Production Data App", page_icon="🏭", layout="wide")
    init_page_state()
    if "user" not in st.session_state:
        st.session_state.user = None
    if st.session_state.user is None:
        login_screen()
        return
    user = st.session_state.user
    st.sidebar.title("🏭 Production App")
    st.sidebar.write(f"User: {user['username']} ({user['role']})")
    if st.sidebar.button("🔄 Refresh current page"):
        st.rerun()
    page = st.sidebar.radio("Navigate", ["Home", "Production Entry", "Downtime Board", "Analysis", "Settings"])
    if st.sidebar.button("Logout"):
        st.session_state.user = None
        st.rerun()
    if page == "Home":
        home_page()
    elif page == "Production Entry":
        production_page(user)
    elif page == "Downtime Board":
        downtime_page(user)
    elif page == "Analysis":
        analysis_page()
    elif page == "Settings":
        settings_page(user)


if __name__ == "__main__":
    main()
