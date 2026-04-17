import io
import sqlite3
from datetime import datetime, date, time, timedelta

import pandas as pd
import plotly.express as px
import streamlit as st

DB_NAME = "production.db"
DT_FORMAT = "%Y-%m-%d %H:%M"

DOWNTIME_CAUSES = [
    "Breakdown", "Maintenance", "No operator", "No loader", "No excavator",
    "No feed", "No tipper", "Conveyor issue", "Screen issue", "Jaw issue",
    "Cone issue", "Generator issue", "Electrical", "Hydraulic", "Tyre puncture",
    "Refuel", "Shift change", "Weather", "Blasting", "Standby", "Other"
]


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


def rebuild_downtime_table(cur):
    cur.execute("DROP TABLE IF EXISTS downtime")
    cur.execute(
        '''CREATE TABLE downtime (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            machine_id INTEGER NOT NULL,
            stop_datetime TEXT NOT NULL,
            start_datetime TEXT,
            cause TEXT NOT NULL,
            comments TEXT,
            is_open INTEGER NOT NULL DEFAULT 1,
            created_by TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(machine_id) REFERENCES machines(id)
        )'''
    )


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
            UNIQUE(production_date, shift, hour_label, machine_id),
            FOREIGN KEY(machine_id) REFERENCES machines(id),
            FOREIGN KEY(material_id) REFERENCES materials(id),
            FOREIGN KEY(equipment_id) REFERENCES feeding_equipment(id)
        )'''
    )

    existing_prod_cols = [r[1] for r in cur.execute("PRAGMA table_info(production)").fetchall()]
    if 'hour_index' not in existing_prod_cols:
        cur.execute("ALTER TABLE production ADD COLUMN hour_index INTEGER NOT NULL DEFAULT 0")
    if 'current_totalizer' not in existing_prod_cols:
        cur.execute("ALTER TABLE production ADD COLUMN current_totalizer INTEGER")
    if 'previous_totalizer' not in existing_prod_cols:
        cur.execute("ALTER TABLE production ADD COLUMN previous_totalizer INTEGER")

    cur.execute("CREATE TABLE IF NOT EXISTS downtime_schema_marker (id INTEGER PRIMARY KEY, version INTEGER NOT NULL)")
    marker = cur.execute("SELECT version FROM downtime_schema_marker WHERE id=1").fetchone()
    if marker is None or int(marker[0]) < 2:
        rebuild_downtime_table(cur)
        cur.execute("DELETE FROM downtime_schema_marker WHERE id=1")
        cur.execute("INSERT INTO downtime_schema_marker(id, version) VALUES(1, 2)")
    else:
        existing_dt_cols = [r[1] for r in cur.execute("PRAGMA table_info(downtime)").fetchall()]
        required_cols = {'machine_id', 'stop_datetime', 'start_datetime', 'cause', 'comments', 'is_open'}
        if not required_cols.issubset(set(existing_dt_cols)):
            rebuild_downtime_table(cur)
            cur.execute("DELETE FROM downtime_schema_marker WHERE id=1")
            cur.execute("INSERT INTO downtime_schema_marker(id, version) VALUES(1, 2)")

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

    conn.commit()
    conn.close()


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


def export_df(df, filename_prefix):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="data", index=False)
    st.download_button(label=f"Download {filename_prefix}.xlsx", data=output.getvalue(), file_name=f"{filename_prefix}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


def add_refresh_button():
    if st.button("🔄 Refresh page"):
        st.rerun()


def init_page_state():
    defaults = {
        "prod_date": date.today(),
        "prod_hour": production_hour_options()[0],
        "prod_machine": None,
        "prod_material": None,
        "prod_equipment": None,
        "prod_totalizer": 0,
        "prod_buckets": 0.0,
        "prod_comments": "",
        "pending_replace_payload": None,
        "pending_existing_text": None,
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


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
            reason_text = row["cause"]
            if pd.notna(row["comments"]) and str(row["comments"]).strip():
                reason_text = f"{row['cause']} - {row['comments']}"
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


def home_page():
    st.title("🏭 Production Dashboard")
    add_refresh_button()
    c1, c2 = st.columns(2)
    with c1:
        selected_date = st.date_input("Production day", value=date.today(), key="home_date")
    with c2:
        selected_hour = st.selectbox("Selected hour", production_hour_options(), key="home_hour")
    summary = build_home_summary(selected_date, selected_hour)
    st.subheader("Hourly summary")
    st.dataframe(summary, width="stretch", hide_index=True)
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
    if not summary.empty:
        export_df(summary, "home_summary")


def production_page(user):
    st.title("🟢 Production Entry")
    add_refresh_button()
    machines = read_df("SELECT * FROM machines WHERE active=1 ORDER BY display_order,name")
    materials = read_df("SELECT * FROM materials WHERE active=1 ORDER BY name")
    equipment = read_df("SELECT * FROM feeding_equipment WHERE active=1 ORDER BY name")
    if machines.empty:
        st.warning("Please load machines in Settings first.")
        return
    if st.session_state.prod_machine is None or st.session_state.prod_machine not in machines["name"].tolist():
        st.session_state.prod_machine = machines["name"].tolist()[0]
    if not materials.empty and (st.session_state.prod_material is None or st.session_state.prod_material not in materials["name"].tolist()):
        st.session_state.prod_material = materials["name"].tolist()[0]
    if not equipment.empty and (st.session_state.prod_equipment is None or st.session_state.prod_equipment not in equipment["name"].tolist()):
        st.session_state.prod_equipment = equipment["name"].tolist()[0]

    selected_machine = st.selectbox("Machine", machines["name"].tolist(), index=machines["name"].tolist().index(st.session_state.prod_machine))
    st.session_state.prod_machine = selected_machine
    machine = machines.loc[machines["name"] == selected_machine].iloc[0]
    is_plant = machine["machine_type"] == "plant"

    with st.form("production_form", clear_on_submit=False):
        c1, c2, c3 = st.columns(3)
        with c1:
            production_date = st.date_input("Production date", value=st.session_state.prod_date)
            hour_label = st.selectbox("Hour", production_hour_options(), index=production_hour_options().index(st.session_state.prod_hour))
            st.text_input("Machine type", value=machine["machine_type"], disabled=True)
        with c2:
            if is_plant:
                current_totalizer = st.number_input("Current scale totalizer", min_value=0, step=1, value=int(st.session_state.prod_totalizer))
                prev_totalizer_preview = get_previous_totalizer(int(machine["id"]), production_date.isoformat(), hour_to_index(hour_label))
                st.text_input("Previous totalizer used", value=str(prev_totalizer_preview), disabled=True)
                material_name = None
                equipment_name = None
                loads = 0.0
            else:
                material_name = st.selectbox("Material", materials["name"].tolist(), index=materials["name"].tolist().index(st.session_state.prod_material))
                equipment_name = st.selectbox("Feeding equipment", equipment["name"].tolist(), index=equipment["name"].tolist().index(st.session_state.prod_equipment))
                loads = st.number_input("No of buckets", min_value=0.0, step=1.0, value=float(st.session_state.prod_buckets))
                current_totalizer = 0
        with c3:
            comments = st.text_input("Comments", value=st.session_state.prod_comments)
            if is_plant:
                est = int(current_totalizer) - int(get_previous_totalizer(int(machine["id"]), production_date.isoformat(), hour_to_index(hour_label)))
                st.text_input("Estimated output tons", value=f"{est:,.0f}", disabled=True)
            else:
                mat = materials.loc[materials["name"] == material_name].iloc[0]
                eq = equipment.loc[equipment["name"] == equipment_name].iloc[0]
                preview = float(loads) * float(eq["bucket_volume"]) * float(mat["bulk_density"]) * deduction_multiplier_from_percent(float(machine["deduction_percent"]))
                st.text_input("Estimated output tons", value=f"{preview:,.2f}", disabled=True)
        submitted = st.form_submit_button("Save production")

    st.session_state.prod_date = production_date
    st.session_state.prod_hour = hour_label
    st.session_state.prod_totalizer = int(current_totalizer)
    st.session_state.prod_comments = comments
    if not is_plant:
        st.session_state.prod_material = material_name
        st.session_state.prod_equipment = equipment_name
        st.session_state.prod_buckets = float(loads)

    if submitted:
        shift = get_shift_from_hour_label(hour_label)
        hour_index = hour_to_index(hour_label)
        payload = {"production_date": production_date.isoformat(), "shift": shift, "hour_label": hour_label, "hour_index": hour_index, "machine_id": int(machine["id"]), "material_id": None, "equipment_id": None, "loads": 0.0, "ton_per_load": 0.0, "input_tons": 0.0, "output_tons": 0.0, "recirculation_factor": 1.0, "deduction_percent": 0.0, "deduction_multiplier": 1.0, "current_totalizer": None, "previous_totalizer": None, "comments": comments, "created_by": user["username"]}
        if is_plant:
            previous_totalizer = get_previous_totalizer(int(machine["id"]), production_date.isoformat(), hour_index)
            output_tons = int(current_totalizer) - int(previous_totalizer)
            if output_tons < 0:
                st.error("Current totalizer cannot be smaller than the previous totalizer or starting totalizer.")
                return
            payload.update({"input_tons": float(output_tons), "output_tons": float(output_tons), "current_totalizer": int(current_totalizer), "previous_totalizer": int(previous_totalizer)})
        else:
            material = materials.loc[materials["name"] == material_name].iloc[0]
            feed = equipment.loc[equipment["name"] == equipment_name].iloc[0]
            bulk_density = float(material["bulk_density"])
            bucket_volume = float(feed["bucket_volume"])
            deduction_percent = float(machine["deduction_percent"])
            deduction_multiplier = deduction_multiplier_from_percent(deduction_percent)
            input_tons = float(loads) * bucket_volume * bulk_density
            output_tons = round(input_tons * deduction_multiplier, 3)
            payload.update({"material_id": int(material["id"]), "equipment_id": int(feed["id"]), "loads": float(loads), "ton_per_load": round(bucket_volume * bulk_density, 3), "input_tons": input_tons, "output_tons": output_tons, "deduction_percent": deduction_percent, "deduction_multiplier": deduction_multiplier})
        existing = get_existing_production(int(machine["id"]), production_date.isoformat(), shift, hour_label)
        if existing and user["role"] == "operator":
            st.session_state.pending_replace_payload = payload
            st.session_state.pending_existing_text = f"Data already exists for {selected_machine} on {production_date.isoformat()} {hour_label}. Do you want to replace the data?"
            st.warning(st.session_state.pending_existing_text)
            yes_col, no_col = st.columns(2)
            if yes_col.button("Yes, replace"):
                save_or_replace_production(st.session_state.pending_replace_payload, replace=True)
                st.success("Production data replaced.")
                st.session_state.pending_replace_payload = None
                st.session_state.pending_existing_text = None
                st.rerun()
            if no_col.button("No, keep existing"):
                st.info("Existing data kept.")
                st.session_state.pending_replace_payload = None
                st.session_state.pending_existing_text = None
            return
        status = save_or_replace_production(payload, replace=(user["role"] == "admin" and existing is not None))
        if status == "saved":
            st.success(f"Saved for {selected_machine}. Tons = {payload['output_tons']:,.2f}")
            next_idx = min(hour_index + 1, 23)
            st.session_state.prod_hour = production_hour_options()[next_idx]
            st.rerun()


def downtime_page(user):
    st.title("🔴 Downtime Board")
    add_refresh_button()
    machines = read_df("SELECT * FROM machines WHERE active=1 ORDER BY display_order,name")
    open_dt = read_df('''SELECT d.*, m.name AS machine_name FROM downtime d JOIN machines m ON d.machine_id=m.id WHERE d.is_open=1 ORDER BY m.display_order, m.name''')
    if machines.empty:
        st.warning("Please add machines in Settings first.")
        return
    st.caption("One row per machine. Stop opens downtime. Start closes downtime. Datetime format: YYYY-MM-DD HH:MM")

    for _, machine in machines.iterrows():
        machine_id = int(machine["id"])
        machine_name = machine["name"]
        open_row = open_dt.loc[open_dt["machine_id"] == machine_id]
        has_open = not open_row.empty
        with st.container(border=True):
            cols = st.columns([1.2, 0.8, 1.5, 1.4, 1.6, 1.6, 1.5, 1, 1])
            cols[0].markdown(f"**{machine_name}**")
            cols[1].write("Off" if has_open else "On")
            stop_text = cols[2].text_input("Stop", value=fmt_dt(datetime.now().replace(second=0, microsecond=0)), key=f"stoptxt_{machine_id}")
            cause = cols[3].selectbox("Reason", DOWNTIME_CAUSES, key=f"cause_{machine_id}")
            dt_comment = cols[4].text_input("Detail", key=f"detail_{machine_id}")
            if has_open:
                open_item = open_row.iloc[0]
                cols[5].write(f"Open since {str(open_item['stop_datetime']).replace('T', ' ')}")
                start_default = datetime.fromisoformat(str(open_item["stop_datetime"])) + timedelta(minutes=1)
            else:
                cols[5].write("No open downtime")
                start_default = datetime.now().replace(second=0, microsecond=0)
            start_text = cols[6].text_input("Start", value=fmt_dt(start_default), key=f"starttxt_{machine_id}")

            if cols[7].button("Stop", key=f"stopbtn_{machine_id}"):
                if has_open:
                    st.warning(f"{machine_name} already has an open downtime. Close it with Start first.")
                else:
                    try:
                        stop_dt = parse_dt_input(stop_text)
                    except ValueError as e:
                        st.error(f"{machine_name}: {e}")
                    else:
                        execute("INSERT INTO downtime(machine_id,stop_datetime,cause,comments,is_open,created_by) VALUES(?,?,?,?,1,?)", [machine_id, stop_dt.isoformat(timespec='minutes'), cause, dt_comment, user["username"]])
                        st.success(f"Downtime started for {machine_name}.")
                        st.rerun()

            if cols[8].button("Start", key=f"resume_{machine_id}"):
                if not has_open:
                    st.warning(f"{machine_name} has no open downtime to close.")
                else:
                    try:
                        start_dt = parse_dt_input(start_text)
                        open_stop_dt = datetime.fromisoformat(str(open_item["stop_datetime"]))
                    except ValueError as e:
                        st.error(f"{machine_name}: {e}")
                    else:
                        if start_dt <= open_stop_dt:
                            st.error(f"{machine_name}: Start must be after Stop.")
                        else:
                            execute("UPDATE downtime SET start_datetime=?, is_open=0 WHERE id=?", [start_dt.isoformat(timespec='minutes'), int(open_item["id"])])
                            st.success(f"Downtime closed for {machine_name}.")
                            st.rerun()

    history = read_df('''SELECT d.id,m.name AS machine,d.stop_datetime,d.start_datetime,d.cause,d.comments,
                  CASE WHEN d.start_datetime IS NULL THEN NULL ELSE ROUND((julianday(d.start_datetime)-julianday(d.stop_datetime))*24*60,1) END AS duration_minutes,
                  d.is_open,d.created_by
           FROM downtime d JOIN machines m ON d.machine_id=m.id ORDER BY d.stop_datetime DESC''')
    if not history.empty:
        st.subheader("Downtime history")
        st.dataframe(history, width="stretch")


def settings_page(user):
    if user["role"] != "admin":
        st.warning("Only admin can access Settings.")
        return

    st.title("⚙️ Settings")
    add_refresh_button()
    tab1, tab2, tab3, tab4 = st.tabs(["Machines", "Materials", "Feeding Equipment", "Users"])

    with tab1:
        st.subheader("Machines")
        machines = read_df("SELECT * FROM machines ORDER BY display_order,name")
        st.dataframe(machines, width="stretch", hide_index=True)
        if not machines.empty:
            selected = st.selectbox("Select machine to edit", machines["name"].tolist(), key="edit_machine_pick")
            row = machines[machines["name"] == selected].iloc[0]
            with st.form("edit_machine_form"):
                c1, c2, c3, c4 = st.columns(4)
                with c1:
                    name = st.text_input("Machine name", value=row["name"])
                    machine_type = st.selectbox("Machine type", ["plant", "mobile"], index=0 if row["machine_type"] == "plant" else 1)
                with c2:
                    area = st.text_input("Area", value=row["area"])
                    display_order = st.number_input("Display order", min_value=1, step=1, value=int(row["display_order"]))
                with c3:
                    apply_recirculation = st.checkbox("Apply recirculation", value=bool(row["apply_recirculation"]))
                    deduction_percent = st.number_input("Deduction %", min_value=0.0, max_value=100.0, step=0.1, value=float(row["deduction_percent"]))
                with c4:
                    totalizer_start = st.number_input("Starting totalizer", min_value=0, step=1, value=int(row["current_totalizer_start"]))
                    active = st.checkbox("Active", value=bool(row["active"]))
                save = st.form_submit_button("Save machine changes")
            if save:
                execute("UPDATE machines SET name=?, machine_type=?, area=?, display_order=?, active=?, apply_recirculation=?, deduction_percent=?, current_totalizer_start=? WHERE id=?", [name.strip(), machine_type, area.strip(), int(display_order), 1 if active else 0, 1 if apply_recirculation else 0, float(deduction_percent), int(totalizer_start), int(row["id"])])
                execute("INSERT INTO machine_deduction_history(machine_id,deduction_percent,deduction_multiplier,effective_from,changed_by) VALUES(?,?,?,?,?)", [int(row["id"]), float(deduction_percent), deduction_multiplier_from_percent(deduction_percent), date.today().isoformat(), user["username"]])
                st.success("Machine updated.")
                st.rerun()

    with tab2:
        st.subheader("Materials")
        mats = read_df("SELECT * FROM materials ORDER BY name")
        st.dataframe(mats, width="stretch", hide_index=True)
        if not mats.empty:
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
                    active = st.checkbox("Active", value=bool(row["active"]))
                save = st.form_submit_button("Save material changes")
            if save:
                execute("UPDATE materials SET name=?, bulk_density=?, recirculation_factor=?, active=? WHERE id=?", [name.strip(), float(bulk_density), float(recirculation_factor), 1 if active else 0, int(row["id"])])
                st.success("Material updated.")
                st.rerun()

    with tab3:
        st.subheader("Feeding Equipment")
        equip = read_df("SELECT * FROM feeding_equipment ORDER BY name")
        st.dataframe(equip, width="stretch", hide_index=True)
        if not equip.empty:
            selected = st.selectbox("Select equipment to edit", equip["name"].tolist(), key="edit_eq_pick")
            row = equip[equip["name"] == selected].iloc[0]
            with st.form("edit_eq_form"):
                c1, c2, c3 = st.columns(3)
                with c1:
                    name = st.text_input("Equipment name", value=row["name"])
                with c2:
                    bucket_volume = st.number_input("Bucket volume", min_value=0.0, step=0.1, value=float(row["bucket_volume"]))
                with c3:
                    active = st.checkbox("Active", value=bool(row["active"]))
                save = st.form_submit_button("Save equipment changes")
            if save:
                execute("UPDATE feeding_equipment SET name=?, bucket_volume=?, active=? WHERE id=?", [name.strip(), float(bucket_volume), 1 if active else 0, int(row["id"])])
                st.success("Equipment updated.")
                st.rerun()

    with tab4:
        st.subheader("Users")
        users = read_df("SELECT id, username, role, active, created_at FROM users ORDER BY username")
        st.dataframe(users, width="stretch", hide_index=True)
        all_users = read_df("SELECT * FROM users ORDER BY username")
        if not all_users.empty:
            selected = st.selectbox("Select user to edit", all_users["username"].tolist(), key="edit_user_pick")
            row = all_users[all_users["username"] == selected].iloc[0]
            with st.form("edit_user_form"):
                c1, c2, c3, c4 = st.columns(4)
                with c1:
                    username = st.text_input("Username", value=row["username"])
                with c2:
                    password = st.text_input("Password", value=row["password"], type="password")
                with c3:
                    role = st.selectbox("Role", ["admin", "operator"], index=0 if row["role"] == "admin" else 1)
                with c4:
                    active = st.checkbox("Active", value=bool(row["active"]))
                save = st.form_submit_button("Save user changes")
            if save:
                execute("UPDATE users SET username=?, password=?, role=?, active=? WHERE id=?", [username.strip(), password, role, 1 if active else 0, int(row["id"])] )
                st.success("User updated.")
                st.rerun()

            with st.form("add_user_form"):
                st.markdown("Add new user")
                c1, c2, c3 = st.columns(3)
                with c1:
                    new_user = st.text_input("New username")
                with c2:
                    new_pass = st.text_input("New password", type="password")
                with c3:
                    new_role = st.selectbox("New role", ["admin", "operator"])
                add_user = st.form_submit_button("Add user")
            if add_user:
                if not new_user.strip() or not new_pass:
                    st.warning("Username and password are required.")
                else:
                    existing = read_df("SELECT id FROM users WHERE LOWER(username)=LOWER(?)", [new_user.strip()])
                    if not existing.empty:
                        st.warning("User already exists.")
                    else:
                        execute("INSERT INTO users(username,password,role,active) VALUES(?,?,?,1)", [new_user.strip(), new_pass, new_role])
                        st.success("User added.")
                        st.rerun()


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
    page = st.sidebar.radio("Navigate", ["Home", "Production Entry", "Downtime Board", "Settings"])
    if st.sidebar.button("Logout"):
        st.session_state.user = None
        st.rerun()
    if page == "Home":
        home_page()
    elif page == "Production Entry":
        production_page(user)
    elif page == "Downtime Board":
        downtime_page(user)
    elif page == "Settings":
        settings_page(user)


if __name__ == "__main__":
    main()
