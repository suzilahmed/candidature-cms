import streamlit as st
import pandas as pd
import sqlite3
import re
from io import BytesIO
from pathlib import Path
import yaml
from datetime import datetime

DB_PATH = Path(st.secrets.get("DB_PATH", "candidature.db"))
DEFAULT_XLSX = Path(st.secrets.get("DEFAULT_XLSX", "2024.11.08 - Candidature Matrix.xlsx"))
COUNTRIES_CSV = Path("countries.csv")
UPLOADS_DIR = Path("uploads")

CANONICAL_COLUMNS = [
    "Ref #",
    "Candidate Countries",
    "Respective Country's Election Body",
    "Proposal sent by",
    "Indicate Confirmation with date and TPN #",
    "Attachment Path",
    "Created By",
    "Created At",
]

ROLE_PERMS = {
    "admin": {"import": True, "add": True, "edit": True, "export": True},
    "editor": {"import": False, "add": True, "edit": True, "export": True},
    "viewer": {"import": False, "add": False, "edit": False, "export": True},
}

# --- Auth ---
def load_users():
    path = Path("users.yaml")
    if not path.exists():
        return {}
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return data.get("users", {})

def login_flow():
    st.sidebar.subheader("Login")
    users = load_users()
    if "auth" not in st.session_state:
        st.session_state.auth = {"user": None, "role": "viewer"}
    if st.session_state.auth["user"]:
        st.sidebar.success(f"Signed in as {st.session_state.auth['user']} ({st.session_state.auth['role']})")
        if st.sidebar.button("Sign out"):
            st.session_state.auth = {"user": None, "role": "viewer"}
            st.rerun()
        return st.session_state.auth
    u = st.sidebar.text_input("Username")
    p = st.sidebar.text_input("Password", type="password")
    if st.sidebar.button("Sign in"):
        if u in users and p == str(users[u].get("password")):
            st.session_state.auth = {"user": u, "role": users[u].get("role", "viewer")}
            st.rerun()
        else:
            st.sidebar.error("Invalid credentials")
    return st.session_state.auth

# --- DB helpers ---
def slug(name: str) -> str:
    s = re.sub(r"[^0-9a-zA-Z]+", "_", name.strip()).strip("_").lower()
    s = re.sub(r"_+", "_", s)
    return s or "sheet"

def connect():
    return sqlite3.connect(DB_PATH)

def ensure_table(conn, table: str, columns: list[str]):
    cols_sql = ", ".join([f'"{c}" TEXT' for c in columns])
    conn.execute(f'CREATE TABLE IF NOT EXISTS "{table}" (id INTEGER PRIMARY KEY AUTOINCREMENT, {cols_sql})')
    conn.commit()

def ensure_records_table(conn):
    base_cols = ["category"] + CANONICAL_COLUMNS
    ensure_table(conn, "records", base_cols)
    cur = conn.execute('PRAGMA table_info("records")').fetchall()
    existing = {r[1] for r in cur}
    for c in base_cols:
        if c not in existing:
            conn.execute(f'ALTER TABLE "records" ADD COLUMN "{c}" TEXT')
    conn.commit()

def detect_header_row(df, max_scan=50):
    for i in range(min(max_scan, len(df))):
        row = df.iloc[i]
        if row.notna().sum() >= 3 and any(isinstance(x, str) for x in row.values):
            return i
    return 0

def normalize_columns(cols):
    return [re.sub(r"\s+", " ", str(c)).replace("/", " ").replace("&", "and").strip() for c in cols]

def get_template_categories() -> list[str]:
    if DEFAULT_XLSX.exists():
        try:
            xls = pd.ExcelFile(DEFAULT_XLSX)
            return list(xls.sheet_names)
        except Exception:
            return []
    return []

def import_sheet_into_table(conn, xlsx_path: Path, sheet_name: str):
    raw = pd.read_excel(xlsx_path, sheet_name=sheet_name, header=None)
    hdr = detect_header_row(raw)
    df = pd.read_excel(xlsx_path, sheet_name=sheet_name, header=hdr)
    df = df.dropna(axis=1, how="all").dropna(how="all")
    df.columns = normalize_columns(df.columns)
    colmap = {}
    basic_cols = CANONICAL_COLUMNS[:5]
    for canon in basic_cols:
        candidates = [c for c in df.columns if c.lower().startswith(canon.lower().split()[0])]
        colmap[canon] = candidates[0] if candidates else None
    recs = pd.DataFrame({})
    recs["category"] = sheet_name
    for canon in basic_cols:
        src = colmap.get(canon)
        recs[canon] = df[src] if src in df.columns else None
    recs["Attachment Path"] = ""
    recs["Created By"] = ""
    recs["Created At"] = ""
    ensure_records_table(conn)
    recs.to_sql("records", conn, if_exists="append", index=False)
    table = slug(sheet_name)
    ensure_table(conn, table, normalize_columns(df.columns))
    conn.execute(f'DELETE FROM "{table}"')
    df.to_sql(table, conn, if_exists="append", index=False)
    conn.commit()
    return len(df)

def read_table(conn, table):
    return pd.read_sql_query(f'SELECT * FROM "{table}" ORDER BY id', conn)

def read_records(conn):
    ensure_records_table(conn)
    return pd.read_sql_query('SELECT * FROM "records" ORDER BY id', conn)

def save_table(conn, table, df):
    cols = [c for c in df.columns if c != "id"]
    conn.execute(f'DELETE FROM "{table}"')
    df[cols].to_sql(table, conn, if_exists="append", index=False)
    conn.commit()

def insert_record(conn, record: dict):
    ensure_records_table(conn)
    cols = ["category"] + CANONICAL_COLUMNS
    values = [record.get(c, "") for c in cols]
    placeholders = ", ".join(["?"] * len(cols))
    conn.execute(f'INSERT INTO records ("' + '","'.join(cols) + f'") VALUES ({placeholders})', values)
    conn.commit()

def export_report_workbook(conn, include_all_template_sheets: bool = True) -> bytes:
    df = read_records(conn)
    template_sheets = get_template_categories() if include_all_template_sheets else []
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        if not df.empty:
            for cat, g in df.groupby("category"):
                out = g[CANONICAL_COLUMNS[:5]].copy()
                out.index = range(1, len(out) + 1)
                sheet_name = (cat or "Sheet")[:31]
                out.to_excel(writer, sheet_name=sheet_name, index_label="Ref # (auto)")
        if include_all_template_sheets and template_sheets:
            existing = set(writer.sheets.keys())
            for cat in template_sheets:
                nm = (cat or "Sheet")[:31]
                if nm not in existing:
                    pd.DataFrame(columns=CANONICAL_COLUMNS[:5]).to_excel(writer, sheet_name=nm, index=False)
    output.seek(0)
    return output.read()

# --- App UI ---
st.set_page_config(page_title="Candidature CMS", layout="wide")
st.title("Candidature CMS")

auth = login_flow()
perms = ROLE_PERMS.get(auth["role"], ROLE_PERMS["viewer"])

with st.sidebar:
    st.header("Setup & Import")
    st.write(f"DB: `{DB_PATH}`")
    st.write(f"Template: `{DEFAULT_XLSX.name}` {'✅' if DEFAULT_XLSX.exists() else '⚠️ not found'}")
    if st.button("Initialize DB"):
        with connect() as conn:
            ensure_records_table(conn)
        st.success("Tables ready.")
    st.subheader("Import")
    if perms["import"]:
        if st.button("Import template workbook into CMS"):
            with connect() as conn:
                try:
                    if not DEFAULT_XLSX.exists():
                        st.error("Template workbook not found.")
                    else:
                        xls = pd.ExcelFile(DEFAULT_XLSX)
                        ensure_records_table(conn)
                        conn.execute('DELETE FROM "records"')
                        for s in xls.sheet_names:
                            import_sheet_into_table(conn, DEFAULT_XLSX, s)
                        st.success("Imported all sheets.")
                except Exception as e:
                    st.error(f"Import failed: {e}")
        uploaded = st.file_uploader("Upload workbook to import", type=["xlsx"])
        if uploaded is not None and st.button("Import uploaded workbook"):
            tmp = Path(uploaded.name)
            tmp.write_bytes(uploaded.getbuffer())
            with connect() as conn:
                try:
                    xls = pd.ExcelFile(tmp)
                    ensure_records_table(conn)
                    conn.execute('DELETE FROM "records"')
                    for s in xls.sheet_names:
                        import_sheet_into_table(conn, tmp, s)
                    st.success("Imported uploaded workbook.")
                finally:
                    tmp.unlink(missing_ok=True)
    else:
        st.info("You don't have permission to import (admin only).")

tabs = st.tabs(["➕ Data Entry", "🔎 Browse & Filter", "📤 Reports", "🛠️ Advanced (per-sheet)"])

countries = []
if COUNTRIES_CSV.exists():
    try:
        df_c = pd.read_csv(COUNTRIES_CSV)
        countries = sorted(df_c["name"].dropna().tolist())
    except Exception:
        pass
if not countries:
    countries = ["Maldives", "Benin", "Burkina Faso", "Other"]

with tabs[0]:
    st.subheader("Add a new record")
    if not perms["add"]:
        st.info("You don't have permission to add records.")
    else:
        dyn_categories = get_template_categories() or ["General"]
        with st.form("new_record"):
            category = st.selectbox("Category (report sheet)", dyn_categories)
            col1, col2 = st.columns(2)
            with col1:
                refno = st.text_input("Ref #")
                election_body = st.text_input("Respective Country's Election Body")
                country = st.selectbox("Candidate Countries", countries + ["Other"])
                if country == "Other":
                    country = st.text_input("Enter country name")
            with col2:
                proposal = st.text_area("Proposal sent by", height=80)
                confirm = st.text_area("Indicate Confirmation with date and TPN #", height=80)
                attachment = st.file_uploader("Attachment (PDF/Doc/Image)", type=["pdf","doc","docx","png","jpg","jpeg"])
            submitted = st.form_submit_button("Add record")
            if submitted:
                attachment_path = ""
                if attachment is not None:
                    UPLOADS_DIR.mkdir(exist_ok=True, parents=True)
                    safe_name = f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{re.sub(r'[^0-9a-zA-Z_.-]+','_', attachment.name)}"
                    dest = UPLOADS_DIR / safe_name
                    dest.write_bytes(attachment.getbuffer())
                    attachment_path = str(dest)
                with connect() as conn:
                    insert_record(conn, {
                        "category": category,
                        "Ref #": refno,
                        "Candidate Countries": country,
                        "Respective Country's Election Body": election_body,
                        "Proposal sent by": proposal,
                        "Indicate Confirmation with date and TPN #": confirm,
                        "Attachment Path": attachment_path,
                        "Created By": auth["user"] or "",
                        "Created At": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                    })
                st.success("Record added.")
