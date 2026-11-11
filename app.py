"""Lakehouse//RT Benchmark App — run ClickBench & TPC-H directly from the app.

Executes queries against SQL warehouses via the Statement Execution API
using the visiting user's OBO token. No external jobs or notebooks needed.
"""

import os
import json
import re
import time
import uuid
import requests as _requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import benchmark_queries

try:
    from databricks.sdk import WorkspaceClient
    SDK_AVAILABLE = True
except ImportError:
    SDK_AVAILABLE = False

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
RESULTS_TABLE = os.getenv("RESULTS_TABLE", "main.benchmark_results.query_latencies")
_raw_host = os.getenv("DATABRICKS_HOST", "").rstrip("/")
HOST = _raw_host if _raw_host.startswith("https://") else f"https://{_raw_host}"
NUM_REPS = 3

st.set_page_config(page_title="Lakehouse//RT Benchmark", page_icon="\u26a1", layout="wide")

# ---------------------------------------------------------------------------
# Theme
# ---------------------------------------------------------------------------
_CUSTOM_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&display=swap');
html, body, [class*="css"] { font-family: 'DM Sans', sans-serif !important; }

/* ── Page background ────────────────────────────────────────────────── */
.stApp { background: #0F1318; }
.block-container { max-width: 1200px; }

/* ── Header banner ──────────────────────────────────────────────────── */
.lhrt-banner {
  background: linear-gradient(135deg, #6B1510 0%, #3B0A08 50%, #1A0504 100%);
  border: 1px solid rgba(255,54,33,0.25);
  border-radius: 16px;
  padding: 1.6rem 2.2rem;
  margin-bottom: 1.8rem;
  position: relative;
  overflow: hidden;
}
.lhrt-banner::before {
  content: '';
  position: absolute; top: -40%; right: -10%; width: 300px; height: 300px;
  background: radial-gradient(circle, rgba(255,54,33,0.15) 0%, transparent 70%);
  pointer-events: none;
}
.lhrt-banner .lhrt-title {
  color: #FFFFFF; font-size: 1.8rem; font-weight: 700;
  letter-spacing: -0.03em; margin: 0; line-height: 1.2;
}
.lhrt-banner .lhrt-subtitle {
  color: rgba(255,255,255,0.65); font-size: 0.95rem;
  font-weight: 400; margin: 0.3rem 0 0 0;
}
.lhrt-banner .rt-badge {
  display: inline-flex; align-items: center; gap: 4px;
  background: #FF3621; color: #fff; font-weight: 700; font-size: 0.85rem;
  padding: 3px 10px 3px 7px; border-radius: 8px;
  vertical-align: middle; margin-left: 6px;
}
.lhrt-banner .rt-badge svg { width: 14px; height: 14px; }

/* ── Step chip ──────────────────────────────────────────────────────── */
.step-chip {
  display: inline-block;
  background: rgba(255,54,33,0.12);
  color: #FF6A33; border-radius: 20px;
  padding: 0.3rem 1rem; font-size: 0.78rem;
  font-weight: 600; letter-spacing: 0.04em;
  margin-bottom: 0.6rem;
}

/* ── KPI metric cards ───────────────────────────────────────────────── */
[data-testid="stMetric"] {
  background: #161B22; border: 1px solid #21262D;
  border-radius: 12px; padding: 1.1rem 1.3rem;
}
[data-testid="stMetricLabel"] {
  color: #8B949E !important; font-size: 0.78rem !important;
  text-transform: uppercase; letter-spacing: 0.06em;
}
[data-testid="stMetricValue"] {
  color: #F0F3F6 !important; font-weight: 700 !important;
  font-size: 1.5rem !important;
}

/* ── Buttons ────────────────────────────────────────────────────────── */
button[kind="primary"] {
  background: #FF3621 !important;
  border: none !important; border-radius: 10px !important;
  font-weight: 600 !important; font-size: 0.9rem !important;
  padding: 0.6rem 1.5rem !important;
  transition: all 0.15s;
}
button[kind="primary"]:hover { background: #E02E1B !important; }
button[kind="secondary"], button[kind="tertiary"] {
  border: 1px solid #30363D !important;
  border-radius: 10px !important; color: #C9D1D9 !important;
}

/* ── Alerts ─────────────────────────────────────────────────────────── */
[data-testid="stAlert"] { border-radius: 12px !important; }

/* ── DataFrames ─────────────────────────────────────────────────────── */
[data-testid="stDataFrame"] {
  border: 1px solid #21262D; border-radius: 12px; overflow: hidden;
}

/* ── Sidebar ────────────────────────────────────────────────────────── */
section[data-testid="stSidebar"] {
  background: #0F1318; border-right: 1px solid #21262D;
}

/* ── Progress bar ───────────────────────────────────────────────────── */
[data-testid="stProgress"] > div > div {
  background: #FF3621 !important; border-radius: 6px;
}

/* ── Inputs ─────────────────────────────────────────────────────────── */
[data-testid="stSelectbox"] > div > div,
[data-testid="stTextInput"] > div > div > input {
  border-radius: 10px !important; border-color: #30363D !important;
  background: #161B22 !important; color: #C9D1D9 !important;
}

/* ── Checkbox labels ────────────────────────────────────────────────── */
[data-testid="stCheckbox"] label span {
  font-size: 0.95rem !important; color: #C9D1D9 !important;
}

/* ── Dividers ───────────────────────────────────────────────────────── */
hr { border-color: #21262D !important; }

/* ── Tabs ───────────────────────────────────────────────────────────── */
[data-testid="stTabs"] button {
  color: #8B949E !important; font-weight: 500 !important;
  border-bottom-color: transparent !important;
}
[data-testid="stTabs"] button[aria-selected="true"] {
  color: #F0F3F6 !important;
  border-bottom-color: #FF3621 !important;
}

/* ── Plotly chart containers ────────────────────────────────────────── */
[data-testid="stPlotlyChart"] {
  background: #161B22; border: 1px solid #21262D;
  border-radius: 12px; padding: 0.5rem; overflow: hidden;
}
</style>
"""
st.markdown(_CUSTOM_CSS, unsafe_allow_html=True)

# ⚡ bolt SVG for the RT badge
_BOLT_SVG = '<svg viewBox="0 0 16 16" fill="currentColor"><path d="M9.5 1L4 9h4l-1 6L13 7H9l.5-6z"/></svg>'


def _render_banner():
    """Render the branded Lakehouse//RT banner."""
    st.markdown(
        f'<div class="lhrt-banner">'
        f'<p class="lhrt-title">'
        f'Lakehouse <span class="rt-badge">{_BOLT_SVG} RT</span>'
        f' Benchmark</p>'
        f'<p class="lhrt-subtitle">Measure SQL query latency across warehouse types and sizes</p>'
        f'</div>',
        unsafe_allow_html=True,
    )


def _step_chip(label: str):
    st.markdown(f'<span class="step-chip">{label}</span>', unsafe_allow_html=True)


# Plotly theme defaults
_PLOTLY_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="#161B22",
    font=dict(family="DM Sans, sans-serif", color="#C9D1D9", size=13),
    margin=dict(l=50, r=20, t=40, b=50),
    xaxis=dict(gridcolor="#21262D", zerolinecolor="#21262D"),
    yaxis=dict(gridcolor="#21262D", zerolinecolor="#21262D"),
    colorway=["#FF3621", "#FF6A33", "#FFA066", "#58A6FF", "#3FB950",
              "#D2A8FF", "#79C0FF", "#F778BA"],
    legend=dict(bgcolor="rgba(0,0,0,0)"),
)

# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------
def _get_user_token() -> str | None:
    try:
        headers = dict(st.context.headers)
        for name in ["X-Forwarded-Access-Token", "Authorization"]:
            val = headers.get(name, "")
            if val.startswith("Bearer "):
                return val[len("Bearer "):]
            elif val and name == "X-Forwarded-Access-Token":
                return val
    except Exception:
        pass
    return None


def _get_user_client() -> "WorkspaceClient | None":
    if not SDK_AVAILABLE:
        return None
    token = _get_user_token()
    if token:
        return WorkspaceClient(host=HOST, token=token, auth_type="pat")
    return None


w = _get_user_client()

# ---------------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------------
for key, default in {
    "step": 1, "selected_workloads": [], "warehouse_id": None,
    "warehouse_name": None, "catalog": "main", "results": None,
    "test_id": None, "run_start": None, "wh_info": None,
    "validation": None, "execution_mode": "sequential",
    "write_warehouse_id": None, "write_warehouse_name": None,
}.items():
    if key not in st.session_state:
        st.session_state[key] = default

if not SDK_AVAILABLE:
    st.error("databricks-sdk is not installed.")
elif w is None:
    st.warning("Could not connect. Please refresh the page.")

# ---------------------------------------------------------------------------
# SQL Statement Execution API
# ---------------------------------------------------------------------------
API_BASE = f"{HOST}/api/2.0/sql/statements"


def _api_headers() -> dict:
    token = _get_user_token()
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def execute_sql(warehouse_id: str, statement: str, catalog: str = "main",
                schema: str = "default", *, headers: dict | None = None,
                disable_cache: bool = False) -> dict:
    """Run SQL via Statement Execution API. Returns timing & metadata.
    Pass pre-built *headers* when calling from a background thread.
    Set disable_cache=True for benchmark queries to skip QRC."""
    if headers is None:
        headers = _api_headers()
    start = time.time()
    payload = {
        "warehouse_id": warehouse_id,
        "statement": statement,
        "wait_timeout": "50s",
        "disposition": "EXTERNAL_LINKS",
        "catalog": catalog,
        "schema": schema,
    }
    if disable_cache:
        payload["session_parameters"] = {"use_cached_result": "false"}
    resp = _requests.post(API_BASE, headers=headers, json=payload, timeout=300)
    data = resp.json()
    if resp.status_code >= 400:
        latency = time.time() - start
        msg = data.get("message", "") or data.get("error", "") or resp.text[:200]
        return {"latency": latency, "row_count": 0, "status": "error",
                "statement_id": "", "error": f"HTTP {resp.status_code}: {msg}"}
    stmt_id = data.get("statement_id", "")
    state = data.get("status", {}).get("state", "")

    while state in ("PENDING", "RUNNING"):
        time.sleep(0.5)
        poll = _requests.get(f"{API_BASE}/{stmt_id}", headers=headers, timeout=30)
        poll.raise_for_status()
        data = poll.json()
        state = data.get("status", {}).get("state", "")

    latency = time.time() - start
    row_count = data.get("manifest", {}).get("total_row_count", 0)
    error_msg = data.get("status", {}).get("error", {}).get("message", "")

    try:
        _requests.post(f"{API_BASE}/{stmt_id}/cancel", headers=headers, timeout=5)
    except Exception:
        pass

    return {
        "latency": latency,
        "row_count": row_count or 0,
        "status": "ok" if state == "SUCCEEDED" else "error",
        "statement_id": stmt_id,
        "error": error_msg,
    }


def execute_sql_rows(warehouse_id: str, statement: str, catalog: str = "main",
                     schema: str = "default", *, headers: dict | None = None) -> tuple[list[dict], str]:
    """Execute SQL and return (rows_as_dicts, error_string).
    Uses EXTERNAL_LINKS + JSON_ARRAY, then fetches the result data.
    Pass pre-built *headers* when calling from a background thread."""
    if headers is None:
        headers = _api_headers()
    resp = _requests.post(API_BASE, headers=headers, json={
        "warehouse_id": warehouse_id,
        "statement": statement,
        "wait_timeout": "50s",
        "disposition": "EXTERNAL_LINKS",
        "format": "JSON_ARRAY",
        "catalog": catalog,
        "schema": schema,
    }, timeout=120)
    data = resp.json()
    if resp.status_code >= 400:
        msg = data.get("message", "") or data.get("error", "") or f"HTTP {resp.status_code}"
        return [], f"HTTP {resp.status_code}: {msg}"
    stmt_id = data.get("statement_id", "")
    state = data.get("status", {}).get("state", "")
    while state in ("PENDING", "RUNNING"):
        time.sleep(0.5)
        poll = _requests.get(f"{API_BASE}/{stmt_id}", headers=headers, timeout=30)
        data = poll.json()
        state = data.get("status", {}).get("state", "")
    if state != "SUCCEEDED":
        err = data.get("status", {}).get("error", {}).get("message", f"state={state}")
        return [], err
    columns = [c["name"] for c in data.get("manifest", {}).get("schema", {}).get("columns", [])]
    total_rows = data.get("manifest", {}).get("total_row_count", 0)
    if total_rows == 0:
        return [], ""

    # Fetch result data from external links
    ext_links = data.get("result", {}).get("external_links", [])
    all_rows = []
    for link_info in ext_links:
        link_url = link_info.get("external_link", "")
        if not link_url:
            continue
        try:
            dl_resp = _requests.get(link_url, timeout=60)
            if dl_resp.status_code == 200:
                chunk_rows = dl_resp.json()
                if isinstance(chunk_rows, list):
                    all_rows.extend(chunk_rows)
        except Exception as e:
            return [], f"Failed to fetch result chunk: {e}"

    # Also try inline data_array as fallback
    if not all_rows:
        all_rows = data.get("result", {}).get("data_array", [])

    if not all_rows:
        return [], f"Query OK ({total_rows} rows) but could not retrieve data"

    return [dict(zip(columns, row)) for row in all_rows], ""




# ---------------------------------------------------------------------------
# Query History API — fetch server-side metrics
# ---------------------------------------------------------------------------
def _fetch_query_history_metrics(warehouse_id: str, statement_ids: list) -> dict:
    """Fetch server-side metrics from the Query History API.
    Returns {statement_id: metrics_dict} for each found statement."""
    headers = _api_headers()
    metrics_map = {}
    # The API returns most recent first; fetch enough to cover our run
    page_token = None
    remaining = set(statement_ids)
    for _ in range(20):  # max pages to check
        if not remaining:
            break
        params = {
            "include_metrics": "true",
            "max_results": 100,
            "filter_by.warehouse_ids": warehouse_id,
        }
        if page_token:
            params["page_token"] = page_token
        resp = _requests.get(f"{HOST}/api/2.0/sql/history/queries",
                             headers=headers, params=params, timeout=30)
        if resp.status_code != 200:
            break
        data = resp.json()
        for q in data.get("res", []):
            qid = q.get("query_id", "")
            if qid in remaining:
                metrics_map[qid] = q.get("metrics", {})
                metrics_map[qid]["duration_ms"] = q.get("duration", 0)
                metrics_map[qid]["query_start_time_ms"] = q.get("query_start_time_ms", 0)
                metrics_map[qid]["query_end_time_ms"] = q.get("query_end_time_ms", 0)
                remaining.discard(qid)
        page_token = data.get("next_page_token")
        if not page_token or not remaining:
            break
    return metrics_map


# ---------------------------------------------------------------------------
# Benchmark query execution via Python SQL Connector (low-latency path)
# ---------------------------------------------------------------------------
def _get_benchmark_connection(warehouse_id: str, catalog: str, schema: str,
                              disable_cache: bool = True):
    """Open a databricks-sql-connector connection for benchmark queries.
    Uses use_kernel=True (fast path) with fallback to use_sea=True.
    session_configuration disables QRC when requested."""
    import importlib, sys
    # Fix namespace shadowing (same as Jenny's harness)
    try:
        from databricks import sql as dbsql
    except ImportError:
        import databricks
        for p in sys.path:
            cand = os.path.join(p, "databricks")
            if os.path.isdir(os.path.join(cand, "sql")) and cand not in getattr(databricks, "__path__", []):
                databricks.__path__.append(cand)
        importlib.invalidate_caches()
        from databricks import sql as dbsql

    host_bare = HOST.replace("https://", "").replace("http://", "").rstrip("/")
    token = _get_user_token()
    session_cfg = {"use_cached_result": "false"} if disable_cache else None

    connect_kwargs = dict(
        server_hostname=host_bare,
        http_path=f"/sql/1.0/warehouses/{warehouse_id}",
        access_token=token,
        catalog=catalog,
        schema=schema,
        session_configuration=session_cfg,
    )

    # Try kernel first (lowest latency), then SEA, then plain
    for backend_kw in [{"use_kernel": True}, {"use_sea": True}, {}]:
        try:
            conn = dbsql.connect(**{**connect_kwargs, **backend_kw})
            return conn
        except (TypeError, ImportError):
            continue
        except Exception:
            if not backend_kw:  # plain also failed
                raise
            continue
    raise RuntimeError("Could not establish benchmark connection")


def execute_benchmark_query(conn, statement: str) -> dict:
    """Execute a single benchmark query on an existing connection.
    Returns {latency, row_count, status, statement_id, error}."""
    cur = conn.cursor()
    try:
        t0 = time.perf_counter()
        cur.execute(statement)
        cur.fetchall()
        latency = time.perf_counter() - t0
        stmt_id = getattr(cur, "query_id", None) or ""
        row_count = cur.rowcount if cur.rowcount and cur.rowcount >= 0 else 0
        return {"latency": latency, "row_count": row_count, "status": "ok",
                "statement_id": stmt_id, "error": ""}
    except Exception as exc:
        latency = time.perf_counter() - t0
        stmt_id = getattr(cur, "query_id", None) or ""
        return {"latency": latency, "row_count": 0, "status": "error",
                "statement_id": stmt_id, "error": str(exc)[:500]}
    finally:
        cur.close()


# ---------------------------------------------------------------------------
# Workload helpers
# ---------------------------------------------------------------------------
WORKLOAD_META = {
    "clickbench": {"name": "ClickBench", "schema": "benchmark_clickbench", "queries": 43,
                   "tables": ["hits"], "source_schema": "clickbench"},
    "tpch_sf1": {"name": "TPC-H SF1", "schema": "benchmark_tpch_sf1", "queries": 22,
                 "tables": ["region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem"],
                 "source_schema": "tpch_sf1"},
    "tpch_sf10": {"name": "TPC-H SF10", "schema": "benchmark_tpch_sf10", "queries": 22,
                  "tables": ["region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem"],
                  "source_schema": "tpch_sf10"},
    "tpch_sf100": {"name": "TPC-H SF100", "schema": "benchmark_tpch_sf100", "queries": 22,
                   "tables": ["region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem"],
                   "source_schema": "tpch_sf100"},
    "tpch_sf1000": {"name": "TPC-H SF1000", "schema": "benchmark_tpch_sf1000", "queries": 22,
                    "tables": ["region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem"],
                    "source_schema": "tpch_sf1000"},
}

# Canonical run order (ClickBench first, then TPC-H smallest to largest)
WORKLOAD_ORDER = ["clickbench", "tpch_sf1", "tpch_sf10", "tpch_sf100", "tpch_sf1000"]

# Warehouse size recommendation per workload
WORKLOAD_WH_SIZE = {
    "tpch_sf1": "Small", "tpch_sf10": "Small",
    "tpch_sf100": "Medium", "tpch_sf1000": "Large",
    "clickbench": "Medium",
}

def _classify_warehouse(wh_dict: dict) -> str:
    """Classify a warehouse as LAKEHOUSE_RT, SERVERLESS, PRO, or CLASSIC.

    LH//RT warehouses have warehouse_type='REYDEN' — that is the only
    reliable indicator across workspaces.
    """
    raw_type = str(wh_dict.get("warehouse_type") or "").strip().upper()
    serverless = wh_dict.get("enable_serverless_compute")

    if "REYDEN" in raw_type or raw_type in {"REALTIME", "REAL_TIME", "LAKEHOUSE_RT"}:
        return "LAKEHOUSE_RT"
    if serverless is True:
        return "SERVERLESS"
    if raw_type in {"PRO", "WAREHOUSE_TYPE_PRO"}:
        return "PRO"
    if raw_type in {"CLASSIC", "WAREHOUSE_TYPE_CLASSIC"}:
        return "CLASSIC"
    return "UNKNOWN"


PRODUCT_LABELS = {
    "LAKEHOUSE_RT": "Lakehouse//RT",
    "SERVERLESS": "Serverless",
    "PRO": "Pro",
    "CLASSIC": "Classic",
}


def list_warehouses():
    """List warehouses via REST API (to get raw warehouse_type). Returns list of dicts."""
    headers = _api_headers()
    try:
        resp = _requests.get(f"{HOST}/api/2.0/sql/warehouses", headers=headers, timeout=30)
        resp.raise_for_status()
        wh_list = resp.json().get("warehouses", [])
    except Exception:
        return []

    warehouses = []
    for d in wh_list:
        wh_id = d.get("id", "")
        name = d.get("name", "")
        size = str(d.get("cluster_size", "") or "")
        state = str(d.get("state", "") or "")
        wh_class = _classify_warehouse(d)
        type_label = PRODUCT_LABELS.get(wh_class, wh_class)
        icon = "\u2705" if "RUNNING" in state.upper() else "\u23f8\ufe0f"
        label = f"{icon} {name} ({type_label}, {size})"
        warehouses.append({
            "label": label,
            "id": wh_id,
            "name": name,
            "size": size,
            "state": state,
            "product": wh_class,
            "can_write": wh_class != "LAKEHOUSE_RT",
        })
    # Sort: running first, then alphabetically by name
    warehouses.sort(key=lambda w: (0 if "RUNNING" in w["state"].upper() else 1, w["name"].lower()))
    return warehouses


def _get_warehouse_info(warehouse_id: str) -> dict:
    headers = _api_headers()
    try:
        resp = _requests.get(f"{HOST}/api/2.0/sql/warehouses/{warehouse_id}",
                             headers=headers, timeout=15)
        resp.raise_for_status()
        wh = resp.json()
        product = _classify_warehouse(wh)
        return {"name": wh.get("name", ""), "size": wh.get("cluster_size", ""),
                "product": product}
    except Exception:
        return {"name": "", "size": "", "product": ""}


REYDEN_SIZES = ["Small", "Medium", "Large", "X-Large"]


def create_reyden_warehouse(name: str, size: str = "Small") -> dict:
    """Create a new Lakehouse//RT (REYDEN) warehouse. Returns {"id": ..., "error": ...}."""
    headers = _api_headers()
    payload = {
        "name": name,
        "cluster_size": size,
        "min_num_clusters": 1,
        "max_num_clusters": 1,
        "auto_stop_mins": 1,
        "auto_resume": True,
        "warehouse_type": "REYDEN",
        "enable_serverless_compute": True,
        "enable_photon": True,
        "spot_instance_policy": "COST_OPTIMIZED",
        "channel": {"name": "CHANNEL_NAME_CURRENT"},
    }
    try:
        resp = _requests.post(f"{HOST}/api/2.0/sql/warehouses", headers=headers,
                              json=payload, timeout=60)
        data = resp.json()
        if resp.status_code >= 400:
            msg = data.get("message", "") or data.get("error", "") or resp.text[:300]
            return {"id": None, "error": msg}
        return {"id": data.get("id", ""), "error": ""}
    except Exception as e:
        return {"id": None, "error": str(e)}



# ---------------------------------------------------------------------------
# Catalog discovery & permission checks
# SQL-based (information_schema) because the app's user token only carries the
# `sql` scope -- the Unity Catalog REST APIs are not available to it.
# ---------------------------------------------------------------------------
_HIDDEN_CATALOGS = {"system", "samples", "hive_metastore"}
_CATALOG_USE_PRIVS = {"USE_CATALOG", "ALL_PRIVILEGES"}
_SCHEMA_CREATE_PRIVS = {"CREATE_SCHEMA", "ALL_PRIVILEGES"}
_SCHEMA_USE_PRIVS = {"USE_SCHEMA", "ALL_PRIVILEGES"}
_TABLE_CREATE_PRIVS = {"CREATE_TABLE", "ALL_PRIVILEGES"}
_ME_GRANTEE = ("(p.grantee = current_user() OR is_account_group_member(p.grantee) "
               "OR is_member(p.grantee))")
_PERM_ERR_MARKERS = ("permission_denied", "insufficient_permissions", "does not have",
                     "not authorized", "access denied", "unauthorized")


def _sql_str(v) -> str:
    """Quote a value as a SQL string literal."""
    return "'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'"


def _q(ident) -> str:
    """Quote a SQL identifier with backticks."""
    return "`" + str(ident).replace("`", "``") + "`"


def _split_privs(raw) -> set:
    return {p.strip().upper().replace(" ", "_") for p in str(raw or "").split(",") if p.strip()}


def _is_true(v) -> bool:
    return str(v).strip().lower() == "true"


def _is_permission_error(msg) -> bool:
    m = str(msg or "").lower()
    return any(k in m for k in _PERM_ERR_MARKERS)


def _pick_metadata_warehouse(warehouses: list, preferred_id: str | None = None) -> str | None:
    """Pick a warehouse for read-only metadata queries, preferring a running one."""
    usable = [wh for wh in warehouses if wh.get("id")]
    pref = preferred_id if preferred_id and preferred_id != "__CREATE_NEW__" else None
    for wh in usable:
        if wh["id"] == pref and "RUNNING" in str(wh.get("state", "")).upper():
            return pref
    for wh in usable:
        if "RUNNING" in str(wh.get("state", "")).upper():
            return wh["id"]
    if pref:
        return pref
    return usable[0]["id"] if usable else None


def list_catalogs(warehouse_id: str) -> tuple[list, str, str]:
    """List catalogs visible to the current user with their effective privileges.
    Returns (catalogs, current_user, error). Group grants are resolved via
    is_account_group_member()."""
    query = (
        "SELECT c.catalog_name, c.catalog_owner, "
        "CAST((c.catalog_owner = current_user() OR is_account_group_member(c.catalog_owner) "
        "OR is_member(c.catalog_owner)) AS STRING) AS is_owner, "
        "array_join(collect_set(p.privilege_type), ',') AS privs, current_user() AS me "
        "FROM system.information_schema.catalogs c "
        "LEFT JOIN system.information_schema.catalog_privileges p "
        f"ON p.catalog_name = c.catalog_name AND {_ME_GRANTEE} "
        "GROUP BY c.catalog_name, c.catalog_owner ORDER BY c.catalog_name"
    )
    rows, err = execute_sql_rows(warehouse_id, query, "system", "information_schema")
    if err:
        return [], "", err
    me, out = "", []
    for r in rows:
        me = me or str(r.get("me") or "")
        name = str(r.get("catalog_name") or "")
        if not name or name in _HIDDEN_CATALOGS or name.startswith("__"):
            continue
        privs = _split_privs(r.get("privs"))
        is_owner = _is_true(r.get("is_owner"))
        use_cat = is_owner or bool(privs & _CATALOG_USE_PRIVS)
        if not use_cat:
            continue  # visible (e.g. BROWSE only) but not usable
        out.append({
            "name": name,
            "owner": str(r.get("catalog_owner") or ""),
            "is_owner": is_owner,
            "privs": privs,
            "can_create_schema": is_owner or (use_cat and bool(privs & _SCHEMA_CREATE_PRIVS)),
            "can_manage": is_owner or "MANAGE" in privs,
        })
    return out, me, ""


def check_create_permissions(warehouse_id: str, catalog: str, schemas: list) -> dict:
    """Pre-flight: can the current user create the given benchmark schemas (or
    tables inside them, if the schema already exists) in `catalog`?

    Based on Unity Catalog grants visible to the user (ownership, direct + group
    grants, inherited). Metastore-admin access is not detectable here, so callers
    should allow an override. Returns a dict; `blocked` lists schemas the user
    likely cannot populate."""
    cat = str(catalog or "").strip().lower()
    schemas = [str(s).lower() for s in schemas]
    perm = {"catalog": catalog, "me": "", "catalog_owner": "", "catalog_missing": False,
            "can_create_schema": False, "can_manage_catalog": False,
            "schemas": {}, "blocked": [], "error": ""}
    q_cat = (
        "SELECT c.catalog_owner, "
        "CAST((c.catalog_owner = current_user() OR is_account_group_member(c.catalog_owner) "
        "OR is_member(c.catalog_owner)) AS STRING) AS is_owner, "
        "array_join(collect_set(p.privilege_type), ',') AS privs, current_user() AS me "
        "FROM system.information_schema.catalogs c "
        "LEFT JOIN system.information_schema.catalog_privileges p "
        f"ON p.catalog_name = c.catalog_name AND {_ME_GRANTEE} "
        f"WHERE c.catalog_name = {_sql_str(cat)} GROUP BY c.catalog_owner"
    )
    rows, err = execute_sql_rows(warehouse_id, q_cat, "system", "information_schema")
    if err:
        perm["error"] = err
        return perm
    if not rows:
        perm["catalog_missing"] = True
        perm["blocked"] = list(schemas)
        return perm

    r = rows[0]
    cat_owner = _is_true(r.get("is_owner"))
    cprivs = _split_privs(r.get("privs"))
    use_cat = cat_owner or bool(cprivs & _CATALOG_USE_PRIVS)
    perm["me"] = str(r.get("me") or "")
    perm["catalog_owner"] = str(r.get("catalog_owner") or "")
    perm["can_create_schema"] = cat_owner or (use_cat and bool(cprivs & _SCHEMA_CREATE_PRIVS))
    perm["can_manage_catalog"] = cat_owner or "MANAGE" in cprivs
    if not schemas:
        return perm

    in_list = ", ".join(_sql_str(s) for s in schemas)
    q_sch = (
        "SELECT s.schema_name, s.schema_owner, "
        "CAST((s.schema_owner = current_user() OR is_account_group_member(s.schema_owner) "
        "OR is_member(s.schema_owner)) AS STRING) AS is_owner, "
        "array_join(collect_set(p.privilege_type), ',') AS privs "
        "FROM system.information_schema.schemata s "
        "LEFT JOIN system.information_schema.schema_privileges p "
        f"ON p.catalog_name = s.catalog_name AND p.schema_name = s.schema_name AND {_ME_GRANTEE} "
        f"WHERE s.catalog_name = {_sql_str(cat)} AND s.schema_name IN ({in_list}) "
        "GROUP BY s.schema_name, s.schema_owner"
    )
    srows, serr = execute_sql_rows(warehouse_id, q_sch, "system", "information_schema")
    if serr:
        perm["error"] = serr
        return perm
    found = {str(x.get("schema_name") or "").lower(): x for x in srows}
    for s in schemas:
        x = found.get(s)
        if x is None:
            ok = perm["can_create_schema"]
            perm["schemas"][s] = {"exists": False, "owner": "", "ok": ok}
        else:
            s_owner = _is_true(x.get("is_owner"))
            sprivs = _split_privs(x.get("privs"))
            ok = cat_owner or (use_cat and (s_owner or (
                bool(sprivs & _SCHEMA_USE_PRIVS) and bool(sprivs & _TABLE_CREATE_PRIVS))))
            perm["schemas"][s] = {"exists": True, "owner": str(x.get("schema_owner") or ""), "ok": ok}
        if not ok:
            perm["blocked"].append(s)
    return perm


def _render_permission_help(perm: dict, catalogs: list | None = None) -> None:
    """Explain a data-creation permission problem and list concrete ways to resolve it."""
    catalog = perm.get("catalog", "")
    me = (perm.get("me") or (st.session_state.get("_catalogs") or {}).get("me")
          or "<your-user-or-group>")
    blocked = list(perm.get("blocked") or [])
    sch_info = perm.get("schemas") or {}
    new_schemas = [s for s in blocked if not sch_info.get(s, {}).get("exists")]
    existing = [s for s in blocked if sch_info.get(s, {}).get("exists")]
    owner = perm.get("catalog_owner") or ""
    missing_cat = perm.get("catalog_missing")

    if missing_cat:
        st.error(
            f"Catalog `{catalog}` doesn't exist or you don't have access to it. "
            f"Please request access to catalog `{catalog}`, or choose a different catalog.")
        return
    if new_schemas:
        st.error(
            f"Benchmark data doesn't exist in catalog `{catalog}`, and you don't have "
            f"permission to create schemas there. Please request access to create schemas "
            f"in catalog `{catalog}`, or choose a different catalog.")
    if existing:
        st.error(
            f"Benchmark data is incomplete in catalog `{catalog}`, and you don't have "
            f"permission to create tables in " + ", ".join(f"`{catalog}.{s}`" for s in existing)
            + ". Please request access to create tables in those schemas, or choose a "
            "different catalog.")


EXPECTED_ROWS = {
    "tpch_sf1": {"region": 5, "nation": 25, "supplier": 10_000, "customer": 150_000,
                 "part": 200_000, "partsupp": 800_000, "orders": 1_500_000, "lineitem": 6_001_215},
    "tpch_sf10": {"region": 5, "nation": 25, "supplier": 100_000, "customer": 1_500_000,
                  "part": 2_000_000, "partsupp": 8_000_000, "orders": 15_000_000, "lineitem": 59_986_052},
    "tpch_sf100": {"region": 5, "nation": 25, "supplier": 1_000_000, "customer": 15_000_000,
                   "part": 20_000_000, "partsupp": 80_000_000, "orders": 150_000_000, "lineitem": 600_037_902},
    "tpch_sf1000": {"region": 5, "nation": 25, "supplier": 10_000_000, "customer": 150_000_000,
                    "part": 200_000_000, "partsupp": 800_000_000, "orders": 1_500_000_000, "lineitem": 5_999_989_709},
    "clickbench": {"hits": 99_997_497},
}

# Clustering spec per workload (only certain SF / workloads have clustering)
# {workload_key: {table: [cluster_columns]}}
CLUSTERING_SPEC = {
    "tpch_sf1000": {
        "customer": ["c_mktsegment"],
        "orders": ["o_orderdate"],
        "lineitem": ["l_shipdate"],
    },
    "clickbench": {
        "hits": ["CounterID", "EventDate", "UserID"],
    },
}

# Tables that need OPTIMIZE FULL after clustering
OPTIMIZE_TABLES = {
    "tpch_sf1000": ["customer", "orders", "lineitem"],
    "clickbench": ["hits"],
}


def _target_schema(workload_key: str) -> str:
    """The canonical schema name for a workload."""
    meta = WORKLOAD_META[workload_key]
    return meta["schema"]  # e.g. "tpch_sf1", "clickbench"



def validate_benchmark_data(warehouse_id: str, catalog: str, workload_key: str,
                            write_warehouse_id: str = None,
                            status_fn=None) -> dict:
    """Validate benchmark data. Silently fix stats if possible.

    Returns: {schema_exists, tables: {name: {exists, row_count, row_count_ok, issues[]}},
              all_valid, needs_creation, issues[]}
    """
    meta = WORKLOAD_META[workload_key]
    expected_tables = meta["tables"]
    schema = _target_schema(workload_key)
    expected_rows = EXPECTED_ROWS.get(workload_key, {})
    cluster_spec = CLUSTERING_SPEC.get(workload_key, {})

    result = {"schema": schema, "schema_exists": False, "tables": {},
              "all_valid": False, "needs_creation": False, "issues": []}

    if status_fn:
        status_fn("Checking schema...")

    table_in = ", ".join("'" + t + "'" for t in expected_tables)
    query = (
        f"SELECT table_name, CAST(NULL AS BIGINT) AS row_count "
        f"FROM system.information_schema.tables "
        f"WHERE table_catalog = {_sql_str(catalog)} "
        f"AND table_schema = '{schema}' "
        f"AND LOWER(table_name) IN ({table_in})"
    )
    # Run from system context: the target catalog may be unusable or lack a `default` schema
    rows, err = execute_sql_rows(warehouse_id, query, "system", "information_schema")
    if err:
        result["issues"].append(f"Schema check failed: {err}")
        return result

    found_tables = set()
    for r in rows:
        for k in r:
            if k.lower() == "table_name":
                found_tables.add(str(r[k]).lower())

    if not found_tables:
        result["needs_creation"] = True
        result["issues"].append(f"Schema `{catalog}.{schema}` needs to be created")
        return result

    result["schema_exists"] = True
    missing = set(t.lower() for t in expected_tables) - found_tables
    if missing:
        result["needs_creation"] = True
        result["issues"].append(f"Missing tables: {', '.join(sorted(missing))}")
        return result

    # --- Parallel validation ---
    tables_to_check = sorted(found_tables & set(t.lower() for t in expected_tables))
    if status_fn:
        status_fn("Validating tables...")

    _hdrs = _api_headers()

    def _check_table(table: str) -> tuple:
        info = {"exists": True, "row_count": None, "row_count_ok": True, "issues": []}

        # Row count
        cnt_rows, cnt_err = execute_sql_rows(
            warehouse_id,
            f"SELECT COUNT(1) AS cnt FROM {_q(catalog)}.{schema}.{table}",
            catalog, schema, headers=_hdrs)
        if not cnt_err and cnt_rows:
            try:
                cnt_val = int(list(cnt_rows[0].values())[0])
            except (ValueError, TypeError, IndexError):
                cnt_val = None
            info["row_count"] = cnt_val
            if cnt_val is not None:
                expected = expected_rows.get(table, 0)
                # Allow 0.1% tolerance for FK-filtered tables
                if expected:
                    tol = max(1, int(expected * 0.001))
                    info["row_count_ok"] = abs(cnt_val - expected) <= tol
                else:
                    info["row_count_ok"] = True
                if not info["row_count_ok"]:
                    info["issues"].append(f"row count {cnt_val:,} != expected {expected:,}")

        # Check stats + clustering via DESCRIBE DETAIL
        det_rows, det_err = execute_sql_rows(
            warehouse_id,
            f"DESCRIBE DETAIL {_q(catalog)}.{schema}.{table}",
            catalog, schema, headers=_hdrs)
        needs_stats = False
        if not det_err and det_rows:
            detail = det_rows[0]
            # Check stats via DESCRIBE EXTENDED
            ext_rows, ext_err = execute_sql_rows(
                warehouse_id,
                f"DESCRIBE EXTENDED {_q(catalog)}.{schema}.{table}",
                catalog, schema, headers=_hdrs)
            stats_found = False
            if not ext_err and ext_rows:
                for dr in ext_rows:
                    col_name = ""
                    data_type = ""
                    for k in dr:
                        kl = k.lower()
                        if kl in ("col_name", "name"):
                            col_name = str(dr[k]).strip()
                        elif kl in ("data_type", "value"):
                            data_type = str(dr[k]).strip()
                    if col_name == "Statistics" and data_type and "bytes" in data_type.lower():
                        stats_found = True
                        break
            if not stats_found:
                needs_stats = True

            # Clustering check
            cluster_cols_found = []
            raw_cluster = detail.get("clusteringColumns", None)
            if raw_cluster:
                if isinstance(raw_cluster, list):
                    cluster_cols_found = [str(c).strip().lower() for c in raw_cluster if c]
                elif isinstance(raw_cluster, str):
                    try:
                        parsed = json.loads(raw_cluster)
                        if isinstance(parsed, list):
                            cluster_cols_found = [str(c).strip().lower() for c in parsed if c]
                    except (json.JSONDecodeError, ValueError):
                        cluster_cols_found = [
                            c.strip().lower()
                            for c in raw_cluster.replace('[','').replace(']','').replace('"','').split(',')
                            if c.strip()
                        ]
            # TODO: Re-enable clustering validation after tuning is finalized
            # if table in cluster_spec:
            #     expected_cluster = sorted(c.lower() for c in cluster_spec[table])
            #     actual_cluster = sorted(cluster_cols_found)
            #     if actual_cluster != expected_cluster:
            #         info["issues"].append(
            #             f"clustering mismatch: found {cluster_cols_found}, expected {expected_cluster}")

        # Silently fix stats if we have a write warehouse
        if needs_stats and write_warehouse_id:
            try:
                _w_hdrs = _hdrs  # same user token
                fix_result = execute_sql(
                    write_warehouse_id,
                    f"ANALYZE TABLE {_q(catalog)}.{schema}.{table} COMPUTE STATISTICS FOR ALL COLUMNS",
                    catalog, schema, headers=_w_hdrs)
                if fix_result.get("status") != "ok":
                    info["issues"].append(
                        f"stats missing; auto-fix failed — run: "
                        f"ANALYZE TABLE {_q(catalog)}.{schema}.{table} COMPUTE STATISTICS FOR ALL COLUMNS")
            except Exception:
                info["issues"].append(
                    f"stats missing; auto-fix failed — run: "
                    f"ANALYZE TABLE {_q(catalog)}.{schema}.{table} COMPUTE STATISTICS FOR ALL COLUMNS")
        elif needs_stats:
            info["issues"].append(
                f"stats missing — run: "
                f"ANALYZE TABLE {_q(catalog)}.{schema}.{table} COMPUTE STATISTICS FOR ALL COLUMNS")

        return table, info

    with ThreadPoolExecutor(max_workers=min(len(tables_to_check), 8)) as pool:
        futures = {pool.submit(_check_table, t): t for t in tables_to_check}
        for fut in as_completed(futures):
            try:
                tbl, info = fut.result(timeout=300)
                result["tables"][tbl] = info
            except Exception as e:
                tbl = futures[fut]
                result["tables"][tbl] = {
                    "exists": True, "row_count": None, "row_count_ok": False,
                    "issues": [f"validation error: {e}"]}

    for t, info in result["tables"].items():
        for issue in info.get("issues", []):
            result["issues"].append(f"{t}: {issue}")

    result["all_valid"] = (
        result["schema_exists"]
        and all(
            info["row_count_ok"] and not info.get("issues")
            for info in result["tables"].values()
        )
    )
    return result


def create_benchmark_tables(warehouse_id: str, catalog: str, workload_key: str,
                            progress_fn=None) -> list:
    """Create the benchmark schema + tables. Parallelizes CTAS, OPTIMIZE, and ANALYZE phases.
    Returns a list of error strings (empty on success). Never raises."""
    import sys
    meta = WORKLOAD_META[workload_key]
    schema = _target_schema(workload_key)
    tables = meta["tables"]
    source_schema = meta["source_schema"]
    cluster_spec = CLUSTERING_SPEC.get(workload_key, {})
    optimize_tables = OPTIMIZE_TABLES.get(workload_key, [])
    total_ops = len(tables) + len(optimize_tables) + len(tables)  # CTAS + OPTIMIZE + ANALYZE
    _step = {"n": 0}
    _errors = []  # collect errors from workers

    # Capture auth headers on the main Streamlit thread BEFORE spawning workers
    _hdrs = _api_headers()
    print(f"[create_benchmark_tables] headers captured, token present: {bool(_hdrs.get('Authorization'))}",
          flush=True)

    if progress_fn:
        progress_fn("Creating schema...", 0, total_ops)
    r_schema = execute_sql(warehouse_id, f"CREATE SCHEMA IF NOT EXISTS {_q(catalog)}.{schema}",
                           "system", "information_schema", headers=_hdrs)
    if r_schema["status"] != "ok":
        err = f"CREATE SCHEMA {catalog}.{schema}: {r_schema.get('error') or 'unknown error'}"
        print(f"[create] FAILED: {err}", flush=True)
        if progress_fn:
            progress_fn(f"\u274c {err}", 0, total_ops)
        return [err]

    def _run_table_pipeline(t):
        """Run CTAS \u2192 OPTIMIZE \u2192 ANALYZE for a single table in this thread."""
        t_errors = []

        # 1. CTAS
        cluster_clause = ""
        if t in cluster_spec:
            cols = ", ".join(cluster_spec[t])
            cluster_clause = f" CLUSTER BY ({cols})"
        ctas_sql = (f"CREATE TABLE IF NOT EXISTS {_q(catalog)}.{schema}.{t} "
                    f"USING DELTA{cluster_clause} AS SELECT * FROM samples.{source_schema}.{t}")
        print(f"[create] CTAS {t}: submitting...", flush=True)
        r = execute_sql(warehouse_id, ctas_sql, catalog, schema, headers=_hdrs)
        if r["status"] != "ok":
            err = f"CTAS {t}: {r.get('error', 'unknown')}"
            print(f"[create] FAILED: {err}", flush=True)
            t_errors.append(err)
        else:
            print(f"[create] CTAS {t}: done in {r['latency']:.1f}s", flush=True)
        _step["n"] += 1

        # 2. OPTIMIZE (only for applicable tables, skip if CTAS failed)
        if t in optimize_tables and not t_errors:
            print(f"[create] OPTIMIZE {t}: submitting...", flush=True)
            r = execute_sql(warehouse_id, f"OPTIMIZE {_q(catalog)}.{schema}.{t} FULL",
                            catalog, schema, headers=_hdrs)
            if r["status"] != "ok":
                err = f"OPTIMIZE {t}: {r.get('error', 'unknown')}"
                print(f"[create] FAILED: {err}", flush=True)
                t_errors.append(err)
            else:
                print(f"[create] OPTIMIZE {t}: done in {r['latency']:.1f}s", flush=True)
        if t in optimize_tables:
            _step["n"] += 1

        # 3. ANALYZE (skip if CTAS failed)
        if not t_errors:
            print(f"[create] ANALYZE {t}: submitting...", flush=True)
            r = execute_sql(warehouse_id,
                            f"ANALYZE TABLE {_q(catalog)}.{schema}.{t} COMPUTE STATISTICS FOR ALL COLUMNS",
                            catalog, schema, headers=_hdrs)
            if r["status"] != "ok":
                err = f"ANALYZE {t}: {r.get('error', 'unknown')}"
                print(f"[create] FAILED: {err}", flush=True)
                t_errors.append(err)
            else:
                print(f"[create] ANALYZE {t}: done in {r['latency']:.1f}s", flush=True)
        _step["n"] += 1

        return t, t_errors

    # --- Run all tables in parallel (each thread: CTAS \u2192 OPTIMIZE \u2192 ANALYZE) ---
    if progress_fn:
        progress_fn(f"Creating {len(tables)} tables in parallel (CTAS \u2192 OPTIMIZE \u2192 ANALYZE)...",
                    0, total_ops)

    with ThreadPoolExecutor(max_workers=min(len(tables), 8)) as pool:
        futs = {pool.submit(_run_table_pipeline, t): t for t in tables}
        for f in as_completed(futs):
            t_name = futs[f]
            try:
                _, t_errs = f.result()
                if t_errs:
                    _errors.extend(t_errs)
                    if progress_fn:
                        progress_fn(f"\u274c {t_name}: {'; '.join(t_errs)}",
                                    _step["n"], total_ops)
                else:
                    if progress_fn:
                        progress_fn(f"\u2705 {t_name} ready", _step["n"], total_ops)
            except Exception as exc:
                _errors.append(f"{t_name}: {exc}")
                if progress_fn:
                    progress_fn(f"\u274c {t_name}: {exc}", _step["n"], total_ops)

    if _errors:
        print(f"[create_benchmark_tables] WARNINGS: {_errors}", file=sys.stderr, flush=True)
    return _errors


def fix_table_issue(warehouse_id: str, catalog: str, workload_key: str,
                    table: str, issue_type: str, progress_fn=None) -> None:
    """Fix a specific table issue."""
    schema = _target_schema(workload_key)
    source_schema = WORKLOAD_META[workload_key]["source_schema"]
    cluster_spec = CLUSTERING_SPEC.get(workload_key, {})
    optimize_tables = OPTIMIZE_TABLES.get(workload_key, [])

    if issue_type == "row_count":
        if progress_fn:
            progress_fn(f"Reloading {table}...")
        execute_sql(warehouse_id, f"DROP TABLE IF EXISTS {_q(catalog)}.{schema}.{table}", catalog, schema)
        cluster_clause = ""
        if table in cluster_spec:
            cols = ", ".join(cluster_spec[table])
            cluster_clause = f" CLUSTER BY ({cols})"
        execute_sql(
            warehouse_id,
            f"CREATE TABLE {_q(catalog)}.{schema}.{table} "
            f"USING DELTA{cluster_clause} AS SELECT * FROM samples.{source_schema}.{table}",
            catalog, schema)
        if table in optimize_tables:
            if progress_fn:
                progress_fn(f"Optimizing {table}...")
            execute_sql(warehouse_id, f"OPTIMIZE {_q(catalog)}.{schema}.{table} FULL", catalog, schema)
        if progress_fn:
            progress_fn(f"Analyzing {table}...")
        execute_sql(warehouse_id,
                    f"ANALYZE TABLE {_q(catalog)}.{schema}.{table} COMPUTE STATISTICS FOR ALL COLUMNS",
                    catalog, schema)

    elif issue_type == "stats":
        if progress_fn:
            progress_fn(f"Computing statistics for {table}...")
        execute_sql(warehouse_id,
                    f"ANALYZE TABLE {_q(catalog)}.{schema}.{table} COMPUTE STATISTICS FOR ALL COLUMNS",
                    catalog, schema)

    elif issue_type == "clustering":
        if progress_fn:
            progress_fn(f"Fixing clustering for {table}...")
        if table in cluster_spec:
            cols = ", ".join(cluster_spec[table])
            execute_sql(warehouse_id,
                        f"ALTER TABLE {_q(catalog)}.{schema}.{table} CLUSTER BY ({cols})",
                        catalog, schema)
            if progress_fn:
                progress_fn(f"Optimizing {table}...")
            execute_sql(warehouse_id, f"OPTIMIZE {_q(catalog)}.{schema}.{table} FULL", catalog, schema)



# ---------------------------------------------------------------------------
# Results write-back (disabled — kept as utility for future use)
# ---------------------------------------------------------------------------
def _save_results_to_delta(results: list, warehouse_id: str, catalog: str):
    """Write benchmark results to a Delta tracking table.
    Requires a warehouse with write permissions (e.g. DBSQL, not LH//RT).
    Currently not called — re-enable in Step 3 when tracking is needed.
    """
    write_wh = st.session_state.get("write_warehouse_id") or warehouse_id
    test_id = st.session_state.get("test_id", "unknown")
    wh_info = st.session_state.get("wh_info") or {}
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    execute_sql(write_wh, f"CREATE SCHEMA IF NOT EXISTS {catalog}.benchmark_results",
                catalog, "default")
    execute_sql(write_wh, f"""CREATE TABLE IF NOT EXISTS {RESULTS_TABLE} (
        test_id STRING, run_timestamp TIMESTAMP, workload STRING,
        workload_name STRING, scale_factor STRING, warehouse_id STRING,
        warehouse_product STRING, warehouse_name STRING, warehouse_size STRING,
        catalog STRING, query_id STRING, repetition INT,
        latency_seconds DOUBLE, row_count BIGINT, status STRING,
        statement_id STRING, connector_version STRING, cache_state STRING,
        total_queries INT, requested_runs INT
    ) USING DELTA""", catalog, "default")

    rows_sql = []
    for r in results:
        rows_sql.append(
            f"('{test_id}', '{now_str}', '{r['workload']}', '{r['workload_name']}', "
            f"'{r.get('scale_factor', '')}', '{warehouse_id}', '{wh_info.get('product', '')}', "
            f"'{wh_info.get('name', '')}', '{wh_info.get('size', '')}', "
            f"'{catalog}', '{r['query_id']}', {r['repetition']}, "
            f"{r['latency_seconds']}, {r['row_count']}, '{r['status']}', "
            f"'{r.get('statement_id', '')}', 'statement-api', 'disabled', "
            f"{WORKLOAD_META[r['workload']]['queries']}, {NUM_REPS})"
        )
    for i in range(0, len(rows_sql), 50):
        batch = rows_sql[i:i + 50]
        execute_sql(write_wh, f"INSERT INTO {RESULTS_TABLE} VALUES {', '.join(batch)}",
                    catalog, "benchmark_results")


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.markdown(
        '<p style="text-align:center;font-weight:700;font-size:1.8rem;margin:1rem 0 0.2rem 0;color:#F0F3F6;">'
        'Lakehouse<span style="color:#FF3621;">//</span>RT</p>'
        '<p style="text-align:center;color:#8B949E;font-size:0.85rem;margin:0;">'
        '<span style="background:#FF3621;color:#fff;padding:2px 8px;border-radius:5px;'
        'font-size:0.7rem;vertical-align:middle;margin-right:5px;">\u26a1 RT</span> BENCHMARK SUITE</p>',
        unsafe_allow_html=True,
    )
    st.markdown("---")

    # --- Turning the app on/off: done from the app's overview page (no auto-stop) ---
    _app_name = os.getenv("DATABRICKS_APP_NAME", "lakehouse-rt-benchmark")
    _ws_id = os.getenv("DATABRICKS_WORKSPACE_ID", "")
    _app_page = (f"{HOST}/apps/{_app_name}" + (f"?o={_ws_id}" if _ws_id else "")) if _raw_host else ""
    st.markdown("**Turning this app on or off**")
    st.caption(
        "This app does **not** stop automatically when idle, and its compute is billed while it "
        "is running. We suggest stopping it when it isn't being used.\n\n"
        "To turn the app on or off, use **Start** / **Stop** on the app's overview page in "
        "Databricks (requires CAN MANAGE on the app). Stopping the app ends any benchmark "
        "that is in progress.")
    if _app_page:
        st.link_button("Open app overview page", _app_page, use_container_width=True)
    st.markdown("---")

    # Load help content from help.md (ships with the app)
    _help_path = os.path.join(os.path.dirname(__file__), "help.md")
    try:
        with open(_help_path, encoding="utf-8") as _hf:
            _help_md = _hf.read()
    except FileNotFoundError:
        _help_md = "_Help file not found._"
    with st.expander("Help & Troubleshooting", expanded=False):
        st.markdown(_help_md)
    with st.expander("Need more help? Ask Genie", expanded=False):
        st.markdown(
            "If you're running into issues or have questions this guide doesn't cover, "
            "open the **Databricks Assistant** from anywhere in your workspace and ask "
            "about this app. The assistant can read the app's built-in `help.md` and "
            "source code to help troubleshoot.\n\n"
            "**How to:** Open the Databricks sidebar \u2192 click the Assistant icon \u2192 "
            "ask your question (e.g. *\"Why is my benchmark warehouse not showing up "
            "in the lakehouse-rt-benchmark app?\"*)"
        )


# ===================================================================
# STEP 1 — Welcome
# ===================================================================
if st.session_state.step == 1:
    _render_banner()

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("### ClickBench")
        st.markdown(
            "**43** analytical queries against a web-analytics dataset (~15 GB). "
            "Covers aggregation, filtering, sorting, and string operations "
            "on a single wide table."
        )
    with col2:
        st.markdown("### TPC-H")
        st.markdown(
            "**22** TPC-H-derived queries at scale factors from 1 GB to 1 TB. "
            "Covers complex joins, subqueries, and aggregations across an "
            "8-table star schema."
        )

    st.markdown("")
    if st.button("Get Started", type="primary", use_container_width=True):
        st.session_state.step = 2
        st.rerun()


# ===================================================================
# STEP 2 — Configure (multi-benchmark)
# ===================================================================
elif st.session_state.step == 2:
    _render_banner()
    _step_chip("STEP 1 \u2014 CONFIGURE")
    st.markdown("### Select Benchmarks")

    # --- Workload selection ---
    cb_col, tpch_col = st.columns(2)
    with cb_col:
        run_clickbench = st.checkbox("ClickBench", value=False,
                                     help="43 queries, ~15 GB web-analytics dataset")
    with tpch_col:
        run_tpch = st.checkbox("TPC-H", value=False,
                               help="22 queries across 8-table star schema")

    selected_workloads = []
    if run_clickbench:
        selected_workloads.append("clickbench")

    if run_tpch:
        st.markdown("**TPC-H Scale Factors**")
        sf_cols = st.columns(4)
        sf_checks = {}
        for i, (sf_key, sf_label) in enumerate([
            ("tpch_sf1", "SF1 (~1 GB)"), ("tpch_sf10", "SF10 (~10 GB)"),
            ("tpch_sf100", "SF100 (~100 GB)"),
            ("tpch_sf1000", "SF1000 (~1 TB)")
        ]):
            with sf_cols[i]:
                sf_checks[sf_key] = st.checkbox(sf_label, value=False)
        for sf_key in ["tpch_sf1", "tpch_sf10", "tpch_sf100", "tpch_sf1000"]:
            if sf_checks.get(sf_key):
                selected_workloads.append(sf_key)

    if run_tpch and not any(sf_checks.get(k) for k in ["tpch_sf1", "tpch_sf10", "tpch_sf100", "tpch_sf1000"]):
        st.warning("Select at least one TPC-H scale factor.")
        if not run_clickbench:
            st.markdown("---")
            st.stop()
    if not selected_workloads:
        st.warning("Select at least one benchmark to continue.")
        st.markdown("---")
        st.stop()

    # --- Determine largest required WH size ---
    size_order = {"Small": 0, "Medium": 1, "Large": 2, "X-Large": 3}
    required_size = max(
        (WORKLOAD_WH_SIZE.get(w, "Small") for w in selected_workloads),
        key=lambda s: size_order.get(s, 0)
    )

    st.markdown("---")
    st.markdown("### Select Warehouses")

    # Fetch all warehouses once
    if "_all_warehouses" not in st.session_state:
        st.session_state._all_warehouses = list_warehouses()
    all_warehouses = st.session_state._all_warehouses
    rt_warehouses = [wh for wh in all_warehouses if wh["product"] == "LAKEHOUSE_RT"]
    dbsql_warehouses = [wh for wh in all_warehouses if wh["product"] != "LAKEHOUSE_RT"]
    rt_matching = [wh for wh in rt_warehouses
                   if wh["size"].upper() == required_size.upper()]

    # --- Reyden (benchmark) warehouse ---
    st.markdown(f"**Lakehouse//RT Warehouse** (runs the benchmark \u2014 {required_size})")
    col_wh_mode, col_refresh = st.columns([4, 1])
    with col_wh_mode:
        wh_mode = st.radio("Warehouse", ["Use existing", "Create new"],
                           horizontal=True, label_visibility="collapsed")
    with col_refresh:
        if st.button("\u21bb Refresh", help="Reload warehouse and catalog lists"):
            st.session_state.pop("_all_warehouses", None)
            st.session_state.pop("_catalogs", None)
            st.rerun()

    warehouse_id = warehouse_name = None

    if wh_mode == "Use existing":
        if not rt_matching:
            st.warning(f"No Lakehouse//RT warehouses of size **{required_size}** found.")
        else:
            labels = [wh["label"] for wh in rt_matching]
            idx = st.selectbox("Select Reyden warehouse", range(len(labels)),
                               format_func=lambda i: labels[i],
                               label_visibility="collapsed")
            warehouse_id = rt_matching[idx]["id"]
            warehouse_name = rt_matching[idx]["name"]
    else:
        new_wh_name = st.text_input("Warehouse name", placeholder="e.g. my-reyden-benchmark")
        st.caption(f"Size: **{required_size}** (auto-determined by workload selection)")
        if new_wh_name:
            warehouse_id = "__CREATE_NEW__"
            warehouse_name = new_wh_name
            st.session_state._new_wh_size = required_size

    # --- Catalog (dropdown of catalogs the signed-in user can access) ---
    st.markdown("**Catalog** (where the benchmark schemas live or will be created)")
    if "_catalogs" not in st.session_state:
        _meta_wh = _pick_metadata_warehouse(all_warehouses, warehouse_id)
        if _meta_wh:
            with st.spinner("Loading catalogs..."):
                _cats, _me, _cat_err = list_catalogs(_meta_wh)
        else:
            _cats, _me, _cat_err = [], "", "no SQL warehouse available to list catalogs"
        st.session_state._catalogs = {"items": _cats, "me": _me, "error": _cat_err}
    _cat_state = st.session_state._catalogs
    _cats = _cat_state["items"]
    _OTHER_CATALOG = "__OTHER__"

    if _cats:
        _names = [c["name"] for c in _cats]
        _by_name = {c["name"]: c for c in _cats}
        _cur = st.session_state.get("catalog") or "main"
        if _cur in _by_name:
            _default_idx = _names.index(_cur)
        else:
            _creatable = [i for i, c in enumerate(_cats) if c["can_create_schema"]]
            _default_idx = _creatable[0] if _creatable else 0

        def _catalog_label(n):
            return "Other (enter a catalog name)..." if n == _OTHER_CATALOG else n

        _sel_cat = st.selectbox("Catalog", _names + [_OTHER_CATALOG], index=_default_idx,
                                format_func=_catalog_label, label_visibility="collapsed")
        if _sel_cat == _OTHER_CATALOG:
            catalog = st.text_input("Catalog name", value="",
                                    placeholder="e.g. my_catalog").strip()
        else:
            catalog = _sel_cat
    else:
        if _cat_state.get("error"):
            st.caption(f"Couldn't load the catalog list ({str(_cat_state['error'])[:200]}). "
                       f"Enter a catalog name instead.")
        catalog = st.text_input("Catalog", value=st.session_state.get("catalog") or "main").strip()

    st.markdown("")
    col_back, col_continue = st.columns([1, 3])
    with col_back:
        if st.button("Back"):
            st.session_state.step = 1
            st.rerun()
    with col_continue:
        if st.button("Continue", type="primary", use_container_width=True):
            if not selected_workloads:
                st.error("Select at least one benchmark.")
            elif not warehouse_id:
                st.error("Select a Lakehouse//RT warehouse.")
            elif not catalog:
                st.error("Select or enter a catalog.")
            elif warehouse_id == "__CREATE_NEW__":
                with st.spinner(f"Creating Lakehouse//RT warehouse **{warehouse_name}**..."):
                    result = create_reyden_warehouse(
                        warehouse_name,
                        st.session_state.get("_new_wh_size", "Small"),
                    )
                if result["error"]:
                    st.error(f"Failed: {result['error']}")
                else:
                    st.success(f"Created **{warehouse_name}** (starting up...)")
                    st.session_state.selected_workloads = [
                        w for w in WORKLOAD_ORDER if w in selected_workloads]
                    st.session_state.warehouse_id = result["id"]
                    st.session_state.warehouse_name = warehouse_name
                    st.session_state.write_warehouse_id = None
                    st.session_state.write_warehouse_name = None
                    st.session_state.catalog = catalog
                    st.session_state.wh_info = None
                    st.session_state.validation = None
                    st.session_state.step = "2b"
                    st.rerun()
            else:
                st.session_state.selected_workloads = [
                    w for w in WORKLOAD_ORDER if w in selected_workloads]
                st.session_state.warehouse_id = warehouse_id
                st.session_state.warehouse_name = warehouse_name
                st.session_state.write_warehouse_id = None
                st.session_state.write_warehouse_name = None
                st.session_state.catalog = catalog
                st.session_state.wh_info = None
                st.session_state.validation = None
                st.session_state.step = "2b"
                st.rerun()



# ===================================================================
# STEP 2b — Validate Benchmark Data (simplified UX)
# ===================================================================
elif st.session_state.step == "2b":
    _render_banner()
    _step_chip("STEP 2 \u2014 VALIDATE")
    st.markdown("### Validate Benchmark Data")

    selected_workloads = st.session_state.selected_workloads
    catalog = st.session_state.catalog
    warehouse_id = st.session_state.warehouse_id
    write_wh_id = st.session_state.get("write_warehouse_id")

    # --- Validate warehouse connectivity ---
    if st.session_state.wh_info is None:
        with st.spinner("Connecting to warehouse..."):
            wh_info = _get_warehouse_info(warehouse_id)
            st.session_state.wh_info = wh_info
            probe = execute_sql(warehouse_id, "SELECT 1", "system", "information_schema")
            if probe["status"] != "ok":
                st.error(
                    f"Could not connect to warehouse. "
                    f"It may be stopped or you may not have access.\n\n"
                    f"Error: {probe.get('error', 'Unknown')}"
                )
                if st.button("Back to Configuration", type="primary"):
                    st.session_state.step = 2
                    st.rerun()
                st.stop()

    # --- Validate each selected workload ---
    if st.session_state.validation is None:
        all_validations = {}
        for wl_key in selected_workloads:
            meta = WORKLOAD_META[wl_key]
            with st.spinner(f"Validating **{meta['name']}** tables..."):
                v = validate_benchmark_data(
                    warehouse_id, catalog, wl_key,
                    write_warehouse_id=write_wh_id)
                all_validations[wl_key] = v
        st.session_state.validation = all_validations
        st.rerun()

    # --- Render results ---
    validations = st.session_state.validation
    _flags = {"needs_creation": False, "has_issues": False}
    stats_failures = []

    def _render_single_workload(wl_key):
        """Render validation for a single workload (vertical list)."""
        v = validations.get(wl_key, {})
        meta = WORKLOAD_META[wl_key]
        schema = meta["schema"]
        if v.get("needs_creation"):
            _flags["needs_creation"] = True
            st.warning(f"**{meta['name']}** \u2014 `{catalog}.{schema}` needs to be created")
            return
        st.markdown(f"**{meta['name']}** \u2014 `{catalog}.{schema}`")
        for t in sorted(v.get("tables", {})):
            info = v["tables"][t]
            rc = f"{info['row_count']:,}" if info["row_count"] is not None else "?"
            if info.get("issues"):
                _flags["has_issues"] = True
                st.markdown(f"\u2003\u26a0\ufe0f **{t}**")
                for iss in info["issues"]:
                    if "stats missing" in iss and "auto-fix failed" in iss:
                        stats_failures.append(iss)
                    st.caption(f"\u2003\u2003\u2003{iss}")
            else:
                st.markdown(f"\u2003\u2705 **{t}**")

    # Separate ClickBench and TPC-H workloads
    cb_workloads = [w for w in selected_workloads if w == "clickbench"]
    tpch_workloads = [w for w in selected_workloads if w.startswith("tpch_")]

    # Render ClickBench (single table, always vertical)
    for wl_key in cb_workloads:
        _render_single_workload(wl_key)

    # Render TPC-H
    if len(tpch_workloads) <= 1:
        # Single TPC-H scale: vertical list (same as ClickBench)
        for wl_key in tpch_workloads:
            _render_single_workload(wl_key)
    else:
        # Multiple TPC-H scales: matrix layout
        st.markdown("**TPC-H Validation**")
        # Check for any that need creation first
        needs_create_keys = []
        valid_keys = []
        for wl_key in tpch_workloads:
            v = validations.get(wl_key, {})
            if v.get("needs_creation"):
                _flags["needs_creation"] = True
                needs_create_keys.append(wl_key)
            else:
                valid_keys.append(wl_key)
        if needs_create_keys:
            for wl_key in needs_create_keys:
                meta = WORKLOAD_META[wl_key]
                st.warning(f"**{meta['name']}** \u2014 `{catalog}.{meta['schema']}` needs to be created")
        if valid_keys:
            # Build matrix: rows = tables, columns = scale factors
            all_tables = WORKLOAD_META[valid_keys[0]]["tables"]  # same tables for all TPC-H
            sf_labels = [WORKLOAD_META[k]["name"] for k in valid_keys]
            header = "| Table | " + " | ".join(sf_labels) + " |\n"
            header += "|---|" + "|".join(["---"] * len(valid_keys)) + "|\n"
            rows_md = ""
            for t in all_tables:
                row = f"| **{t}** |"
                for wl_key in valid_keys:
                    v = validations.get(wl_key, {})
                    info = v.get("tables", {}).get(t)
                    if info is None:
                        row += " \u274c missing |"
                        _flags["has_issues"] = True
                    elif info.get("issues"):
                        _flags["has_issues"] = True
                        row += f" \u26a0\ufe0f |"
                        for iss in info["issues"]:
                            if "stats missing" in iss and "auto-fix failed" in iss:
                                stats_failures.append(iss)
                    else:
                        row += f" \u2705 |"
                rows_md += row + "\n"
            st.markdown(header + rows_md)

    if stats_failures:
        with st.expander("Stats fix scripts (run manually or ask a table owner)"):
            for s in stats_failures:
                sql_part = s.split("run: ")[-1] if "run: " in s else s
                st.code(sql_part, language="sql")

    # --- Extract flags ---
    any_needs_creation = _flags["needs_creation"]
    any_has_issues = _flags["has_issues"]

    # --- Permission pre-flight for data creation ---
    perm_blocked = False
    if any_needs_creation:
        _needed_schemas = [WORKLOAD_META[k]["schema"] for k in selected_workloads
                           if validations.get(k, {}).get("needs_creation")]
        _pc_key = (catalog, tuple(_needed_schemas))
        if st.session_state.get("_perm_check_key") != _pc_key:
            with st.spinner(f"Checking your permissions in `{catalog}`..."):
                st.session_state._perm_check = check_create_permissions(
                    warehouse_id, catalog, _needed_schemas)
                st.session_state._perm_check_key = _pc_key
        _perm = st.session_state._perm_check
        if _perm.get("error") and not _perm.get("catalog_missing"):
            st.caption(f"Couldn't pre-check permissions ({str(_perm['error'])[:200]}). "
                       f"Data creation will be attempted and any errors reported.")
        elif _perm.get("blocked"):
            perm_blocked = True
            st.markdown("---")
            _render_permission_help(_perm, (st.session_state.get("_catalogs") or {}).get("items"))
            _pc1, _pc2 = st.columns(2)
            with _pc1:
                if st.button("Change catalog", use_container_width=True):
                    st.session_state.pop("_perm_check_key", None)
                    st.session_state.validation = None
                    st.session_state.step = 2
                    st.rerun()
            with _pc2:
                if st.button("Re-check permissions", use_container_width=True):
                    st.session_state.pop("_perm_check_key", None)
                    st.session_state.pop("_catalogs", None)
                    st.session_state.validation = None
                    st.rerun()
            if st.checkbox("I have access another way (for example, metastore admin). Try anyway.",
                           value=False):
                perm_blocked = False

    # --- DBSQL warehouse (only when data creation or fixes are needed) ---
    if any_needs_creation or any_has_issues:
        st.markdown("---")
        st.markdown("**DBSQL Warehouse** (needed for data creation / fixes)")
        if "_all_warehouses" not in st.session_state:
            st.session_state._all_warehouses = list_warehouses()
        dbsql_warehouses = [wh for wh in st.session_state._all_warehouses
                            if wh["product"] != "LAKEHOUSE_RT"]
        dbsql_wh_id = dbsql_wh_name = None
        if not dbsql_warehouses:
            st.warning("No DBSQL warehouses found. You will not be able to create or fix tables.")
        else:
            dbsql_labels = [wh["label"] for wh in dbsql_warehouses]
            dbsql_idx = st.selectbox("Select DBSQL warehouse", range(len(dbsql_labels)),
                                     format_func=lambda i: dbsql_labels[i],
                                     label_visibility="collapsed")
            dbsql_wh_id = dbsql_warehouses[dbsql_idx]["id"]
            dbsql_wh_name = dbsql_warehouses[dbsql_idx]["name"]

    # --- Actions ---
    st.markdown("---")

    # Cache option + repetitions selector
    _opt_col1, _opt_col2 = st.columns(2)
    with _opt_col1:
        disable_cache = st.checkbox(
            "Disable Query Results Cache",
            value=True,
            help="When checked, each query runs fresh without reusing cached results. "
                 "Recommended for accurate benchmarking.",
        )
    with _opt_col2:
        num_reps = st.selectbox(
            "Repetitions",
            options=[1, 2, 3],
            index=2,  # default to 3
            help="Number of times to run each query. Rep 1 is the cold run; "
                 "reps 2+ are warm runs.",
        )

    # Execution options
    exec_mode = st.radio(
        "Query execution mode",
        ["sequential", "parallel"],
        format_func=lambda m: {
            "sequential": "Sequential (one query at a time)",
            "parallel": "Parallel (all queries concurrently per repetition)",
        }[m],
        index=0, horizontal=True,
    )

    st.markdown("")
    col_b, col_c = st.columns([1, 3])
    with col_b:
        if st.button("Back"):
            st.session_state.step = 2
            st.session_state.validation = None
            st.session_state.pop("_all_warehouses", None)
            st.session_state.pop("_perm_check_key", None)
            st.rerun()
    with col_c:
        if any_needs_creation:
            if perm_blocked:
                st.button("Create Data & Run Benchmark", type="primary", use_container_width=True,
                          disabled=True, help="Resolve the permission issue above first")
            elif not dbsql_wh_id:
                st.error("Select a DBSQL warehouse to create benchmark data.")
            elif st.button("Create Data & Run Benchmark", type="primary", use_container_width=True):
                st.session_state.write_warehouse_id = dbsql_wh_id
                st.session_state.write_warehouse_name = dbsql_wh_name
                st.session_state.execution_mode = exec_mode
                st.session_state.disable_cache = disable_cache
                st.session_state.num_reps = num_reps
                st.session_state.step = "2c"
                st.rerun()
        elif any_has_issues:
            if st.button("Run Anyway", type="primary", use_container_width=True,
                         help="Some tables have issues that may affect results"):
                st.session_state.execution_mode = exec_mode
                st.session_state.disable_cache = disable_cache
                st.session_state.num_reps = num_reps
                st.session_state.test_id = f"bench_{uuid.uuid4().hex[:8]}"
                st.session_state.run_start = time.time()
                st.session_state.results = None
                st.session_state.step = 3
                st.rerun()
        else:
            if st.button("Run Benchmark", type="primary", use_container_width=True):
                st.session_state.execution_mode = exec_mode
                st.session_state.disable_cache = disable_cache
                st.session_state.num_reps = num_reps
                st.session_state.test_id = f"bench_{uuid.uuid4().hex[:8]}"
                st.session_state.run_start = time.time()
                st.session_state.results = None
                st.session_state.step = 3
                st.rerun()


# ===================================================================
# STEP 2c — Create / Fix Data
# ===================================================================
elif st.session_state.step == "2c":
    _render_banner()
    _step_chip("CREATING DATA")
    st.markdown("### Creating Benchmark Data")

    # --- Previous creation attempt failed: show why and how to resolve ---
    _failure = st.session_state.get("_create_failure")
    if _failure:
        _failed_names = ", ".join(WORKLOAD_META[k]["name"] for k in _failure["failed_workloads"])
        st.error(f"Benchmark data could not be created for **{_failed_names}**, "
                 f"so the benchmark was not started.")
        with st.expander("Error details", expanded=not _failure["is_permission"]):
            for _e in _failure["errors"][:20]:
                st.code(str(_e)[:800], language="text")
        if _failure["is_permission"] and _failure.get("perm"):
            _render_permission_help(_failure["perm"],
                                    (st.session_state.get("_catalogs") or {}).get("items"))
        _ok_wls = [k for k in st.session_state.selected_workloads
                   if k not in _failure["failed_workloads"]]
        _fb1, _fb2, _fb3 = st.columns(3)
        with _fb1:
            if st.button("Change catalog", use_container_width=True):
                st.session_state.pop("_create_failure", None)
                st.session_state.pop("_perm_check_key", None)
                st.session_state.validation = None
                st.session_state.step = 2
                st.rerun()
        with _fb2:
            if st.button("Back to validation", use_container_width=True):
                st.session_state.pop("_create_failure", None)
                st.session_state.pop("_perm_check_key", None)
                st.session_state.pop("_catalogs", None)
                st.session_state.validation = None
                st.session_state.step = "2b"
                st.rerun()
        with _fb3:
            if _ok_wls and st.button(f"Run the other {len(_ok_wls)} benchmark(s) anyway",
                                     use_container_width=True):
                st.session_state.pop("_create_failure", None)
                st.session_state.selected_workloads = _ok_wls
                st.session_state.test_id = f"bench_{uuid.uuid4().hex[:8]}"
                st.session_state.run_start = time.time()
                st.session_state.results = None
                st.session_state.step = 3
                st.rerun()
        st.stop()

    _create_errors = []  # list of (workload_key, error_message)

    # Guard: only run creation once per step-2c entry
    if not st.session_state.get("_creation_done"):
        if st.session_state.get("_creating"):
            # Already in progress on a concurrent rerun — block
            st.info("Table creation in progress\u2026 please wait.")
            st.stop()

        st.session_state._creating = True

        try:
            selected_workloads = st.session_state.selected_workloads
            catalog = st.session_state.catalog
            write_wh = st.session_state.get("write_warehouse_id") or st.session_state.warehouse_id

            for wl_key in selected_workloads:
                v = (st.session_state.validation or {}).get(wl_key, {})
                if not v.get("needs_creation"):
                    continue
                meta = WORKLOAD_META[wl_key]
                schema = _target_schema(wl_key)
                st.info(f"Creating **{meta['name']}** tables in `{catalog}.{schema}`...")

                progress_bar = st.progress(0)
                status_ph = st.empty()
                tables = meta["tables"]
                optimize_tables = OPTIMIZE_TABLES.get(wl_key, [])
                total_ops = len(tables) + len(optimize_tables) + len(tables)

                def _on_progress(msg, idx, _total=total_ops):
                    pct = min(int(((idx + 1) / _total) * 100), 99)
                    progress_bar.progress(pct)
                    status_ph.markdown(f"**{msg}**")

                wl_errors = create_benchmark_tables(write_wh, catalog, wl_key, _on_progress) or []
                progress_bar.progress(100)
                if wl_errors:
                    _create_errors.extend((wl_key, e) for e in wl_errors)
                    status_ph.error(f"**{meta['name']}**: {len(wl_errors)} step(s) failed")
                else:
                    status_ph.success(f"**{meta['name']}** tables created")

        except Exception as exc:
            _create_errors.append(("", str(exc)))
            st.warning(f"\u26a0\ufe0f Table creation encountered errors: {exc}")
        finally:
            # Always clear the lock and mark done, even on error
            st.session_state._creating = False
            st.session_state._creation_done = True

    # Any failure blocks the run (queries against missing tables would all fail)
    if _create_errors:
        st.session_state.pop("_creation_done", None)
        st.session_state.pop("_creating", None)
        _needing = [k for k in st.session_state.selected_workloads
                    if (st.session_state.validation or {}).get(k, {}).get("needs_creation")]
        _failed_wls = [k for k in st.session_state.selected_workloads
                       if k in {wk for wk, _ in _create_errors if wk}] or _needing
        _is_perm = any(_is_permission_error(e) for _, e in _create_errors)
        _perm = None
        if _is_perm:
            _cat = st.session_state.catalog
            _failed_schemas = [WORKLOAD_META[k]["schema"] for k in _failed_wls]
            try:
                _perm = check_create_permissions(st.session_state.warehouse_id, _cat, _failed_schemas)
            except Exception:
                _perm = None
            if not _perm or (_perm.get("error") and not _perm.get("catalog_missing")):
                _perm = {"catalog": _cat, "me": "", "catalog_owner": "", "schemas": {},
                         "can_manage_catalog": False}
            # The real attempt failed, so treat every failed schema as blocked
            _perm["blocked"] = _failed_schemas
        st.session_state._create_failure = {
            "errors": [e for _, e in _create_errors],
            "failed_workloads": _failed_wls,
            "is_permission": _is_perm,
            "perm": _perm,
        }
        st.rerun()

    # Transition to benchmark execution
    st.session_state.pop("_creation_done", None)
    st.session_state.pop("_creating", None)
    st.session_state.test_id = f"bench_{uuid.uuid4().hex[:8]}"
    st.session_state.run_start = time.time()
    st.session_state.results = None
    st.session_state.step = 3
    st.rerun()



# ===================================================================
# STEP 3 — Running (multi-benchmark, direct query execution)
# ===================================================================
elif st.session_state.step == 3:
    _render_banner()
    _step_chip("STEP 3 \u2014 RUNNING")
    st.markdown("### Benchmark in Progress")

    selected_workloads = st.session_state.selected_workloads
    catalog = st.session_state.catalog
    warehouse_id = st.session_state.warehouse_id
    exec_mode = st.session_state.get("execution_mode", "sequential")
    start_time = st.session_state.run_start or time.time()

    # Summary
    wl_names = [WORKLOAD_META[w]["name"] for w in selected_workloads]
    total_queries = sum(WORKLOAD_META[w]["queries"] for w in selected_workloads)
    num_reps = st.session_state.get("num_reps", NUM_REPS)
    total_executions = total_queries * num_reps
    mode_label = "Parallel" if exec_mode == "parallel" else "Sequential"

    st.info(
        f"**Workloads:** {', '.join(wl_names)} \u2014 "
        f"{total_queries} queries \u00d7 {num_reps} repetitions  \n"
        f"**Warehouse:** {st.session_state.warehouse_name}  \n"
        f"**Mode:** {mode_label}  \n"
        f"**Query Results Cache:** {'Disabled' if st.session_state.get('disable_cache', True) else 'Enabled'}"
    )

    disable_cache = st.session_state.get("disable_cache", True)

    # Progress UI
    overall_progress = st.progress(0)
    col_elapsed, col_completed = st.columns(2)
    elapsed_ph = col_elapsed.empty()
    completed_ph = col_completed.empty()
    phase_msg = st.empty()
    live_table = st.empty()

    all_results = []
    _counter = {"completed": 0}

    def _update_progress():
        pct = _counter["completed"] / total_executions if total_executions else 1
        overall_progress.progress(min(pct, 1.0))
        elapsed_ph.metric("Elapsed", f"{(time.time() - start_time):.0f}s")
        completed_ph.metric("Completed", f"{_counter['completed']}/{total_executions}")

    def _update_live_table():
        if all_results:
            df = pd.DataFrame(all_results)
            latest = df.groupby(["workload_name", "query_id"]).last().reset_index()
            cols = ["workload_name", "query_id", "repetition", "latency_seconds", "status"]
            live_table.dataframe(latest[cols], use_container_width=True, hide_index=True)

    # --- Sequential execution ---
    if exec_mode == "sequential":
        for wl_key in selected_workloads:
            meta = WORKLOAD_META[wl_key]
            wl_name = meta["name"]
            schema = _target_schema(wl_key)
            queries = benchmark_queries.get_queries(wl_key)
            wh_info = st.session_state.wh_info or {}
            sf = wl_key.replace("tpch_sf", "") if "tpch" in wl_key else ""

            phase_msg.markdown(f"#### Connecting for {wl_name}...")
            try:
                bench_conn = _get_benchmark_connection(
                    warehouse_id, catalog, schema, disable_cache=disable_cache)
            except Exception as exc:
                phase_msg.error(f"Could not open benchmark connection for {wl_name}: {exc}")
                bench_conn = None

            phase_msg.markdown(f"#### Running {wl_name}...")

            for rep in range(1, num_reps + 1):
                rep_label = "cold" if rep == 1 else "warm"
                for q_id, q_sql in queries:
                    phase_msg.markdown(
                        f"#### {wl_name} \u2014 Rep {rep}/{num_reps} ({rep_label}) \u2014 {q_id}")
                    _update_progress()
                    try:
                        if bench_conn is not None:
                            r = execute_benchmark_query(bench_conn, q_sql)
                        else:
                            r = execute_sql(warehouse_id, q_sql, catalog, schema,
                                            disable_cache=disable_cache)
                    except Exception as exc:
                        r = {"latency": 0, "row_count": 0, "status": "error",
                             "statement_id": "", "error": str(exc)}
                    all_results.append({
                        "query_id": q_id, "repetition": rep,
                        "latency_seconds": round(r["latency"], 4),
                        "row_count": r["row_count"], "status": r["status"],
                        "statement_id": r["statement_id"], "error": r.get("error", ""),
                        "workload": wl_key, "workload_name": wl_name,
                        "scale_factor": sf,
                    })
                    _counter["completed"] += 1
                    _update_progress()
                _update_live_table()

            if bench_conn is not None:
                try:
                    bench_conn.close()
                except Exception:
                    pass

    # --- Parallel execution ---
    else:
        import threading, queue
        _results_lock = threading.Lock()
        PARALLEL_POOL_SIZE = 8  # pre-opened connections

        def _run_parallel_query(conn_pool, wl_key, wl_name, sf, rep, q_id, q_sql):
            """Grab a connection from the pool, execute, return it."""
            conn = conn_pool.get()  # blocks until a connection is free
            try:
                r = execute_benchmark_query(conn, q_sql)
            except Exception as exc:
                r = {"latency": 0, "row_count": 0, "status": "error",
                     "statement_id": "", "error": str(exc)}
            finally:
                conn_pool.put(conn)  # return connection to pool
            result = {
                "query_id": q_id, "repetition": rep,
                "latency_seconds": round(r["latency"], 4),
                "row_count": r["row_count"], "status": r["status"],
                "statement_id": r["statement_id"], "error": r.get("error", ""),
                "workload": wl_key, "workload_name": wl_name,
                "scale_factor": sf,
            }
            with _results_lock:
                all_results.append(result)
                _counter["completed"] += 1
            return result

        for wl_key in selected_workloads:
            meta = WORKLOAD_META[wl_key]
            wl_name = meta["name"]
            schema = _target_schema(wl_key)
            queries = benchmark_queries.get_queries(wl_key)
            sf = wl_key.replace("tpch_sf", "") if "tpch" in wl_key else ""
            pool_size = min(len(queries), PARALLEL_POOL_SIZE)

            # Pre-open a pool of connections (sequentially, one at a time)
            phase_msg.markdown(f"#### Opening {pool_size} connections for {wl_name}...")
            conn_pool = queue.Queue()
            opened = 0
            for _ in range(pool_size):
                try:
                    c = _get_benchmark_connection(
                        warehouse_id, catalog, schema, disable_cache=disable_cache)
                    conn_pool.put(c)
                    opened += 1
                except Exception as exc:
                    phase_msg.warning(f"Could only open {opened}/{pool_size} connections: {exc}")
                    break

            if opened == 0:
                phase_msg.error(f"Could not open any connections for {wl_name}, skipping.")
                continue

            phase_msg.markdown(f"#### Running {wl_name} with {opened} parallel connections...")

            for rep in range(1, num_reps + 1):
                rep_label = "cold" if rep == 1 else "warm"
                phase_msg.markdown(
                    f"#### {wl_name} \u2014 Rep {rep}/{num_reps} ({rep_label}) \u2014 "
                    f"{len(queries)} queries \u00d7 {opened} connections")
                _update_progress()

                with ThreadPoolExecutor(max_workers=opened) as pool:
                    futures = {
                        pool.submit(
                            _run_parallel_query,
                            conn_pool, wl_key, wl_name, sf, rep, q_id, q_sql
                        ): q_id
                        for q_id, q_sql in queries
                    }
                    for future in as_completed(futures):
                        q_id = futures[future]
                        try:
                            future.result()
                        except Exception:
                            pass
                        _update_progress()
                _update_live_table()

            # Close all pooled connections
            while not conn_pool.empty():
                try:
                    conn_pool.get_nowait().close()
                except Exception:
                    pass

    elapsed_total = time.time() - start_time
    overall_progress.progress(100)
    _update_progress()

    had_error = any(r["status"] == "error" for r in all_results)
    if had_error:
        phase_msg.warning("Benchmark completed with some errors.")
    else:
        phase_msg.success(f"\u2705 All {total_executions} queries completed in {elapsed_total:.0f}s!")

    # Enrich with server-side metrics from Query History API
    phase_msg.markdown("_Fetching server-side metrics from Query History..._")
    try:
        stmt_ids = [r["statement_id"] for r in all_results if r.get("statement_id")]
        if stmt_ids:
            metrics_map = _fetch_query_history_metrics(warehouse_id, stmt_ids)
            for r in all_results:
                sid = r.get("statement_id", "")
                if sid in metrics_map:
                    m = metrics_map[sid]
                    r["server_latency_ms"] = m.get("total_time_ms", 0)
                    r["execution_ms"] = m.get("execution_time_ms", 0)
                    r["compilation_ms"] = m.get("compilation_time_ms", 0)
                    r["rows_produced"] = m.get("rows_produced_count", 0)
                    r["read_bytes"] = m.get("read_bytes", 0)
                    r["from_cache"] = m.get("result_from_cache", False)
                    r["query_start_time_ms"] = m.get("query_start_time_ms", 0)
                    r["query_end_time_ms"] = m.get("query_end_time_ms", 0)
    except Exception:
        pass  # Non-fatal; client-side latency still available

    # Results write-back disabled for now (re-enable by calling _save_results_to_delta)
    st.session_state.results = all_results
    st.session_state.step = 4
    st.rerun()



# ===================================================================
# STEP 4 — Results (inline Plotly charts)
# ===================================================================
elif st.session_state.step == 4:
    _render_banner()
    _step_chip("RESULTS")
    # --- Heading row with run config in the upper right ---
    _heading_col, _cfg_col = st.columns([1, 1])
    with _heading_col:
        st.markdown("### Benchmark Results")
    with _cfg_col:
        _wl_names = [WORKLOAD_META[w]["name"] for w in st.session_state.get("selected_workloads", [])]
        _wh_name = st.session_state.get("warehouse_name", "Unknown")
        _wh_size = (st.session_state.get("wh_info") or {}).get("size", "")
        _num_reps = st.session_state.get("num_reps", NUM_REPS)
        _exec_mode_label = st.session_state.get("execution_mode", "sequential").capitalize()
        _cache_label = "Disabled" if st.session_state.get("disable_cache", True) else "Enabled"
        _total_q = sum(WORKLOAD_META[w]["queries"] for w in st.session_state.get("selected_workloads", []))
        st.markdown(
            f"<div style='text-align:right;font-size:1.1rem;color:#C9D1D9;line-height:1.7;padding-top:0.3rem;'>"
            f"<b>{', '.join(_wl_names) or 'N/A'}</b> &nbsp;·&nbsp; {_wh_name} ({_wh_size})<br>"
            f"{_exec_mode_label} &nbsp;·&nbsp; {_total_q} queries × {_num_reps} reps "
            f"&nbsp;·&nbsp; Cache {_cache_label}</div>",
            unsafe_allow_html=True,
        )

    results = st.session_state.results or []
    wh_info = st.session_state.wh_info or {}

    if not results:
        st.warning("No results found.")
    else:
        df = pd.DataFrame(results)
        df["run_type"] = df["repetition"].apply(lambda x: "Cold" if x == 1 else "Warm")
        # Cast repetition to string globally so ALL charts treat it as categorical
        df["repetition"] = df["repetition"].astype(str)

        # Server-side latency (from Query History API)
        has_server = "server_latency_ms" in df.columns and df["server_latency_ms"].notna().any()
        if has_server:
            df["server_latency_seconds"] = df["server_latency_ms"] / 1000.0

        # Unique workloads in this run
        workloads_in_run = df["workload_name"].unique().tolist()

        # --- Latency source toggle ---
        if has_server:
            lat_source = st.radio(
                "Latency source",
                ["Server-side (Query History)", "Client-side (Connector)"],
                index=0, horizontal=True,
                help="Server-side = warehouse execution time from Query History API. "
                     "Client-side = round-trip time measured by the Python SQL Connector.",
            )
            use_server = lat_source.startswith("Server")
            lat_col = "server_latency_seconds" if use_server else "latency_seconds"
            lat_unit = "Server Latency" if use_server else "Client Latency"
            if not use_server:
                st.caption(
                    "\u26a0\ufe0f This app runs on a small client driver (as few as 2 vCPUs). "
                    "Server-side results reflect more accurate warehouse performance "
                    "without potential client-side resource limitations."
                )
        else:
            lat_col = "latency_seconds"
            lat_unit = "Latency"

        # --- Per-benchmark tabs ---
        if len(workloads_in_run) > 1:
            selected_wl = st.radio("Benchmark", workloads_in_run, horizontal=True)
        else:
            selected_wl = workloads_in_run[0]

        wdf = df[df["workload_name"] == selected_wl]

        # --- KPI: Wall-Clock Total, Median Query, Geomean Query ---
        import numpy as np

        _exec_mode = st.session_state.get("execution_mode", "sequential")
        _has_ts = ("query_start_time_ms" in wdf.columns and "query_end_time_ms" in wdf.columns
                   and wdf["query_start_time_ms"].gt(0).any() and wdf["query_end_time_ms"].gt(0).any())

        if _exec_mode == "parallel" and _has_ts:
            # Parallel: true wall-clock from timestamps (first start to last finish)
            _wall_per_rep = wdf.groupby("repetition").apply(
                lambda g: (g["query_end_time_ms"].max() - g["query_start_time_ms"].min()) / 1000.0
            )
            wall_clock_avg = _wall_per_rep.mean()
        elif _exec_mode == "sequential" and has_server and lat_col == "server_latency_seconds":
            # Sequential + server-side: sum of server-side latencies per rep
            # (excludes client-side gaps between queries)
            rep_totals = wdf.groupby("repetition")["server_latency_seconds"].sum()
            wall_clock_avg = rep_totals.mean()
        elif _exec_mode == "sequential" and _has_ts:
            # Sequential + client-side: timestamp-based (includes inter-query gaps)
            _wall_per_rep = wdf.groupby("repetition").apply(
                lambda g: (g["query_end_time_ms"].max() - g["query_start_time_ms"].min()) / 1000.0
            )
            wall_clock_avg = _wall_per_rep.mean()
        else:
            # Fallback: sum of selected latency column per rep
            rep_totals = wdf.groupby("repetition")[lat_col].sum()
            wall_clock_avg = rep_totals.mean()

        # Median Query Wall Time (across all reps)
        median_query = wdf[lat_col].median()

        # Geometric Mean Query Wall Time (across all reps)
        _latencies = wdf[lat_col].dropna()
        _latencies = _latencies[_latencies > 0]  # geomean needs positive values
        if len(_latencies) > 0:
            geomean_query = float(np.exp(np.log(_latencies).mean()))
        else:
            geomean_query = 0

        k1, k2, k3 = st.columns(3)
        with k1:
            st.metric("Wall-Clock Total Time Per Run", f"{wall_clock_avg:.1f}s",
                      help="Average wall-clock time from start to finish of each repetition "
                           "(parallel runs will be lower than sum of query times)")
        with k2:
            st.metric("Median Query Wall Time", f"{median_query:.3f}s",
                      help="Median individual query latency across all repetitions")
        with k3:
            st.metric("Geomean Query Wall Time", f"{geomean_query:.3f}s",
                      help="Geometric mean of individual query latencies (less sensitive to outliers)")

        st.markdown("---")

        # --- Charts ---
        _lc = lat_col

        # End-to-end time by repetition
        e2e = wdf.groupby(["run_type", "repetition"])[_lc].sum().reset_index()
        fig_e2e = px.bar(
            e2e, x="repetition", y=_lc, color="run_type",
            barmode="group",
            labels={_lc: "Total Time (s)", "repetition": "Repetition"},
            title=f"{selected_wl} \u2014 End-to-End Time by Repetition",
            color_discrete_map={"Cold": "#58A6FF", "Warm": "#FF3621"},
            category_orders={"repetition": sorted(wdf["repetition"].unique())},
        )
        fig_e2e.update_layout(**_PLOTLY_LAYOUT)
        fig_e2e.update_xaxes(type="category")
        st.plotly_chart(fig_e2e, use_container_width=True)

        # Per-query median warm latency
        warm_only = wdf[wdf["run_type"] == "Warm"]
        if not warm_only.empty:
            q_med = warm_only.groupby("query_id")[_lc].median().reset_index()
            q_med = q_med.sort_values("query_id")
            fig_bar = px.bar(
                q_med, x="query_id", y=_lc,
                labels={_lc: f"Median Warm {lat_unit} (s)", "query_id": "Query"},
                title=f"{selected_wl} \u2014 Per-Query Median Warm {lat_unit}",
                color_discrete_sequence=["#FF6A33"],
            )
            fig_bar.update_layout(**_PLOTLY_LAYOUT)
            fig_bar.update_xaxes(type="category")
            st.plotly_chart(fig_bar, use_container_width=True)

        # Cold vs Warm per query
        cold_med = wdf[wdf["run_type"] == "Cold"].groupby("query_id")[_lc].median().reset_index()
        cold_med.columns = ["query_id", "cold"]
        warm_med = warm_only.groupby("query_id")[_lc].median().reset_index() if not warm_only.empty else pd.DataFrame(columns=["query_id", _lc])
        warm_med.columns = ["query_id", "warm"] if not warm_med.empty else ["query_id", "warm"]
        cw = cold_med.merge(warm_med, on="query_id", how="outer").sort_values("query_id")
        if not cw.empty:
            fig_cw = go.Figure()
            fig_cw.add_trace(go.Bar(x=cw["query_id"].astype(str), y=cw["cold"],
                                    name="Cold", marker_color="#58A6FF"))
            if "warm" in cw.columns:
                fig_cw.add_trace(go.Bar(x=cw["query_id"].astype(str), y=cw["warm"],
                                        name="Warm", marker_color="#FF3621"))
            fig_cw.update_layout(
                barmode="group",
                title=f"{selected_wl} \u2014 Cold vs Warm {lat_unit} by Query",
                xaxis_title="Query", yaxis_title=f"Median {lat_unit} (s)",
                **_PLOTLY_LAYOUT,
            )
            fig_cw.update_xaxes(type="category")
            st.plotly_chart(fig_cw, use_container_width=True)

        # Box plot of warm latency distribution
        if not warm_only.empty:
            fig_box = px.box(
                warm_only, x="query_id", y=_lc,
                labels={_lc: f"{lat_unit} (s)", "query_id": "Query"},
                title=f"{selected_wl} \u2014 Warm {lat_unit} Distribution",
                color_discrete_sequence=["#3FB950"],
            )
            fig_box.update_layout(**_PLOTLY_LAYOUT)
            fig_box.update_xaxes(type="category")
            st.plotly_chart(fig_box, use_container_width=True)

        # --- Detailed results table ---
        with st.expander("Detailed Results Table"):
            show_cols = ["workload_name", "query_id", "repetition", "run_type",
                         "latency_seconds", "status"]
            if has_server:
                show_cols.insert(4, "server_latency_seconds")
            st.dataframe(wdf[[c for c in show_cols if c in wdf.columns]],
                         use_container_width=True, hide_index=True)

    st.markdown("---")
    if st.button("Run Another Benchmark", type="primary", use_container_width=True):
        st.session_state.step = 2
        st.session_state.results = None
        st.session_state.test_id = None
        st.session_state.validation = None
        st.session_state.selected_workloads = []
        st.rerun()
