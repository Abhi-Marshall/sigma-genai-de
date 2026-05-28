import json
import os
import sys
from datetime import datetime

import duckdb
import pandas as pd
import streamlit as st
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "shared"))

from bedrock_helper import call_nova_lite, call_nova_pro
from sample_data import MIGRATION_V1_TO_V2, MIGRATION_V2_TO_V3, SCHEMA_V1, SCHEMA_V2, SCHEMA_V3

# --- App Directories and Constants ---
APP_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(APP_DIR, "..", "shared", "sigma_platform.duckdb")
VERDICT_PATH = os.path.join(APP_DIR, "verdict.json")

PROPOSED_SOLUTION_SCHEMA = """
CREATE TABLE IF NOT EXISTS txn_v3_proposed (
    transaction_id   VARCHAR PRIMARY KEY,
    amount           DOUBLE NOT NULL,
    status           VARCHAR NOT NULL,
    merchant_id      VARCHAR,
    user_id          VARCHAR,
    transaction_date DATE
);

CREATE TABLE IF NOT EXISTS txn_payment_methods (
    transaction_id VARCHAR PRIMARY KEY,
    payment_method VARCHAR,
    FOREIGN KEY (transaction_id) REFERENCES txn_v3_proposed(transaction_id)
);
"""

PROPOSED_SOLUTION_SQL = """
CREATE TABLE txn_v3_proposed AS
SELECT
    transaction_id,
    amount,
    status,
    merchant_id,
    customer_id AS user_id,
    transaction_date
FROM txn_v2;

CREATE TABLE txn_payment_methods AS
SELECT
    transaction_id,
    payment_method
FROM txn_v2;
"""

HISTORIAN_FALLBACK = """- **v1**: Early transaction ledger focused on payment facts: transaction id, amount, status, merchant, and date.
- **v1 to v2**: Customer analytics and payment-channel analysis arrived, so customer_id and payment_method were added.
- **v2 to v3**: Identity language moved from customer_id to user_id, probably for product/platform consistency.
- **Hidden risk**: v3 removes payment_method, which erases the ability to segment UPI, card, and debit-card behavior."""

AUDITOR_FALLBACK = """- **v1 to v2**: LOW risk. It adds nullable columns and preserves existing transaction facts, though backfill quality should be checked.
- **v2 to v3**: MEDIUM risk on paper because customer_id is renamed to user_id and payment_method is dropped.
- **Audit concern**: Dropping payment_method can break channel-based revenue, failure-rate, and UPI adoption reports.
- **Corrected judgment after evidence**: the drop is CRITICAL because downstream reports can still run with wrong zero/null results."""


# --- Page Configurations ---
st.set_page_config(
    page_title="Schema Archaeologist | AI Ops",
    page_icon="🕵️‍♂️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state for active analytics execution
if "ai_analysis_run" not in st.session_state:
    st.session_state.ai_analysis_run = False


# --- Database Helper Functions ---
@st.cache_resource
def get_connection():
    return duckdb.connect(DB_PATH, read_only=True)


@st.cache_data
def table_df(query: str) -> pd.DataFrame:
    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        return con.execute(query).fetchdf()
    finally:
        con.close()


def safe_ai_call(fn, fallback: str, system: str, user: str, max_tokens: int = 1200) -> str:
    try:
        return fn(system, user, max_tokens=max_tokens)
    except (NoCredentialsError, BotoCoreError, ClientError, Exception) as exc:
        return f"{fallback}\n\n_Local fallback used because Bedrock was unavailable: {type(exc).__name__}._"


def schema_profile(table_name: str) -> pd.DataFrame:
    return table_df(f"DESCRIBE {table_name}")


def row_count(table_name: str) -> int:
    try:
        return int(table_df(f"SELECT COUNT(*) AS n FROM {table_name}")["n"].iloc[0])
    except Exception:
        return 0


def column_names(table_name: str) -> list[str]:
    return schema_profile(table_name)["column_name"].tolist()


def schema_diff(left: str, right: str) -> pd.DataFrame:
    left_cols = set(column_names(left))
    right_cols = set(column_names(right))
    rows = []
    for col in sorted(left_cols | right_cols):
        if col in left_cols and col in right_cols:
            status = "🔄 kept"
        elif col in right_cols:
            status = "➕ added"
        else:
            status = "❌ removed"
        rows.append({"column": col, "change": status, left: col in left_cols, right: col in right_cols})
    return pd.DataFrame(rows)


def historian_prompt() -> str:
    return f"""Compare these three transaction schemas and reconstruct the likely business story.
Schema v1: {SCHEMA_V1}
Schema v2: {SCHEMA_V2}
Schema v3: {SCHEMA_V3}
Write concise bullets for v1, v1 to v2, and v2 to v3. Include business motivation and operational risk."""


def auditor_prompt() -> str:
    return f"""Review these migration steps and assign risk LOW, MEDIUM, HIGH, or CRITICAL.
Give specific reasons and what data/report could break.
Migration v1 to v2: {MIGRATION_V1_TO_V2}
Migration v2 to v3: {MIGRATION_V2_TO_V3}"""


def build_verdict() -> dict:
    upi_v2 = table_df(
        "SELECT payment_method, COUNT(*) AS transactions, ROUND(SUM(amount), 2) AS revenue FROM txn_v2 WHERE payment_method = 'UPI' GROUP BY payment_method"
    )
    silent_break = table_df(
        "SELECT COUNT(*) AS transactions, ROUND(SUM(amount), 2) AS revenue FROM (SELECT *, CAST(NULL AS VARCHAR) AS payment_method FROM txn_v3) WHERE payment_method = 'UPI'"
    )
    return {
        "team": "team7_schema_archaeologist",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "verdict": "DO NOT MIGRATE AS WRITTEN",
        "dangerous_step": "v2_to_v3 drops payment_method",
        "risk_rating": "CRITICAL",
        "why_it_is_silent": "A compatibility view can add payment_method as NULL, allowing downstream UPI filters to run and return zero rows instead of failing.",
        "proof": {
            "v2_upi_transactions": int(upi_v2["transactions"].sum()) if not upi_v2.empty else 0,
            "v2_upi_revenue": float(upi_v2["revenue"].sum()) if not upi_v2.empty else 0.0,
            "v3_compat_upi_transactions": int(silent_break["transactions"].iloc[0]),
            "v3_compat_upi_revenue": 0.0 if pd.isna(silent_break["revenue"].iloc[0]) else float(silent_break["revenue"].iloc[0]),
        },
        "downstream_query_that_breaks": "SELECT COUNT(*), SUM(amount) FROM txn_current WHERE payment_method = 'UPI';",
        "safer_migration": [
            "Create txn_v3_proposed with transaction_id as the primary key, and move payment_method into txn_payment_methods keyed by transaction_id.",
            "Keep payment_method in v3 until every downstream report is migrated.",
            "If a rename is required, create a compatibility view that maps customer_id to user_id but preserves payment_method.",
            "Add CI checks comparing channel-level counts and revenue between v2 and v3 before cutover.",
        ],
        "what_ai_got_wrong": "The AI auditor may label the dropped column as medium schema cleanup, but business evidence shows it destroys payment-channel reporting.",
    }


def save_verdict(verdict: dict) -> None:
    with open(VERDICT_PATH, "w", encoding="utf-8") as f:
        json.dump(verdict, f, indent=2)


# --- Execution Safeguard ---
if not os.path.exists(DB_PATH):
    st.error("🚨 Shared DuckDB database not found. Run `python day9/shared/setup_duckdb.py` first.")
    st.stop()

conn = get_connection()
verdict = build_verdict()
save_verdict(verdict)


# --- Sidebar UI Configuration ---
with st.sidebar:
    st.image("https://img.icons8.com/wired/128/000000/archaeology.png", width=80)
    st.title("Platform Control Panel")
    st.caption("Sigma DataTech AI Ops Platform • Day 9")
    
    st.divider()
    st.markdown("### 📋 Run Configurations")
    
    # Active Interactive Element: Control Execution via Button
    if st.button("✨ Execute AI Risk Analysis", type="primary", use_container_width=True):
        st.session_state.ai_analysis_run = True
        st.toast("AI Engines engaged successfully!", icon="🚀")
        
    st.markdown("💡 *Triggering explicitly prevents unnecessary continuous API calling on page state changes.*")
    
    st.divider()
    st.markdown("### 💾 Export Reports")
    # Interactive Element: Dynamic Download Action
    st.download_button(
        label="📥 Download Audit Verdict (JSON)",
        data=json.dumps(verdict, indent=2),
        file_name="verdict_audit_report.json",
        mime="application/json",
        use_container_width=True
    )
    st.success(f"Config auto-saved to system paths.")


# --- Main Dashboard Title ---
st.title("🕵️‍♂️ Schema Archaeologist")
st.markdown("Analyze database layer drift, uncover structural ancestry, and dynamically catch silent reporting failures before staging deployment.")

# --- Real-Time Performance & Metric Grid ---
st.divider()
m_col1, m_col2, m_col3, m_col4 = st.columns(4)
with m_col1:
    st.metric(label="🔢 Tracked Lineages", value="3 Schema Iterations", delta="Stable")
with m_col2:
    st.metric(label="📊 Active Log Footprint (v2)", value=f"{row_count('txn_v2'):,}")
with m_col3:
    st.metric(label="💸 Volume At-Risk (UPI Channel)", value=f"${verdict['proof']['v2_upi_revenue']:,.2f}", delta="-100% Impact", delta_color="inverse")
with m_col4:
    # Highlighting target status
    st.error(f"🚨 Audit Verdict: {verdict['verdict']}")


# --- Primary Tabs Architecture ---
st.subheader("📁 Database Artifact Inspection Matrix")
schema_tabs = st.tabs([
    "📂 Version 1 Lineage", 
    "📂 Version 2 Lineage", 
    "📂 Version 3 Lineage", 
    "📈 Structural Diffs", 
    "🛠️ Proposed Clean Solution"
])

with schema_tabs[0]:
    col_l, col_r = st.columns([1, 1])
    with col_l:
        st.markdown("**Original Base Syntax Definitions**")
        st.code(SCHEMA_V1, language="sql")
    with col_r:
        st.markdown("**Field Profile Spec Descriptions**")
        st.dataframe(schema_profile("txn_v1"), use_container_width=True, hide_index=True)

with schema_tabs[1]:
    col_l, col_r = st.columns([1, 1])
    with col_l:
        st.markdown("**Analytical Evolution Definition**")
        st.code(SCHEMA_V2, language="sql")
    with col_r:
        st.markdown("**Field Profile Spec Descriptions**")
        st.dataframe(schema_profile("txn_v2"), use_container_width=True, hide_index=True)

with schema_tabs[2]:
    col_l, col_r = st.columns([1, 1])
    with col_l:
        st.markdown("**Target Staging Blueprint Layout**")
        st.code(SCHEMA_V3, language="sql")
    with col_r:
        st.markdown("**Field Profile Spec Descriptions**")
        st.dataframe(schema_profile("txn_v3"), use_container_width=True, hide_index=True)

with schema_tabs[3]:
    diff_c1, diff_c2 = st.columns(2)
    with diff_c1:
        st.info("### Shift Manifestation: v1 ➔ v2")
        st.dataframe(schema_diff("txn_v1", "txn_v2"), use_container_width=True, hide_index=True)
    with diff_c2:
        st.warning("### Shift Manifestation: v2 ➔ v3")
        st.dataframe(schema_diff("txn_v2", "txn_v3"), use_container_width=True, hide_index=True)

with schema_tabs[4]:
    st.markdown("### ✅ Optimized Migration Framework Design")
    st.markdown("This solution retains the stable unique constraint field tracking definitions in standard format while cleanly normalizing data access parameters across secondary processing nodes without losing downstream query support structures.")
    
    c_s1, c_s2 = st.columns([1, 1])
    with c_s1:
        st.markdown("**Target Database State Build Definitions:**")
        st.code(PROPOSED_SOLUTION_SCHEMA, language="sql")
    with c_s2:
        st.markdown("**Safe Population Mapping Routine Execution:**")
        st.code(PROPOSED_SOLUTION_SQL, language="sql")
        
    proposed_cols = pd.DataFrame([
        {"table_name": "txn_v3_proposed", "column_name": "transaction_id", "column_type": "VARCHAR", "purpose": "Primary key integration"},
        {"table_name": "txn_v3_proposed", "column_name": "amount", "column_type": "DOUBLE", "purpose": "Transaction balance monitoring"},
        {"table_name": "txn_v3_proposed", "column_name": "status", "column_type": "VARCHAR", "purpose": "Payment confirmation updates"},
        {"table_name": "txn_v3_proposed", "column_name": "merchant_id", "column_type": "VARCHAR", "purpose": "B2B relation metrics indexing"},
        {"table_name": "txn_v3_proposed", "column_name": "user_id", "column_type": "VARCHAR", "purpose": "Normalized uniform identifier tracking"},
        {"table_name": "txn_v3_proposed", "column_name": "transaction_date", "column_type": "DATE", "purpose": "Time window processing tracking"},
        {"table_name": "txn_payment_methods", "column_name": "transaction_id", "column_type": "VARCHAR", "purpose": "Relational structural link tracking"},
        {"table_name": "txn_payment_methods", "column_name": "payment_method", "column_type": "VARCHAR", "purpose": "Required legacy channel preservation layer"}
    ])
    st.dataframe(proposed_cols, use_container_width=True, hide_index=True)


# --- Interactive SQL Engine Sandbox Component ---
st.divider()
st.subheader("🕵️‍♂️ Operational Query Laboratory Sandbox")
st.markdown("Interactively execute metrics queries across different lineage tables to trace the exact breaking patterns.")

# Preset catalog selection
q1_sql = "SELECT payment_method, COUNT(*) AS transactions, ROUND(SUM(amount), 2) AS revenue FROM txn_v2 WHERE payment_method = 'UPI' GROUP BY payment_method;"
q2_sql = "SELECT COUNT(*) AS transactions, ROUND(SUM(amount), 2) AS revenue FROM (SELECT *, CAST(NULL AS VARCHAR) AS payment_method FROM txn_v3) WHERE payment_method = 'UPI';"
safe_view_sql = """SELECT v3.transaction_id, v3.amount, v3.status, v3.user_id, pm.payment_method 
FROM txn_v2 v3 
LEFT JOIN (SELECT DISTINCT transaction_id, payment_method FROM txn_v2) pm 
ON v3.transaction_id = pm.transaction_id
WHERE pm.payment_method = 'UPI' LIMIT 5;"""

query_selection = st.selectbox(
    "Choose or swap preset SQL statements to test execution profile:",
    options=[
        "🔍 Report Option A: Before Migration (Healthy V2 UPI Query Metrics Profile)",
        "⚠️ Report Option B: Post Migration (Unsafe V3 Structural Fallback Execution Profile)",
        "🛡️ Report Option C: Secure View Approach Execution Model Preview"
    ]
)

# Populate workspace string area block context
active_query_string = q1_sql
if "Option B" in query_selection:
    active_query_string = q2_sql
elif "Option C" in query_selection:
    active_query_string = safe_view_sql

sql_input_area = st.text_area("Live Dynamic SQL Script Panel Editor Workspace", value=active_query_string, height=115)

if st.button("🚀 Execute Engine Transaction Query", use_container_width=True):
    with st.status("Accessing runtime engine connection registers...", expanded=True) as status:
        try:
            st.write("Executing isolation compilation processing layers...")
            executed_result_set = table_df(sql_input_area)
            status.update(label="Query successfully rendered!", state="complete", expanded=False)
            st.toast("Execution success!", icon="✅")
            st.markdown("**Output Result Matrix Block View:**")
            st.dataframe(executed_result_set, use_container_width=True, hide_index=True)
        except Exception as query_error_exception:
            status.update(label="Compilation execution failed!", state="error", expanded=True)
            st.error(f"Detailed engine stack execution tracing failure: {query_error_exception}")


# --- AI Engine Core Evaluation Logic Panel Section ---
st.divider()
st.subheader("🤖 Automated Large Language Model Audits")

if not st.session_state.ai_analysis_run:
    st.info("💡 Direct analytical evaluation context logs are paused. Click the **'Execute AI Risk Analysis'** option in the controller sidebar layout tab to evaluate models live.")
else:
    with st.status("Processing Deep Analysis Engine Operations...", expanded=True) as ai_status:
        st.write("Extracting parameters for Agent 1 (Historian)...")
        historian_text = safe_ai_call(
            call_nova_pro, HISTORIAN_FALLBACK,
            "You are a senior data platform historian explaining schema evolution to business stakeholders.",
            historian_prompt()
        )
        
        st.write("Extracting parameters for Agent 2 (Auditor Risk Evaluator Matrix)...")
        auditor_text = safe_ai_call(
            call_nova_lite, AUDITOR_FALLBACK,
            "You are a cautious data migration risk auditor.",
            auditor_prompt()
        )
        ai_status.update(label="AI Generation Modules Finished Output Evaluation Pass", state="complete", expanded=False)
        
    ai_col1, ai_col2 = st.columns(2)
    with ai_col1:
        with st.chat_message("assistant", avatar="📜"):
            st.markdown("### Round 1: AI Historian Analytical Narrative")
            st.markdown(historian_text)
            
    with ai_col2:
        with st.chat_message("assistant", avatar="🛡️"):
            st.markdown("### Round 2: AI Architectural Risk Auditor Framework Evaluation")
            st.markdown(auditor_text)

    # Risk Warning Callout Banner Details
    st.warning(f"**🚨 Strategic Forensic Discovery Conclusion Notification:** {verdict['dangerous_step']}. This introduces structural silent payload failure vulnerabilities across processing chains.")

    st.subheader("🔍 What the Base AI Context Engine Miscalculated During Raw Review Passes")
    st.info(verdict["what_ai_got_wrong"])


# --- Safe Structural Deployment Blueprint Guide Layer ---
st.divider()
st.subheader("🛡️ Implementation Roadmap Guide Blueprint Pattern Requirements")
st.markdown("Implement this orchestration architecture sequence layer framework to deploy target state safely without structural system side-effects:")

safe_solution_deployment_sql_script = """
CREATE VIEW txn_v3_compat AS
SELECT
    v3.transaction_id,
    v3.amount,
    v3.status,
    v3.merchant_id,
    v3.user_id,
    v3.transaction_date,
    pm.payment_method
FROM txn_v3_proposed v3
LEFT JOIN txn_payment_methods pm
    ON v3.transaction_id = pm.transaction_id;

-- Validation Gate Testing Hook Routine Verification Protocol:
SELECT payment_method, COUNT(*) AS txns, SUM(amount) AS revenue
FROM txn_v3_compat
GROUP BY payment_method;
"""
st.code(safe_solution_deployment_sql_script, language="sql")

st.markdown("""
* **Encapsulation Extraction Separation Rules**: Isolate the dynamic variable metrics collection criteria (`payment_method`) onto distinct database tables tied back directly via standard unique foreign keys.
* **Backwards-Compatibility Access Layer Implementations**: Always present decoupled backward-compatible view translation components (`txn_v3_compat`) while system optimization modifications run.
* **Gate Operations Assertion Verification Automation**: Inject functional continuous integration tracking assertions evaluating balance aggregates group configurations before severing baseline legacy infrastructure interfaces.
""")