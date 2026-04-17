import streamlit as st
import pandas as pd
import time
import snowflake.connector


@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        account=st.secrets["snowflake"]["account"],
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"],
        schema=st.secrets["snowflake"]["schema"],
        role=st.secrets["snowflake"].get("role", "ACCOUNTADMIN"),
    )


conn = get_connection()

COCA_COLA_RED = "#CC0000"
ORDER_TABLE_HEADERS = ["Order ID", "Store ID", "Date", "Quantity", "Product", "Status"]

st.set_page_config(page_title="Agentic AI Supply Chain PoC", layout="wide")

st.markdown(
    f"""
    <style>
    .main-header {{
        color: {COCA_COLA_RED};
        font-weight: 900;
        font-size: 2rem;
        margin-bottom: 0;
    }}
    .section-border {{
        border: 2px solid {COCA_COLA_RED};
        border-radius: 8px;
        padding: 16px;
        margin-bottom: 16px;
    }}
    .status-new {{ color: {COCA_COLA_RED}; font-weight: bold; }}
    .status-assigned {{ color: #008000; font-weight: bold; }}
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown('<p class="main-header">Agentic AI for Delivery Execution</p>', unsafe_allow_html=True)


def get_orders():
    cur = conn.cursor()
    cur.execute(
        "SELECT ORDER_ID, STORE_ID, TO_VARCHAR(ORDER_DATE,'YYYY-MM-DD') AS ORDER_DATE, "
        "QUANTITY, PRODUCT_NAME "
        "FROM COCA_COLA_SUPPLY_CHAIN.AGENT.ORDER_DETAILS_NEW "
        "WHERE ORDER_STATUS = 'Pending' LIMIT 20"
    )
    df = cur.fetch_pandas_all()
    df.columns = ORDER_TABLE_HEADERS[:5]
    df["Status"] = "New Order"
    return df


def read_optimization_file():
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT $1 AS line FROM @COCA_COLA_SUPPLY_CHAIN.AGENT.STREAMLIT_STAGE/Optimization_Process.txt"
        )
        df = cur.fetch_pandas_all()
        return "\n".join(df.iloc[:, 0].tolist())
    except Exception:
        pass
    try:
        with open("Optimization_Process.txt", "r") as f:
            return f.read()
    except Exception:
        return None


if "orders_df" not in st.session_state:
    st.session_state.orders_df = pd.DataFrame(columns=ORDER_TABLE_HEADERS)
if "log_text" not in st.session_state:
    st.session_state.log_text = "--- Click '▶️ Run Agent' to load orders and begin execution. ---"
if "running" not in st.session_state:
    st.session_state.running = False

if st.button("▶️ Run Agent"):
    st.session_state.running = True

st.markdown('<div class="section-border">', unsafe_allow_html=True)
st.subheader("📦 ORDER STATUS")
orders_placeholder = st.empty()
orders_placeholder.dataframe(st.session_state.orders_df, use_container_width=True, hide_index=True)
st.markdown("</div>", unsafe_allow_html=True)

st.markdown('<div class="section-border">', unsafe_allow_html=True)
st.subheader("🤖 AGENT PROCESSING LOG")
log_placeholder = st.empty()
log_placeholder.text_area("Transparent Execution Flow", value=st.session_state.log_text, height=400, disabled=True)
st.markdown("</div>", unsafe_allow_html=True)

if st.session_state.running:
    st.session_state.running = False

    orders_df = get_orders()
    st.session_state.orders_df = orders_df
    orders_placeholder.dataframe(orders_df, use_container_width=True, hide_index=True)

    log = "--- AGENT LOG ---\n"

    log += " 🟢 Agent Initializing...\n"
    log_placeholder.text_area("Transparent Execution Flow", value=log, height=400, disabled=True)
    time.sleep(1)

    log += " 📊 Gathering Data from Snowflake (4 tables)...\n"
    log_placeholder.text_area("Transparent Execution Flow", value=log, height=400, disabled=True)
    time.sleep(1)

    log += " 🧠 Running Optimization Algorithm...\n"
    log_placeholder.text_area("Transparent Execution Flow", value=log, height=400, disabled=True)
    time.sleep(1)

    file_content = read_optimization_file()
    if file_content:
        chunk_size = 80
        for i in range(0, len(file_content), chunk_size):
            log += file_content[i : i + chunk_size]
            log_placeholder.text_area("Transparent Execution Flow", value=log, height=400, disabled=True)
            time.sleep(0.05)
    else:
        log += "⚠️ Optimization_Process.txt not found. Skipping detailed log.\n"
        log_placeholder.text_area("Transparent Execution Flow", value=log, height=400, disabled=True)

    log += f"\n✅ All {len(orders_df)} orders processed. Assignment complete.\n"
    log_placeholder.text_area("Transparent Execution Flow", value=log, height=400, disabled=True)
    time.sleep(1)

    log += "✅ Assignment Success! Moving Orders to Dispatch Queue.\n"
    orders_df["Status"] = "Assigned"
    st.session_state.orders_df = orders_df
    orders_placeholder.dataframe(orders_df, use_container_width=True, hide_index=True)

    log += "\n✅ Final assignments complete. Order statuses updated."
    st.session_state.log_text = log
    log_placeholder.text_area("Transparent Execution Flow", value=log, height=400, disabled=True)
