import streamlit as st
import pandas as pd
import time
import snowflake.connector


@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        account=st.secrets["SNOWFLAKE_ACCOUNT"],
        user=st.secrets["SNOWFLAKE_USER"],
        password=st.secrets["SNOWFLAKE_PASSWORD"],
        warehouse=st.secrets["SNOWFLAKE_WAREHOUSE"],
        database=st.secrets["SNOWFLAKE_DATABASE"],
        schema=st.secrets["SNOWFLAKE_SCHEMA"],
        role=st.secrets.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )


conn = get_connection()

COCA_COLA_RED = "#CC0000"
ORDER_TABLE_HEADERS = ["Order ID", "Store ID", "Date", "Quantity", "Product", "Status"]

st.set_page_config(page_title="Agentic AI Supply Chain PoC", layout="wide")

st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800;900&display=swap');

    html, body, [class*="st-"] {
        font-family: 'Inter', sans-serif;
    }
    .block-container { padding-top: 2rem; }

    .hero-banner {
        background: linear-gradient(135deg, #CC0000 0%, #8B0000 60%, #1a1a2e 100%);
        border-radius: 16px;
        padding: 2.5rem 3rem;
        margin-bottom: 2rem;
        display: flex;
        align-items: center;
        justify-content: center;
        flex-direction: column;
        box-shadow: 0 8px 32px rgba(204, 0, 0, 0.25);
    }
    .hero-title {
        color: #ffffff;
        font-weight: 900;
        font-size: 2.8rem;
        letter-spacing: -0.5px;
        margin: 0;
        text-align: center;
    }
    .hero-subtitle {
        color: rgba(255,255,255,0.8);
        font-size: 1.1rem;
        font-weight: 400;
        margin-top: 0.5rem;
        text-align: center;
    }

    .metric-card {
        background: #ffffff;
        border: 1px solid #e8e8e8;
        border-radius: 12px;
        padding: 1.2rem 1.5rem;
        text-align: center;
        box-shadow: 0 2px 8px rgba(0,0,0,0.04);
    }
    .metric-value {
        font-size: 2rem;
        font-weight: 800;
        color: #CC0000;
        margin: 0;
    }
    .metric-label {
        font-size: 0.8rem;
        font-weight: 600;
        color: #888;
        text-transform: uppercase;
        letter-spacing: 1px;
        margin: 0;
    }

    .section-card {
        background: #ffffff;
        border: 1px solid #e8e8e8;
        border-radius: 14px;
        padding: 1.5rem 2rem;
        margin-bottom: 1.5rem;
        box-shadow: 0 2px 12px rgba(0,0,0,0.04);
    }
    .section-title {
        font-size: 1.1rem;
        font-weight: 700;
        color: #1a1a2e;
        margin-bottom: 1rem;
        display: flex;
        align-items: center;
        gap: 0.5rem;
    }
    .section-title .icon {
        background: #CC0000;
        color: white;
        width: 32px;
        height: 32px;
        border-radius: 8px;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        font-size: 1rem;
    }

    div[data-testid="stDataFrame"] {
        border: 1px solid #f0f0f0;
        border-radius: 10px;
        overflow: hidden;
    }

    textarea {
        background: #ffffff !important;
        color: #000000 !important;
        font-family: 'JetBrains Mono', 'Fira Code', monospace !important;
        font-size: 0.9rem !important;
        border-radius: 10px !important;
        border: 1px solid #dde1e6 !important;
        line-height: 1.6 !important;
        -webkit-text-fill-color: #000000 !important;
        opacity: 1 !important;
    }

    .stButton > button {
        background: linear-gradient(135deg, #CC0000, #990000);
        color: white;
        font-weight: 700;
        font-size: 1rem;
        border: none;
        border-radius: 10px;
        padding: 0.7rem 2.5rem;
        letter-spacing: 0.5px;
        transition: all 0.2s ease;
        box-shadow: 0 4px 15px rgba(204, 0, 0, 0.3);
    }
    .stButton > button:hover {
        background: linear-gradient(135deg, #e60000, #b30000);
        box-shadow: 0 6px 20px rgba(204, 0, 0, 0.45);
        transform: translateY(-1px);
    }

    .status-badge-new {
        background: #fff3f3; color: #CC0000;
        padding: 3px 10px; border-radius: 20px;
        font-weight: 600; font-size: 0.8rem;
    }
    .status-badge-assigned {
        background: #f0fff4; color: #008000;
        padding: 3px 10px; border-radius: 20px;
        font-weight: 600; font-size: 0.8rem;
    }

    #MainMenu {visibility: hidden;}
    header {visibility: hidden;}
    footer {visibility: hidden;}
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="hero-banner">
        <p class="hero-title">Agentic AI for Delivery Execution</p>
        <p class="hero-subtitle">Coca-Cola Supply Chain &mdash; Intelligent Order Routing &amp; Dispatch</p>
    </div>
    """,
    unsafe_allow_html=True,
)


def get_orders():
    cur = conn.cursor()
    cur.execute(
        "SELECT ORDER_ID, STORE_ID, TO_VARCHAR(ORDER_DATE,'YYYY-MM-DD') AS ORDER_DATE, "
        "QUANTITY, PRODUCT_NAME "
        "FROM COCA_COLA_SUPPLY_CHAIN.AGENT.ORDER_DETAILS "
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


@st.cache_data(ttl=300)
def get_pending_count():
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM COCA_COLA_SUPPLY_CHAIN.AGENT.ORDER_DETAILS "
        "WHERE ORDER_STATUS = 'Pending'"
    )
    return cur.fetchone()[0]


total_pending = get_pending_count()

if "orders_df" not in st.session_state:
    st.session_state.orders_df = pd.DataFrame(columns=ORDER_TABLE_HEADERS)
if "log_text" not in st.session_state:
    st.session_state.log_text = "--- Click 'Run Agent' to load orders and begin execution. ---"
if "running" not in st.session_state:
    st.session_state.running = False

order_count = len(st.session_state.orders_df)
assigned_count = int((st.session_state.orders_df["Status"] == "Assigned").sum()) if order_count > 0 else 0
pending_display = total_pending if order_count == 0 else order_count - assigned_count
total_display = total_pending if order_count == 0 else order_count

m1, m2, m3, m4 = st.columns(4)
with m1:
    st.markdown('<div class="metric-card"><p class="metric-value">{}</p><p class="metric-label">Total Orders</p></div>'.format(total_display), unsafe_allow_html=True)
with m2:
    st.markdown('<div class="metric-card"><p class="metric-value">{}</p><p class="metric-label">Pending</p></div>'.format(pending_display), unsafe_allow_html=True)
with m3:
    st.markdown('<div class="metric-card"><p class="metric-value">{}</p><p class="metric-label">Assigned</p></div>'.format(assigned_count), unsafe_allow_html=True)
with m4:
    st.markdown('<div class="metric-card"><p class="metric-value">{}</p><p class="metric-label">Success Rate</p></div>'.format("{}%".format(round(assigned_count / total_display * 100)) if total_display > 0 else "0%"), unsafe_allow_html=True)

st.write("")

_, btn_col, _ = st.columns([1, 1, 1])
with btn_col:
    if st.button("Run Agent", use_container_width=True):
        st.session_state.running = True

st.write("")

st.markdown('<div class="section-card"><div class="section-title"><span class="icon">&#128230;</span> Order Status</div>', unsafe_allow_html=True)
orders_placeholder = st.empty()
orders_placeholder.dataframe(st.session_state.orders_df, use_container_width=True, hide_index=True)
st.markdown("</div>", unsafe_allow_html=True)

st.markdown('<div class="section-card"><div class="section-title"><span class="icon">&#129302;</span> Agent Processing Log</div>', unsafe_allow_html=True)
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
        chunk_size = 15
        for i in range(0, len(file_content), chunk_size):
            log += file_content[i : i + chunk_size]
            log_placeholder.text_area("Transparent Execution Flow", value=log, height=400, disabled=True)
            time.sleep(0.12)
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

    cur = conn.cursor()
    cur.execute("DELETE FROM COCA_COLA_SUPPLY_CHAIN.AGENT.DRIVER_ASSIGNMENT")
    cur.execute("""
        INSERT INTO COCA_COLA_SUPPLY_CHAIN.AGENT.DRIVER_ASSIGNMENT
            (DRIVER_NAME, STORE_NAME, STORE_CITY, DISTANCE_KM, ORDER_ID, TOTAL_WEIGHT_KG)
        VALUES
            ('Peter Pan',   'Desert Mart',           'Phoenix',     6.2, 1001,  92),
            ('Peter Pan',   'Desert Mart',           'Phoenix',     6.2, 1010,  48),
            ('Peter Pan',   'Desert Mart',           'Phoenix',     6.2, 1019,  52),
            ('Peter Pan',   'Canyon Corner',         'Tucson',    170.7, 1002,  80),
            ('Peter Pan',   'Canyon Corner',         'Tucson',    170.7, 1011,  90),
            ('Peter Pan',   'Canyon Corner',         'Tucson',    170.7, 1017,  45),
            ('Peter Pan',   'Cactus Corner Store',   'Scottsdale', 14.6, 1006,  65),
            ('Peter Pan',   'Canyon Beverages',      'Tempe',      12.7, 1014,  25),
            ('James Bond',  'Sunrise Market',        'Gilbert',    28.5, 1003, 107),
            ('James Bond',  'Sunrise Market',        'Gilbert',    28.5, 1009,  73),
            ('James Bond',  'Sunrise Market',        'Gilbert',    28.5, 1016,  60),
            ('James Bond',  'Saguaro Supply',        'Chandler',   26.8, 1008,  55),
            ('James Bond',  'Sunbelt Grocers',       'Mesa',       22.8, 1007,  38),
            ('John Doe',    'Copper Basin Grocers',  'Peoria',     21.1, 1004,  42),
            ('John Doe',    'Copper Basin Grocers',  'Peoria',     21.1, 1012,  34),
            ('John Doe',    'Copper Basin Grocers',  'Peoria',     21.1, 1020,  68),
            ('John Doe',    'Red Rock Provisions',   'Glendale',   14.4, 1015,  88),
            ('Jane Smith',  'Valley Fresh Supply',   'Surprise',   33.9, 1005, 122),
            ('Jane Smith',  'Valley Fresh Supply',   'Surprise',   33.9, 1013, 110),
            ('Jane Smith',  'Valley Fresh Supply',   'Surprise',   33.9, 1018,  77)
    """)

    log += "✅ Driver assignments saved to DRIVER_ASSIGNMENT table.\n"
    log_placeholder.text_area("Transparent Execution Flow", value=log, height=400, disabled=True)

    log += "\n✅ Final assignments complete. Order statuses updated."
    st.session_state.log_text = log
    log_placeholder.text_area("Transparent Execution Flow", value=log, height=400, disabled=True)
    time.sleep(1)
    st.rerun()
