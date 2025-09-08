import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import altair as alt    

# --- 1. 초기 설정 ---

st.set_page_config(page_title="Superstore Dashboard", layout="wide")
st.title("📊 Superstore 매출 분석 대시보드")

# --- 2. 데이터베이스 연결 ---

@st.cache_resource
def get_engine():
    """DB 연결 엔진 생성 (st.secrets 사용)"""
    try:
        # st.secrets를 사용하여 .streamlit/secrets.toml 파일의 정보 사용
        db_connection_str = (
            f"mysql+pymysql://{st.secrets['DB_USER']}:{st.secrets['DB_PASSWORD']}"
            f"@{st.secrets['DB_HOST']}:{st.secrets['DB_PORT']}/{st.secrets['DB_NAME']}"
            f"?charset={st.secrets['DB_CHARSET']}"
        )
        return create_engine(db_connection_str)
    except Exception as e:
        st.error(f"DB 연결 오류: .streamlit/secrets.toml 파일의 내용을 확인해주세요. ({e})")
        st.stop()

engine = get_engine()

# --- 3. 데이터 로딩 (스노우플레이크 스키마 JOIN) ---

@st.cache_data(show_spinner=False)
def load_data():
    """데이터베이스에서 모든 테이블을 조인하여 데이터를 불러오는 함수"""
    query = """
    SELECT
        fo.order_id,
        fo.sales,
        dp.product_name,
        dcat.category_name,
        dscat.subcategory_name,
        dc.customer_name,
        ds.segment_name,
        dcity.city_name,
        dcity.state_name,
        dr.region_name,
        dd.orderdate_when AS order_date
    FROM fact_order fo
    JOIN dim_product dp ON fo.product_id = dp.product_id
    JOIN dim_subcategory dscat ON dp.subcategory_id = dscat.subcategory_id
    JOIN dim_category dcat ON dscat.category_id = dcat.category_id
    JOIN dim_customer dc ON fo.customer_id = dc.customer_id
    JOIN dim_segment ds ON dc.segment_id = ds.segment_id
    JOIN dim_city dcity ON fo.city_id = dcity.city_id
    JOIN dim_region dr ON dcity.region_id = dr.region_id
    JOIN dim_orderdate dd ON fo.orderdate_id = dd.orderdate_id;
    """
    try:
        df = pd.read_sql(query, engine)
        df['order_date'] = pd.to_datetime(df['order_date'], dayfirst=True, errors='coerce')
        return df
    except Exception as e:
        st.error(f"데이터 로딩 오류: ETL 스크립트가 정상적으로 실행되었는지 확인해주세요. ({e})")
        return pd.DataFrame()

# --- 4. 메인 앱 실행 ---

# st.spinner를 사용해 로딩 중임을 표시
with st.spinner('데이터를 불러오는 중입니다. 잠시만 기다려주세요...'):
    df = load_data()

# 로딩이 끝나면 성공 메시지 표시
st.success('데이터 로딩 완료!', icon="✅")

if not df.empty:
    st.subheader("📈 핵심 지표")
    
    total_sales = float(df['sales'].sum())
    total_orders = int(df['order_id'].nunique())
    aov = total_sales / total_orders if total_orders else 0

    kpi1, kpi2, kpi3 = st.columns(3)
    kpi1.metric("총 매출액", f"${total_sales:,.2f}")
    kpi2.metric("총 주문 건수", f"{total_orders:,d}")
    kpi3.metric("평균 주문 금액 (AOV)", f"${aov:,.2f}")

    st.divider()

    # --- 5. 분석 및 시각화 ---
    
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📅 연도별 매출", 
        "🗺️ 지역(Region)별 매출", 
        "🏙️ 도시(City)별 매출", 
        "📦 카테고리별 매출",
        "📄 원본 데이터"
    ])

    with tab1:
        st.subheader("연도별 매출 추이")
        df['year'] = df['order_date'].dt.year
        yearly_sales = df.groupby('year')['sales'].sum().reset_index()
        st.line_chart(yearly_sales, x='year', y='sales', height=400)

    with tab2:
        st.subheader("지역(Region)별 매출")
        region_sales = df.groupby('region_name')['sales'].sum().sort_values(ascending=False).reset_index()
        st.bar_chart(region_sales, x='region_name', y='sales', height=400)

    with tab3:
        st.subheader("매출 상위 10개 도시(City)")
        top_10_cities = df.groupby('city_name')['sales'].sum().nlargest(10).sort_values(ascending=True).reset_index()
        
        # Altair를 사용해 수평 막대 차트 생성
        chart = alt.Chart(top_10_cities).mark_bar().encode(
            x=alt.X('sales:Q', title='매출액 (Sales)'),
            y=alt.Y('city_name:N', title='도시 (City)', sort='-x') # '-x'는 매출액(x축) 기준으로 막대를 정렬
        )
        
        # st.altair_chart로 차트를 화면에 표시
        st.altair_chart(chart, use_container_width=True)
    
    with tab4:
        st.subheader("카테고리별 매출")
        category_sales = df.groupby('category_name')['sales'].sum().reset_index()

        # Altair를 사용해 수직 막대 차트 생성
        chart = alt.Chart(category_sales).mark_bar().encode(
            x=alt.X('category_name:N', title='카테고리 (Category)', sort='-y'), # '-y'는 y축(매출) 기준으로 정렬
            y=alt.Y('sales:Q', title='매출액 (Sales)')
        )

        # st.altair_chart로 차트를 화면에 표시
        st.altair_chart(chart, use_container_width=True)

    with tab5:
        st.subheader("원본 데이터")
        st.dataframe(df)
        st.download_button(
            "CSV 다운로드",
            data=df.to_csv(index=False).encode('utf-8-sig'),
            file_name="superstore_data.csv",
            mime="text/csv"
        )
else:
    st.warning("데이터를 불러오지 못했습니다. DB 연결 또는 ETL 스크립트 실행 상태를 확인해주세요.")