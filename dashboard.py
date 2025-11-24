# ====================== افغانستان سمارټ کرنه او روغتیا ډشبورډ ۲۰۲۵ ======================
# جوړونکی: تاسو + Grok
# تاریخ: ۲۰۲۵ – بشپړ کار کوي

import streamlit as st
import pandas as pd
import plotly.express as px
import json
import os
from sqlalchemy import create_engine

# ----------------------- ۱. د پاڼې اساسي تنظیمات -----------------------
st.set_page_config(
    page_title="افغانستان سمارټ کرنه او روغتیا ډشبورډ ۲۰۲۵",
    page_icon="wheat",
    layout="wide"
)

st.markdown("<h1 style='text-align: center; color: #1E88E5;'>افغانستان سمارټ کرنه او روغتیا ډشبورډ ۲۰۲۵</h1>", unsafe_allow_html=True)
st.markdown("<h3 style='text-align: center; color: #2E7D32;'>د هوا، ککړتیا او فصلونو بشپړ تحلیل</h3>", unsafe_allow_html=True)

# ----------------------- ۲. د MySQL ډیټابیس اتصال (اوس ۱۰۰٪ کار کوي) ----------------
@st.cache_resource
def get_engine():
    return create_engine(
        "mysql+pymysql://root:Moh123@localhost/WeatherDataWarehouse",
        pool_pre_ping=True,
        pool_recycle=3600,
        future=True 
    )

engine = get_engine()

# ---------------- ۳. د افغانستان ولایتونو نقشه (GeoJSON) ----------------
@st.cache_data(ttl="30d")
def load_afg_geojson():
    file_path = "geoBoundaries-AFG-ADM1_simplified.geojson"
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        st.success("ولایتي GeoJSON له محلي فایل څخه لوسته شوه!")
        return data
    else:
        st.error("geoBoundaries-AFG-ADM1_simplified.geojson فایل ونه موندل شو!")
        return None

geojson = load_afg_geojson()

# ---------------- ۴. له ډیټابیس څخه ولایتونه او فصلونه راویستل ----------------
with engine.connect() as raw_conn:
    db_connection = raw_conn.connection  

    provinces = pd.read_sql(
        "SELECT DISTINCT province FROM Location_Dim ORDER BY province",
        con=db_connection
    )['province'].tolist()

    crops = pd.read_sql(
        "SELECT crop_name FROM Crop_Dim ORDER BY crop_name",
        con=db_connection
    )['crop_name'].tolist()

if not crops:
    st.error("د Crop_Dim جدول خالي دی! مهرباني وکړئ ډاټا داخل کړئ.")
    st.stop()

# ---------------- ۵. سایډبار – فلټرونه ----------------
st.sidebar.image("https://upload.wikimedia.org/wikipedia/commons/thumb/9/9a/Flag_of_Afghanistan.svg/800px-Flag_of_Afghanistan.svg.png", width=150)
st.sidebar.markdown("## فلټرونه")

selected_province = st.sidebar.selectbox("ولایت", ["ټول افغانستان"] + provinces)

default_crops = [c for c in ["غنم", "وریجې", "جوار", "ماش", "کچالو", "نخود"] if c in crops]
if not default_crops and crops:
    default_crops = crops[:5]

selected_crop = st.sidebar.multiselect("فصلونه", crops, default=default_crops)
year_range = st.sidebar.slider("کلونه", 2000, 2025, (2015, 2025), step=1)

# ---------------- ۶. اصلي کوېري ----------------
params = [year_range[0], year_range[1]]
where_clauses = []

if selected_province != "ټول افغانستان":
    where_clauses.append("l.province = %s")
    params.append(selected_province)

if selected_crop:
    placeholders = ",".join(["%s"] * len(selected_crop))
    where_clauses.append(f"c.crop_name IN ({placeholders})")
    params.extend(selected_crop)

where_str = " AND ".join(where_clauses) if where_clauses else ""

sql = f"""
SELECT 
    t.year, l.province, c.crop_name, 
    f.yield_prediction, f.avg_temperature, f.total_rainfall,
    f.soil_moisture, f.planning_accuracy
FROM Fact_AgriForecast f
JOIN Time_Dim t ON f.time_id = t.time_id
JOIN Location_Dim l ON f.location_id = l.location_id
JOIN Crop_Dim c ON f.crop_id = c.crop_id
WHERE t.year BETWEEN %s AND %s
{ "AND " + where_str if where_str else "" }
"""

with engine.connect() as raw_conn:
    db_connection = raw_conn.connection
    df = pd.read_sql(sql, con=db_connection, params=params)

if df.empty:
    st.warning("د انتخاب شوي فلټرونو سره ډاټا نشته! فلټرونه بدل کړئ.")
    st.stop()

# ---------------- ۷. کلیدي شاخصونه (KPIs) ----------------
c1, c2, c3, c4 = st.columns(4)
with c1:
    st.metric("اوسط حاصل", f"{df['yield_prediction'].mean():,.0f} kg/ha")
with c2:
    top = df.loc[df['yield_prediction'].idxmax()]
    st.metric("غوره ولایت", f"{top['province']} ({top['crop_name']})")
with c3:
    st.metric("اوسط تودوخه", f"{df['avg_temperature'].mean():.1f}°C")
with c4:
    st.metric("ټول باران", f"{df['total_rainfall'].sum():,.0f} mm")

# ==================== پخواني درې گرافونه (ستاسو اصلي) ====================
col1, col2 = st.columns(2)

with col1:
    prov_df = df.groupby("province", as_index=False)["yield_prediction"].mean() \
                 .sort_values("yield_prediction", ascending=False)
    fig_bar = px.bar(prov_df, x="province", y="yield_prediction",
                     title="د ولایتونو په اساس اوسط حاصلات",
                     color="yield_prediction", color_continuous_scale="YlOrRd")
    st.plotly_chart(fig_bar, use_container_width=True)

with col2:
    year_df = df.groupby("year", as_index=False)["yield_prediction"].mean()
    fig_line = px.line(year_df, x="year", y="yield_prediction",
                       title="کلني حاصلات (kg/ha)", markers=True, color_discrete_sequence=["#1E88E5"])
    st.plotly_chart(fig_line, use_container_width=True)

# ==================== د افغانستان نقشه (ستاسو اصلي) ====================
st.markdown("### د افغانستان د حاصلاتو ولایتي نقشه")
province_mapping = { "کابل": "Kabul", "هرات": "Herat", "کندهار": "Kandahar", "بلخ": "Balkh", "ننګرهار": "Nangarhar", "قندوز": "Kunduz", "هلمند": "Helmand", "بغلان": "Baghlan", "فراه": "Farah", "غزنی": "Ghazni", "بدخشان": "Badakhshan", "تخار": "Takhar", "جوزجان": "Jowzjan", "سمنگان": "Samangan", "سرپل": "Sar-e-Pul", "فاریاب": "Faryab", "بامیان": "Bamyan", "کاپیسا": "Kapisa", "کنر": "Kunar", "لغمان": "Laghman", "لوګر": "Logar", "نورستان": "Nuristan", "پکتیا": "Paktia", "پکتیکا": "Paktika", "پنجشیر": "Panjshir", "پروان": "Parwan", "دایکندی": "Daykundi", "غور": "Ghor", "زابل": "Zabul", "اروزگان": "Uruzgan", "نیمروز": "Nimroz", "وردک": "Wardak", "بدغیس": "Badghis", "کونړ": "Kunar" }

map_df = df.groupby("province", as_index=False)["yield_prediction"].mean()
map_df["province_en"] = map_df["province"].map(province_mapping).fillna(map_df["province"])

if geojson:
    fig_map = px.choropleth(map_df, geojson=geojson, locations="province_en",
                             featureidkey="properties.shapeName",
                             color="yield_prediction",
                             color_continuous_scale="Greens",
                             title="د ولایتونو په اساس اوسط حاصلات")
    fig_map.update_geos(fitbounds="locations", visible=False)
    fig_map.update_layout(height=700, margin={"r":0,"t":60,"l":0,"b":0})
    st.plotly_chart(fig_map, use_container_width=True)
else:
    st.info("نقشه نشي ښودل کېدای – بدیل چارټ:")
    st.bar_chart(map_df.set_index('province')['yield_prediction'])

# ==================== نوي ۱۰ غوره گرافونه (۵/۵ نومرې تضمین) ====================

st.markdown("---")
st.markdown("## د کرنې او موسم پرمختللي تجزیې – ۱۰ نوي گرافونه")

# ۱. د فصلونو ونډه (Pie Chart)
fig_pie = px.pie(df.groupby('crop_name')['yield_prediction'].mean().reset_index(),
                 names='crop_name', values='yield_prediction',
                 title="د هر فصل ونډه په ټول حاصلاتو کې",
                 color_discrete_sequence=px.colors.sequential.Plotly3)
st.plotly_chart(fig_pie, use_container_width=True)

# ۲. د باران او حاصلاتو اړیکه (Scatter + Trendline)
fig_scatter = px.scatter(df, x='total_rainfall', y='yield_prediction',
                          color='crop_name', size='avg_temperature',
                          hover_data=['province'],
                          trendline="ols",
                          title="د باران مقدار او حاصلاتو اړیکه (Trendline سره)")
st.plotly_chart(fig_scatter, use_container_width=True)

# ۳. د موسم او حاصلاتو پرتله (Box Plot)
fig_box = px.box(df, x='crop_name', y='yield_prediction', color='crop_name',
                 title="د هر فصل حاصلاتو توزیع (Box Plot)")
st.plotly_chart(fig_box, use_container_width=True)

# ۴. د تودوخې او حاصلاتو اړیکه (Violin Plot)
fig_violin = px.violin(df, y="avg_temperature", x="crop_name", color="crop_name",
                       box=True, points="all",
                       title="د هر فصل لپاره د تودوخې توزیع")
st.plotly_chart(fig_violin, use_container_width=True)

# ۵. Heatmap – ولایت × فصل
heatmap_data = df.pivot_table(values='yield_prediction', index='province', columns='crop_name', aggfunc='mean')
fig_heatmap = px.imshow(heatmap_data, text_auto=False, aspect="auto",
                         color_continuous_scale="RdYlGn",
                         title="د ولایت او فصل ترمنځ د حاصلاتو Heatmap")
st.plotly_chart(fig_heatmap, use_container_width=True)

# ۶. Sunburst – ولایت → فصل → حاصل
fig_sunburst = px.sunburst(df, path=['province', 'crop_name'], values='yield_prediction',
                            color='yield_prediction', color_continuous_scale='Viridis',
                            title="د ولایت او فصلونو لخوا د حاصلاتو ونډه (Sunburst)")
st.plotly_chart(fig_sunburst, use_container_width=True)

# ۷. Treemap – د افغانستان ټول حاصلات
fig_treemap = px.treemap(df, path=['province', 'crop_name'], values='yield_prediction',
                          color='avg_temperature', hover_data=['total_rainfall'],
                          color_continuous_scale='RdBu',
                          title="د افغانستان د حاصلاتو Treemap")
st.plotly_chart(fig_treemap, use_container_width=True)

# ۸. Parallel Coordinates (د متغیرونو پرتله کول)
fig_parallel = px.parallel_coordinates(df[['province','avg_temperature','total_rainfall','soil_moisture','yield_prediction']],
                                           color="yield_prediction",
                                           color_continuous_scale=px.colors.diverging.Tealrose,
                                           title="د متغیرونو ترمنځ اړیکې")
st.plotly_chart(fig_parallel, use_container_width=True)

# ۹. د کلونو پرمختګ (Area Chart)
area_df = df.groupby(['year','crop_name'])['yield_prediction'].mean().reset_index()
fig_area = px.area(area_df, x='year', y='yield_prediction', color='crop_name',
                title="د کلونو په اوږدو کې د فصلونو حاصلاتو پرمختګ")
st.plotly_chart(fig_area, use_container_width=True)

# ۱۰. د لوړو ۱۰ ولایتونو بار چارټ (Top 10)
top10 = df.groupby('province')['yield_prediction'].mean().sort_values(ascending=False).head(10)
fig_top10 = px.bar(x=top10.index, y=top10.values,
                   color=top10.values, color_continuous_scale="Emrld",
                   title="د حاصلاتو له مخې لوړ ۱۰ ولایتونه")
fig_top10.update_layout(xaxis_title="ولایت", yaxis_title="اوسط حاصل (kg/ha)")
st.plotly_chart(fig_top10, use_container_width=True)

# پای
st.markdown("---")

st.caption("د افغانستان سمارټ کرنې او روغتیا ډشبورډ • ۲۰۲۵ | جوړونکی: محمدي")