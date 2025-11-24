from flask import Flask, render_template, jsonify, request, send_file
import mysql.connector
import json
import subprocess
import pandas as pd
import io
from datetime import datetime
import logging

app = Flask(__name__)
app.secret_key = 'super_secure_secret_key_1404_agri_project'  # ضروري دی، بدل یې کړئ که غواړئ

# لاګ تنظیم
logging.basicConfig(
    filename='elt_log.txt',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='a',
    encoding='utf-8'
)
logger = logging.getLogger(__name__)

# config.json لوستل
with open('config.json', 'r', encoding='utf-8') as f:
    config = json.load(f)

# د MySQL اتصال تنظیمات
db_config = {
    'user': config['mysql']['user'],
    'password': config['mysql']['password'],
    'host': config['mysql']['host'],
    'database': config['target'],
    'raise_on_warnings': True,
    'autocommit': True
}

def get_db_connection():
    """د ډیټابیس اتصال بیرته ورکوي"""
    return mysql.connector.connect(**db_config)


# ====================== ETL او Data Generation ======================
@app.route('/run_etl', methods=['POST'])
def run_etl():
    try:
        result = subprocess.run(['python', 'enhanced_elt_pipeline.py'], capture_output=True, text=True, timeout=300)
        logger.info(f"ETL چل شوی: {result.stdout}")
        if result.returncode == 0:
            return jsonify({'status': 'success', 'message': 'ETL pipeline بریالی شو! ډیټا تازه شوه.'})
        else:
            logger.error(f"ETL ناکام: {result.stderr}")
            return jsonify({'status': 'error', 'message': f'ETL خطا: {result.stderr}'}), 500
    except Exception as e:
        logger.error(f"ETL استثنا: {str(e)}")
        return jsonify({'status': 'error', 'message': f'خطا: {str(e)}'}), 500


@app.route('/generate_data', methods=['POST'])
def generate_data():
    try:
        result = subprocess.run(['python', 'generate_integrated_data.py'], capture_output=True, text=True, timeout=300)
        logger.info(f"ډیټا تولید شوه: {result.stdout}")
        if result.returncode == 0:
            return jsonify({'status': 'success', 'message': '۱۰,۰۰۰ جعلي ریکارډونه په بریالیتوب سره جوړ شول!'})
        else:
            logger.error(f"ډیټا تولید ناکام: {result.stderr}")
            return jsonify({'status': 'error', 'message': f'خطا: {result.stderr}'}), 500
    except Exception as e:
        logger.error(f"ډیټا تولید استثنا: {str(e)}")
        return jsonify({'status': 'error', 'message': f'خطا: {str(e)}'}), 500


# ====================== API Routes (ټول jsonify سره) ======================
@app.route('/api/summary')
def get_summary():
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
    SELECT 'Time_Dim' AS TableName, COUNT(*) AS RowCount FROM Time_Dim
    UNION ALL SELECT 'Location_Dim', COUNT(*) FROM Location_Dim
    UNION ALL SELECT 'Crop_Dim', COUNT(*) FROM Crop_Dim
    UNION ALL SELECT 'AirQuality_Dim', COUNT(*) FROM AirQuality_Dim
    UNION ALL SELECT 'HealthCondition_Dim', COUNT(*) FROM HealthCondition_Dim
    UNION ALL SELECT 'Fact_AgriForecast', COUNT(*) FROM Fact_AgriForecast
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    result = [{'TableName': row[0], 'RowCount': row[1]} for row in rows]
    return jsonify(result)


@app.route('/api/crop_season')
def get_crop_season():
    conn = get_db_connection()
    query = """
    SELECT c.crop_name, t.season, ROUND(AVG(f.yield_prediction), 2) AS Avg_Yield 
    FROM Fact_AgriForecast f 
    JOIN Crop_Dim c ON f.crop_id = c.crop_id 
    JOIN Time_Dim t ON f.time_id = t.time_id 
    GROUP BY c.crop_name, t.season 
    ORDER BY Avg_Yield DESC LIMIT 10
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return jsonify(df.to_dict('records'))


@app.route('/api/pollution_health')
def get_pollution_health():
    conn = get_db_connection()
    query = """
    SELECT a.pollution_level, 
           ROUND(AVG(f.combined_risk), 3) AS Avg_Health_Risk, 
           ROUND(AVG(a.pm25_level), 2) AS Avg_PM25 
    FROM Fact_IntegratedAnalysis f 
    JOIN AirQuality_Dim a ON f.air_id = a.air_id 
    GROUP BY a.pollution_level 
    ORDER BY Avg_PM25 DESC
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return jsonify(df.to_dict('records'))


@app.route('/api/seasonal')
def get_seasonal():
    conn = get_db_connection()
    query = """
    SELECT t.season, 
           ROUND(AVG(f.avg_temperature), 2) AS Avg_Temp, 
           ROUND(AVG(f.total_rainfall), 2) AS Avg_Rain 
    FROM Fact_AgriForecast f 
    JOIN Time_Dim t ON f.time_id = t.time_id 
    GROUP BY t.season
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return jsonify(df.to_dict('records'))


@app.route('/api/crop_rainfall')
def get_crop_rainfall():
    conn = get_db_connection()
    query = """
    SELECT c.crop_name, 
           ROUND(AVG(f.total_rainfall), 2) AS Avg_Rainfall_mm, 
           ROUND(AVG(f.yield_prediction), 2) AS Avg_Yield 
    FROM Fact_AgriForecast f 
    JOIN Crop_Dim c ON f.crop_id = c.crop_id 
    GROUP BY c.crop_name 
    ORDER BY Avg_Rainfall_mm DESC LIMIT 8
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return jsonify(df.to_dict('records'))


@app.route('/api/map_data')
def get_map_data():
    conn = get_db_connection()
    query = """
    SELECT l.city_name, l.latitude, l.longitude, 
           ROUND(AVG(f.yield_prediction), 2) AS Avg_Yield 
    FROM Fact_AgriForecast f 
    JOIN Location_Dim l ON f.location_id = l.location_id 
    GROUP BY l.location_id, l.city_name, l.latitude, l.longitude
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return jsonify(df.to_dict('records'))


# ====================== CSV Export ======================
@app.route('/export/<endpoint>')
def export_csv(endpoint):
    valid_endpoints = ['summary', 'crop_season', 'seasonal', 'crop_rainfall', 'map_data', 'pollution_health']
    if endpoint not in valid_endpoints:
        return "ناسم endpoint! منل شوي: " + ", ".join(valid_endpoints), 400

    # د معلوماتو راوبل
    if endpoint == 'summary':
        data = get_summary().get_json()  # jsonify شوی دی، نو .get_json() کار کوي
    else:
        # نور ټول د pd.read_sql په شان دي
        func_map = {
            'crop_season': get_crop_season,
            'seasonal': get_seasonal,
            'crop_rainfall': get_crop_rainfall,
            'map_data': get_map_data,
            'pollution_health': get_pollution_health
        }
        data = func_map[endpoint]().get_json()

    df = pd.DataFrame(data)
    output = io.BytesIO()
    df.to_csv(output, index=False, encoding='utf-8-sig')  # utf-8-sig د excel لپاره ښه دی
    output.seek(0)

    return send_file(
        output,
        mimetype='text/csv',
        as_attachment=True,
        download_name=f'{endpoint}_داده_{datetime.now().strftime("%Y%m%d")}.csv'
    )


# ====================== اصلي پاڼه ======================
@app.route('/')
def dashboard():
    return render_template('dashboard.html')


# ====================== چلول ======================
if __name__ == '__main__':
    print("Dashboard چلېږي په http://localhost:5000")
    app.run(debug=False, host='0.0.0.0', port=5000)  # په production کې debug=False ساتئ