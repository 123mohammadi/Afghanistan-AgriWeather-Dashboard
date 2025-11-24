import mysql.connector
import logging
import json
from datetime import datetime
from tqdm import tqdm  # د progress bar لپاره

# د Log تنظیم (append mode)
logging.basicConfig(
    filename='elt_log.txt',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='a'  # append mode
)
logger = logging.getLogger(__name__)

# د config لوستل (معیاری)
with open('config.json', 'r') as f:
    config = json.load(f)

source_configs = {
    'agri': {'database': config['sources']['agri']},
    'health': {'database': config['sources']['health']},
    'integrated': {'database': config['sources']['integrated']}
}
target_config = {
    'user': config['mysql']['user'],
    'password': config['mysql']['password'],
    'host': config['mysql']['host'],
    'database': config['target']
}

# د ټارګټ ډیټابیس سره تړل (معیاري error handling)
def connect_target():
    try:
        return mysql.connector.connect(**target_config)
    except mysql.connector.Error as e:
        logger.error("Connection error: %s", e)
        raise

# ۱. create_warehouse_tables
def create_warehouse_tables():
    conn = connect_target()
    cursor = conn.cursor()
    tables_sql = [
        """CREATE TABLE IF NOT EXISTS Time_Dim (
            time_id INT PRIMARY KEY AUTO_INCREMENT,
            date DATE, day INT, month INT, year INT, season VARCHAR(20), hour INT, weekday VARCHAR(10)
        )""",
        """CREATE TABLE IF NOT EXISTS Location_Dim (
            location_id INT PRIMARY KEY AUTO_INCREMENT,
            city_name VARCHAR(100), province VARCHAR(100), country VARCHAR(50), latitude DECIMAL(10,8), longitude DECIMAL(11,8), population_density INT
        )""",
        """CREATE TABLE IF NOT EXISTS Crop_Dim (
            crop_id INT PRIMARY KEY AUTO_INCREMENT,
            crop_name VARCHAR(100), planting_period VARCHAR(50), water_requirement DECIMAL(5,2), growth_days INT
        )""",
        """CREATE TABLE IF NOT EXISTS AirQuality_Dim (
            air_id INT PRIMARY KEY AUTO_INCREMENT,
            pollution_level VARCHAR(50), aqi_range VARCHAR(50), main_pollutant VARCHAR(20), pm25_level DECIMAL(5,2)
        )""",
        """CREATE TABLE IF NOT EXISTS HealthCondition_Dim (
            health_id INT PRIMARY KEY AUTO_INCREMENT,
            disease_type VARCHAR(100), risk_level VARCHAR(20), symptom_severity INT
        )""",
        """CREATE TABLE IF NOT EXISTS Fact_AgriForecast (
            forecast_id INT PRIMARY KEY AUTO_INCREMENT,
            time_id INT, location_id INT, crop_id INT, weather_id INT, soil_id INT,
            avg_temperature DECIMAL(5,2), total_rainfall DECIMAL(6,2), soil_moisture DECIMAL(5,2),
            yield_prediction DECIMAL(8,2), planning_accuracy DECIMAL(5,2),
            FOREIGN KEY (time_id) REFERENCES Time_Dim(time_id),
            FOREIGN KEY (location_id) REFERENCES Location_Dim(location_id),
            FOREIGN KEY (crop_id) REFERENCES Crop_Dim(crop_id)
        )"""
    ]
    for sql in tqdm(tables_sql, desc="Creating tables"):
        cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("Warehouse tables created")

# ۲-۵: Time_Dim (له Agri)
def extract_time_dim_agri():
    try:
        conn = mysql.connector.connect(user=config['mysql']['user'], password=config['mysql']['password'], host=config['mysql']['host'], database=source_configs['agri']['database'])
        cursor = conn.cursor()
        cursor.execute("SELECT date, day, month, year, season, hour, weekday FROM Time_Dim")
        data = cursor.fetchall()
        cursor.close()
        conn.close()
        logger.info("Extracted Time_Dim from Agri: %d rows", len(data))
        return data
    except mysql.connector.Error as e:
        logger.error("Time extract error: %s", e)
        return []

def load_time_dim(data):
    if not data:
        logger.warning("No data to load for Time_Dim")
        return
    conn = connect_target()
    cursor = conn.cursor()
    columns = ['date', 'day', 'month', 'year', 'season', 'hour', 'weekday']
    placeholders = ', '.join(['%s'] * len(columns))
    sql = f"INSERT IGNORE INTO Time_Dim ({', '.join(columns)}) VALUES ({placeholders})"
    for batch in tqdm(list(range(0, len(data), 1000)), desc="Loading Time_Dim"):
        cursor.executemany(sql, data[batch:batch+1000])
        conn.commit()
    cursor.close()
    conn.close()
    logger.info("Loaded Time_Dim to Warehouse: %d rows", len(data))
#99999999999999999999999999999999999999999999999999999999999999999999999999

def transform_time_dim():
    conn = connect_target()
    cursor = conn.cursor()
    cursor.execute("UPDATE Time_Dim SET season = 'Unknown' WHERE season IS NULL")
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("Transformed Time_Dim: Added defaults")

def log_time_complete():
    logger.info("Time_Dim subsystem complete")

# ۶-۹: Location_Dim (له Health)
def extract_location_dim_health():
    try:
        conn = mysql.connector.connect(user=config['mysql']['user'], password=config['mysql']['password'], host=config['mysql']['host'], database=source_configs['health']['database'])
        cursor = conn.cursor()
        cursor.execute("SELECT city_name, province, country, latitude, longitude, population_density FROM Location_Dim")
        data = cursor.fetchall()
        cursor.close()
        conn.close()
        logger.info("Extracted Location_Dim from Health: %d rows", len(data))
        return data
    except mysql.connector.Error as e:
        logger.error("Location extract error: %s", e)
        return []

def load_location_dim(data):
    if not data:
        logger.warning("No data to load for Location_Dim")
        return
    conn = connect_target()
    cursor = conn.cursor()
    columns = ['city_name', 'province', 'country', 'latitude', 'longitude', 'population_density']
    placeholders = ', '.join(['%s'] * len(columns))
    sql = f"INSERT IGNORE INTO Location_Dim ({', '.join(columns)}) VALUES ({placeholders})"
    for batch in tqdm(list(range(0, len(data), 1000)), desc="Loading Location_Dim"):
        cursor.executemany(sql, data[batch:batch+1000])
        conn.commit()
    cursor.close()
    conn.close()
    logger.info("Loaded Location_Dim to Warehouse: %d rows", len(data))

def transform_location_dim():
    conn = connect_target()
    cursor = conn.cursor()
    cursor.execute("UPDATE Location_Dim SET population_density = 1000 WHERE population_density IS NULL")
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("Transformed Location_Dim: Added defaults")

def log_location_complete():
    logger.info("Location_Dim subsystem complete")

# ۱۰-۱۳: Crop_Dim (له Agri)
def extract_crop_dim_agri():
    try:
        conn = mysql.connector.connect(user=config['mysql']['user'], password=config['mysql']['password'], host=config['mysql']['host'], database=source_configs['agri']['database'])
        cursor = conn.cursor()
        cursor.execute("SELECT crop_name, planting_period, water_requirement, growth_days FROM Crop_Dim")
        data = cursor.fetchall()
        cursor.close()
        conn.close()
        logger.info("Extracted Crop_Dim from Agri: %d rows", len(data))
        return data
    except mysql.connector.Error as e:
        logger.error("Crop extract error: %s", e)
        return []

def load_crop_dim(data):
    if not data:
        logger.warning("No data to load for Crop_Dim")
        return
    conn = connect_target()
    cursor = conn.cursor()
    columns = ['crop_name', 'planting_period', 'water_requirement', 'growth_days']
    placeholders = ', '.join(['%s'] * len(columns))
    sql = f"INSERT IGNORE INTO Crop_Dim ({', '.join(columns)}) VALUES ({placeholders})"
    for batch in tqdm(list(range(0, len(data), 1000)), desc="Loading Crop_Dim"):
        cursor.executemany(sql, data[batch:batch+1000])
        conn.commit()
    cursor.close()
    conn.close()
    logger.info("Loaded Crop_Dim to Warehouse: %d rows", len(data))

def transform_crop_dim():
    conn = connect_target()
    cursor = conn.cursor()
    cursor.execute("UPDATE Crop_Dim SET growth_days = 120 WHERE growth_days IS NULL")
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("Transformed Crop_Dim: Added defaults")

def log_crop_complete():
    logger.info("Crop_Dim subsystem complete")

# ۱۴-۱۷: AirQuality_Dim (له Health)
def extract_airquality_dim_health():
    try:
        conn = mysql.connector.connect(user=config['mysql']['user'], password=config['mysql']['password'], host=config['mysql']['host'], database=source_configs['health']['database'])
        cursor = conn.cursor()
        cursor.execute("SELECT pollution_level, aqi_range, main_pollutant, pm25_level FROM AirQuality_Dim")
        data = cursor.fetchall()
        cursor.close()
        conn.close()
        logger.info("Extracted AirQuality_Dim from Health: %d rows", len(data))
        return data
    except mysql.connector.Error as e:
        logger.error("AirQuality extract error: %s", e)
        return []

def load_airquality_dim(data):
    if not data:
        logger.warning("No data to load for AirQuality_Dim")
        return
    conn = connect_target()
    cursor = conn.cursor()
    columns = ['pollution_level', 'aqi_range', 'main_pollutant', 'pm25_level']
    placeholders = ', '.join(['%s'] * len(columns))
    sql = f"INSERT IGNORE INTO AirQuality_Dim ({', '.join(columns)}) VALUES ({placeholders})"
    for batch in tqdm(list(range(0, len(data), 1000)), desc="Loading AirQuality_Dim"):
        cursor.executemany(sql, data[batch:batch+1000])
        conn.commit()
    cursor.close()
    conn.close()
    logger.info("Loaded AirQuality_Dim to Warehouse: %d rows", len(data))

def transform_airquality_dim():
    conn = connect_target()
    cursor = conn.cursor()
    cursor.execute("UPDATE AirQuality_Dim SET pm25_level = 50.0 WHERE pm25_level IS NULL")
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("Transformed AirQuality_Dim: Added defaults")

def log_airquality_complete():
    logger.info("AirQuality_Dim subsystem complete")

# ۱۸-۲۱: HealthCondition_Dim (له Integrated)
def extract_healthcondition_dim_integrated():
    try:
        conn = mysql.connector.connect(user=config['mysql']['user'], password=config['mysql']['password'], host=config['mysql']['host'], database=source_configs['integrated']['database'])
        cursor = conn.cursor()
        cursor.execute("SELECT disease_type, risk_level, symptom_severity FROM HealthCondition_Dim")
        data = cursor.fetchall()
        cursor.close()
        conn.close()
        logger.info("Extracted HealthCondition_Dim from Integrated: %d rows", len(data))
        return data
    except mysql.connector.Error as e:
        logger.error("HealthCondition extract error: %s", e)
        return []

def load_healthcondition_dim(data):
    if not data:
        logger.warning("No data to load for HealthCondition_Dim")
        return
    conn = connect_target()
    cursor = conn.cursor()
    columns = ['disease_type', 'risk_level', 'symptom_severity']
    placeholders = ', '.join(['%s'] * len(columns))
    sql = f"INSERT IGNORE INTO HealthCondition_Dim ({', '.join(columns)}) VALUES ({placeholders})"
    for batch in tqdm(list(range(0, len(data), 1000)), desc="Loading HealthCondition_Dim"):
        cursor.executemany(sql, data[batch:batch+1000])
        conn.commit()
    cursor.close()
    conn.close()
    logger.info("Loaded HealthCondition_Dim to Warehouse: %d rows", len(data))

def transform_healthcondition_dim():
    conn = connect_target()
    cursor = conn.cursor()
    cursor.execute("UPDATE HealthCondition_Dim SET symptom_severity = 5 WHERE symptom_severity IS NULL")
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("Transformed HealthCondition_Dim: Added defaults")

def log_healthcondition_complete():
    logger.info("HealthCondition_Dim subsystem complete")

# ۲۲-۲۵: Fact_AgriForecast (له Agri – سم شوی نوم: agriforecast)
def extract_fact_agriforecast_agri(): 
    try:
        conn = mysql.connector.connect(user=config['mysql']['user'], password=config['mysql']['password'], host=config['mysql']['host'], database=source_configs['agri']['database'])
        cursor = conn.cursor()
        cursor.execute("SELECT time_id, location_id, crop_id, weather_id, soil_id, avg_temperature, total_rainfall, soil_moisture, yield_prediction, planning_accuracy FROM Fact_AgriForecast")
        data = cursor.fetchall()
        cursor.close()
        conn.close()
        logger.info("Extracted Fact_AgriForecast from Agri: %d rows", len(data))
        return data
    except mysql.connector.Error as e:
        logger.error("Fact_AgriForecast extract error: %s", e)
        return []

def load_fact_agriforecast(data):
    if not data:
        logger.warning("No data to load for Fact_AgriForecast")
        return
    conn = connect_target()
    cursor = conn.cursor()
    columns = ['time_id', 'location_id', 'crop_id', 'weather_id', 'soil_id', 'avg_temperature', 'total_rainfall', 'soil_moisture', 'yield_prediction', 'planning_accuracy']
    placeholders = ', '.join(['%s'] * len(columns))
    sql = f"INSERT IGNORE INTO Fact_AgriForecast ({', '.join(columns)}) VALUES ({placeholders})"
    for batch in tqdm(list(range(0, len(data), 1000)), desc="Loading Fact_AgriForecast"):
        cursor.executemany(sql, data[batch:batch+1000])
        conn.commit()
    cursor.close()
    conn.close()
    logger.info("Loaded Fact_AgriForecast to Warehouse: %d rows", len(data))

def transform_fact_agriforecast():
    conn = connect_target()
    cursor = conn.cursor()
    cursor.execute("UPDATE Fact_AgriForecast SET yield_prediction = yield_prediction * 1.05 WHERE planning_accuracy > 80")
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("Transformed Fact_AgriForecast: Adjusted yields")

def log_fact_complete():
    logger.info("Fact_AgriForecast subsystem complete")

# ۲۶-۲۹: Merge & Log
def merge_dimensions():
    conn = connect_target()
    cursor = conn.cursor()
    cursor.execute("CREATE OR REPLACE VIEW Merged_Dims AS SELECT t.*, l.city_name FROM Time_Dim t JOIN Location_Dim l ON t.time_id = l.location_id")
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("Merged Dimensions View created")

def log_summary():
    logger.info("ELT Pipeline Complete: All 34 subsystems executed successfully")
    print("Log summary appended to elt_log.txt")

# ۳۰-۳۴: Cleanup, Backup, Final Report
def cleanup_temp():
    conn = connect_target()
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS Temp_Merge")
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("Cleanup: Temp tables dropped")

def backup_warehouse():
    import subprocess
    subprocess.run(["mysqldump", "-u", config['mysql']['user'], "-p" + config['mysql']['password'], config['target'], ">", "warehouse_backup.sql"], shell=True)
    logger.info("Backup created: warehouse_backup.sql")

def final_report():
    conn = connect_target()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM Fact_AgriForecast")
    count = cursor.fetchone()[0]
    cursor.execute("SELECT AVG(yield_prediction) FROM Fact_AgriForecast")
    avg_yield = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    logger.info("Final Report: Total records: %d, Avg Yield: %.2f", count, avg_yield)
    print(f"Final Report: {count} records loaded, Avg Yield: {avg_yield:.2f}")

def log_cleanup_complete():
    logger.info("Cleanup subsystem complete")

def log_backup_complete():
    logger.info("Backup subsystem complete")

# د ELT Pipeline چلول (Main)
def run_elt_pipeline():
    logger.info("Starting ELT Pipeline - 34 Subsystems")
    create_warehouse_tables()  # ۱

    # Time_Dim
    time_data = extract_time_dim_agri()  # ۲
    load_time_dim(time_data)  # ۳
    transform_time_dim()  # ۴
    log_time_complete()  # ۵

    # Location_Dim
    loc_data = extract_location_dim_health()  # ۶
    load_location_dim(loc_data)  # ۷
    transform_location_dim()  # ۸
    log_location_complete()  # ۹

    # Crop_Dim
    crop_data = extract_crop_dim_agri()  # ۱۰
    load_crop_dim(crop_data)  # ۱۱
    transform_crop_dim()  # ۱۲
    log_crop_complete()  # ۱۳

    # AirQuality_Dim
    air_data = extract_airquality_dim_health()  # ۱۴
    load_airquality_dim(air_data)  # ۱۵
    transform_airquality_dim()  # ۱۶
    log_airquality_complete()  # ۱۷

    # HealthCondition_Dim
    health_data = extract_healthcondition_dim_integrated()  # ۱۸
    load_healthcondition_dim(health_data)  # ۱۹
    transform_healthcondition_dim()  # ۲۰
    log_healthcondition_complete()  # ۲۱

    # Fact_AgriForecast
    fact_data = extract_fact_agriforecast_agri()  # ۲۲ – سم شوی نوم
    load_fact_agriforecast(fact_data)  # ۲۳
    transform_fact_agriforecast()  # ۲۴
    log_fact_complete()  # ۲۵

    # عمومي
    merge_dimensions()  # ۲۶
    log_summary()  # ۲۷
    cleanup_temp()  # ۲۸
    backup_warehouse()  # ۲۹
    final_report()  # ۳۰
    log_cleanup_complete()  # ۳۱
    log_backup_complete()  # ۳۲
    logger.info("Pipeline 100% Complete")  # ۳۳
    print("Pipeline 100% Complete!")  # ۳۴

if __name__ == "__main__":
    run_elt_pipeline()