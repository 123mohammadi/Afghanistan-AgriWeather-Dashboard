import mysql.connector
from faker import Faker
import random
from datetime import datetime, timedelta
import numpy as np

# MySQL تړل
config = {
    'user': 'root',
    'password': 'Moh123',  
    'host': 'localhost',
    'database': 'IntegratedWeatherDB'
}

fake = Faker('en_US')
conn = mysql.connector.connect(**config)
cursor = conn.cursor()

BATCH_SIZE = 1000
NUM_RECORDS = 10000

def insert_batch(table_name, columns, data):
    placeholders = ', '.join(['%s'] * len(columns))
    sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
    cursor.executemany(sql, data)
    conn.commit()
    print(f"Inserted {len(data)} records into {table_name}")

# ۱. Time_Dim
print("Generating Time data...")
time_data = []
start_date = datetime(2020, 1, 1)
for _ in range(NUM_RECORDS):
    date = start_date + timedelta(days=random.randint(0, 1825))
    day, month, year = date.day, date.month, date.year
    season = random.choice(['Spring', 'Summer', 'Autumn', 'Winter'])
    hour = random.randint(0, 23)
    weekday = date.strftime('%A')
    time_data.append((date, day, month, year, season, hour, weekday))
columns_time = ['date', 'day', 'month', 'year', 'season', 'hour', 'weekday']
for i in range(0, NUM_RECORDS, BATCH_SIZE):
    batch = time_data[i:i + BATCH_SIZE]
    insert_batch('Time_Dim', columns_time, batch)

# ۲. Location_Dim
print("Generating Location data...")
locations = ['Kabul', 'Herat', 'Mazar-i-Sharif', 'Kandahar', 'Jalalabad'] * (NUM_RECORDS // 5 + 1)
location_data = []
for loc in locations[:NUM_RECORDS]:
    province = fake.city()
    country = 'Afghanistan'
    lat = round(random.uniform(30, 38), 6)
    lon = round(random.uniform(60, 75), 6)
    pop_density = random.randint(1000, 10000)
    elevation = random.randint(500, 3000)
    location_data.append((loc, province, country, lat, lon, pop_density, elevation))
columns_loc = ['city_name', 'province', 'country', 'latitude', 'longitude', 'population_density', 'elevation']
for i in range(0, NUM_RECORDS, BATCH_SIZE):
    batch = location_data[i:i + BATCH_SIZE]
    insert_batch('Location_Dim', columns_loc, batch)

# ۳. Crop_Dim
print("Generating Crop data...")
crops = ['Wheat', 'Rice', 'Corn', 'Barley', 'Cotton', 'Potato'] * (NUM_RECORDS // 6 + 1)
crop_data = []
for crop in crops[:NUM_RECORDS]:
    period = random.choice(['Spring', 'Summer', 'Autumn'])
    water_req = round(random.uniform(50, 200), 1)
    growth_days = random.randint(90, 180)
    crop_data.append((crop, period, water_req, growth_days))
columns_crop = ['crop_name', 'planting_period', 'water_requirement', 'growth_days']
for i in range(0, NUM_RECORDS, BATCH_SIZE):
    batch = crop_data[i:i + BATCH_SIZE]
    insert_batch('Crop_Dim', columns_crop, batch)

# ۴. AirQuality_Dim
print("Generating AirQuality data...")
air_data = []
for _ in range(NUM_RECORDS):
    poll_level = random.choice(['Low', 'Medium', 'High', 'Extreme'])
    aqi_range = f"{random.randint(0, 500)}"
    main_poll = random.choice(['PM2.5', 'NO2', 'CO2', 'O3'])
    pm25 = round(random.uniform(0, 500), 1)
    air_data.append((poll_level, aqi_range, main_poll, pm25))
columns_air = ['pollution_level', 'aqi_range', 'main_pollutant', 'pm25_level']
for i in range(0, NUM_RECORDS, BATCH_SIZE):
    batch = air_data[i:i + BATCH_SIZE]
    insert_batch('AirQuality_Dim', columns_air, batch)

# ۵. Weather_Dim
print("Generating Weather data...")
weather_data = []
for _ in range(NUM_RECORDS):
    temp_range = f"{random.randint(10, 40)}-{random.randint(20, 50)}°C"
    hum_range = f"{random.randint(30, 70)}-{random.randint(60, 90)}%"
    rain_range = f"{random.randint(0, 50)}-{random.randint(50, 200)}mm"
    wind_speed = round(random.uniform(5, 25), 1)
    weather_data.append((temp_range, hum_range, rain_range, wind_speed))
columns_weather = ['temperature_range', 'humidity_range', 'rainfall_range', 'wind_speed']
for i in range(0, NUM_RECORDS, BATCH_SIZE):
    batch = weather_data[i:i + BATCH_SIZE]
    insert_batch('Weather_Dim', columns_weather, batch)

# ۶. HealthCondition_Dim (نوې اضافه شوې – د foreign key لپاره اړین)
print("Generating HealthCondition data...")
health_data = []
diseases = ['Asthma', 'Bronchitis', 'Allergy', 'COVID-19', 'Flu']
for _ in range(NUM_RECORDS):
    disease = random.choice(diseases)
    risk = random.choice(['Low', 'Medium', 'High'])
    severity = random.randint(1, 10)
    health_data.append((disease, risk, severity))
columns_health = ['disease_type', 'risk_level', 'symptom_severity']
for i in range(0, NUM_RECORDS, BATCH_SIZE):
    batch = health_data[i:i + BATCH_SIZE]
    insert_batch('HealthCondition_Dim', columns_health, batch)

# ۷. Integrated_Dim
print("Generating Integrated data...")
int_data = []
for _ in range(NUM_RECORDS):
    crop_id = random.randint(1, NUM_RECORDS)
    health_id = random.randint(1, NUM_RECORDS)
    corr_type = random.choice(['Positive', 'Negative', 'Neutral'])
    impact = round(random.uniform(0, 1), 2)
    int_data.append((crop_id, health_id, corr_type, impact))
columns_int = ['crop_id', 'health_id', 'correlation_type', 'impact_factor']
for i in range(0, NUM_RECORDS, BATCH_SIZE):
    batch = int_data[i:i + BATCH_SIZE]
    insert_batch('Integrated_Dim', columns_int, batch)

# ۸. Fact_IntegratedAnalysis
print("Generating Fact IntegratedAnalysis data...")
analysis_data = []
for _ in range(NUM_RECORDS):
    time_id = random.randint(1, NUM_RECORDS)
    loc_id = random.randint(1, NUM_RECORDS)
    crop_id = random.randint(1, NUM_RECORDS)
    air_id = random.randint(1, NUM_RECORDS)
    weather_id = random.randint(1, NUM_RECORDS)
    int_id = random.randint(1, NUM_RECORDS)
    yield_combined = round(random.uniform(1000, 5000), 1)
    risk_combined = round(random.uniform(0, 100), 1)
    corr_score = round(random.uniform(-1, 1), 2)
    total_impact = round(random.uniform(0, 99.99), 1)
    analysis_data.append((time_id, loc_id, crop_id, air_id, weather_id, int_id, yield_combined, risk_combined, corr_score, total_impact))
columns_analysis = ['time_id', 'location_id', 'crop_id', 'air_id', 'weather_id', 'int_id', 'combined_yield', 'combined_risk', 'correlation_score', 'total_impact']
for i in range(0, NUM_RECORDS, BATCH_SIZE):
    batch = analysis_data[i:i + BATCH_SIZE]
    insert_batch('Fact_IntegratedAnalysis', columns_analysis, batch)

# ۹. Fact_CrossEffects (سم شوی – mitigation_score range سم کړم، او health_id اوس شته)
print("Generating Fact CrossEffects data...")
effect_data = []
for _ in range(NUM_RECORDS):
    time_id = random.randint(1, NUM_RECORDS)
    loc_id = random.randint(1, NUM_RECORDS)
    crop_id = random.randint(1, NUM_RECORDS)
    health_id = random.randint(1, NUM_RECORDS) 
    weather_id = random.randint(1, NUM_RECORDS)
    air_id = random.randint(1, NUM_RECORDS)
    yield_loss = round(random.uniform(0, 50), 1)
    cases_inc = random.randint(0, 200)
    mit_score = round(random.uniform(0, 99.99), 1)  # سم شوی: ۹۹.۹۹ ماکس
    effect_data.append((time_id, loc_id, crop_id, health_id, weather_id, air_id, yield_loss, cases_inc, mit_score))
columns_effect = ['time_id', 'location_id', 'crop_id', 'health_id', 'weather_id', 'air_id', 'yield_loss', 'health_cases_increase', 'mitigation_score']
for i in range(0, NUM_RECORDS, BATCH_SIZE):
    batch = effect_data[i:i + BATCH_SIZE]
    insert_batch('Fact_CrossEffects', columns_effect, batch)

# ۱۰. Fact_SummaryMetrics
print("Generating Fact SummaryMetrics data...")
metric_data = []
for _ in range(NUM_RECORDS):
    time_id = random.randint(1, NUM_RECORDS)
    loc_id = random.randint(1, NUM_RECORDS)
    overall = round(random.uniform(0, 100), 2)
    agri_cont = round(random.uniform(0, 50), 2)
    health_cont = round(random.uniform(0, 50), 2)
    metric_data.append((time_id, loc_id, overall, agri_cont, health_cont))
columns_metric = ['time_id', 'location_id', 'overall_score', 'agri_contribution', 'health_contribution']
for i in range(0, NUM_RECORDS, BATCH_SIZE):
    batch = metric_data[i:i + BATCH_SIZE]
    insert_batch('Fact_SummaryMetrics', columns_metric, batch)

# ۱۱. Users
print("Generating Users data...")
users_data = [(fake.user_name(), random.choice(['Analyst', 'Admin', 'User']), fake.email(), fake.date_this_decade()) for _ in range(NUM_RECORDS)]
columns_users = ['username', 'role', 'email', 'created_date']
for i in range(0, NUM_RECORDS, BATCH_SIZE):
    batch = users_data[i:i + BATCH_SIZE]
    insert_batch('Users', columns_users, batch)

# ۱۲. AuditLogs
print("Generating AuditLogs data...")
audit_data = [(random.randint(1, NUM_RECORDS), fake.sentence(nb_words=4), fake.date_time_this_decade(), random.choice(['Fact_IntegratedAnalysis', 'Users'])) for _ in range(NUM_RECORDS)]
columns_audit = ['user_id', 'action', 'timestamp', 'table_name']
for i in range(0, NUM_RECORDS, BATCH_SIZE):
    batch = audit_data[i:i + BATCH_SIZE]
    insert_batch('AuditLogs', columns_audit, batch)

# ۱۳. Metadata_Integrated
print("Generating Metadata_Integrated data...")
meta_data = [(random.randint(1, NUM_RECORDS), random.choice(['Agri', 'Health']), random.choice(['Daily', 'Weekly'])) for _ in range(NUM_RECORDS)]
columns_meta = ['int_id', 'data_source', 'update_frequency']
for i in range(0, NUM_RECORDS, BATCH_SIZE):
    batch = meta_data[i:i + BATCH_SIZE]
    insert_batch('Metadata_Integrated', columns_meta, batch)

# ۱۴. SensorIntegrated
print("Generating SensorIntegrated data...")
sensor_data = [(random.randint(1, NUM_RECORDS), random.randint(1, NUM_RECORDS), random.choice(['AgriSensor', 'HealthSensor']), round(random.uniform(0, 100), 1)) for _ in range(NUM_RECORDS)]
columns_sensor = ['location_id', 'time_id', 'sensor_type', 'reading_value']
for i in range(0, NUM_RECORDS, BATCH_SIZE):
    batch = sensor_data[i:i + BATCH_SIZE]
    insert_batch('SensorIntegrated', columns_sensor, batch)

# ۱۵. ProfileIntegrated
print("Generating ProfileIntegrated data...")
profile_data = [(random.randint(1, NUM_RECORDS), random.choice(['Farmer', 'Patient']), fake.text(max_nb_chars=200)) for _ in range(NUM_RECORDS)]
columns_profile = ['location_id', 'profile_type', 'details']
for i in range(0, NUM_RECORDS, BATCH_SIZE):
    batch = profile_data[i:i + BATCH_SIZE]
    insert_batch('ProfileIntegrated', columns_profile, batch)

# ۱۶. AlertIntegrated (اضافي، که اړتیا وي)
print("Generating AlertIntegrated data...")
alert_data = [(random.randint(1, NUM_RECORDS), random.randint(1, NUM_RECORDS), random.randint(1, NUM_RECORDS), random.choice(['CrossAlert', 'General']), random.choice(['High', 'Low'])) for _ in range(NUM_RECORDS)]
columns_alert = ['time_id', 'location_id', 'int_id', 'alert_type', 'alert_level']
for i in range(0, NUM_RECORDS, BATCH_SIZE):
    batch = alert_data[i:i + BATCH_SIZE]
    insert_batch('AlertIntegrated', columns_alert, batch)

cursor.close()
conn.close()
print("All 10K records inserted into IntegratedWeatherDB successfully!")