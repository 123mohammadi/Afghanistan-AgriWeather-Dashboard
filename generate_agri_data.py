import mysql.connector
from faker import Faker
import random
from datetime import datetime, timedelta
import numpy as np


config = {
    'user': 'root',
    'password': 'Moh123', 
    'host': 'localhost',
    'database': 'AgriWeatherDB'
}

fake = Faker('en_US')
conn = mysql.connector.connect(**config)
cursor = conn.cursor()

# د batch اندازه او ټول ریکارډونه
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
    elevation = random.randint(500, 3000)
    location_data.append((loc, province, country, lat, lon, elevation))
columns_loc = ['city_name', 'province', 'country', 'latitude', 'longitude', 'elevation']
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

# ۴. Weather_Dim
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

# ۵. Soil_Dim
print("Generating Soil data...")
soil_data = []
soil_types = ['Clay', 'Sandy', 'Loam', 'Silt']
for _ in range(NUM_RECORDS):
    soil_type = random.choice(soil_types)
    soil_ph = round(random.uniform(5.0, 8.5), 1)
    fertility = random.choice(['High', 'Medium', 'Low'])
    soil_data.append((soil_type, soil_ph, fertility))
columns_soil = ['soil_type', 'soil_ph', 'fertility_level']
for i in range(0, NUM_RECORDS, BATCH_SIZE):
    batch = soil_data[i:i + BATCH_SIZE]
    insert_batch('Soil_Dim', columns_soil, batch)

# ۶. Fact_AgriForecast
print("Generating Fact AgriForecast data...")
fact_data = []
for _ in range(NUM_RECORDS):
    time_id = random.randint(1, NUM_RECORDS)
    loc_id = random.randint(1, NUM_RECORDS)
    crop_id = random.randint(1, NUM_RECORDS)
    weather_id = random.randint(1, NUM_RECORDS)
    soil_id = random.randint(1, NUM_RECORDS)
    avg_temp = round(random.uniform(10, 40), 1)
    total_rain = round(random.uniform(0, 100), 1)
    moisture = round(random.uniform(20, 80), 1)
    yield_pred = round(random.uniform(1000, 5000), 1)
    accuracy = round(random.uniform(70, 95), 1)
    fact_data.append((time_id, loc_id, crop_id, weather_id, soil_id, avg_temp, total_rain, moisture, yield_pred, accuracy))
columns_fact = ['time_id', 'location_id', 'crop_id', 'weather_id', 'soil_id', 'avg_temperature', 'total_rainfall', 'soil_moisture', 'yield_prediction', 'planning_accuracy']
for i in range(0, NUM_RECORDS, BATCH_SIZE):
    batch = fact_data[i:i + BATCH_SIZE]
    insert_batch('Fact_AgriForecast', columns_fact, batch)

# ۷. Fact_CropYield
print("Generating Fact CropYield data...")
yield_data = []
for _ in range(NUM_RECORDS):
    time_id = random.randint(1, NUM_RECORDS)
    loc_id = random.randint(1, NUM_RECORDS)
    crop_id = random.randint(1, NUM_RECORDS)
    actual = round(random.uniform(1000, 6000), 1)
    predicted = round(random.uniform(900, 5500), 1)
    variance = round(abs(actual - predicted), 1)
    yield_data.append((time_id, loc_id, crop_id, actual, predicted, variance))
columns_yield = ['time_id', 'location_id', 'crop_id', 'actual_yield', 'predicted_yield', 'variance']
for i in range(0, NUM_RECORDS, BATCH_SIZE):
    batch = yield_data[i:i + BATCH_SIZE]
    insert_batch('Fact_CropYield', columns_yield, batch)
# ۸. Fact_WeatherEvents (سم شوی – impact_score range نور هم سخت کړم)
print("Generating Fact WeatherEvents data...")
event_data = []
for _ in range(NUM_RECORDS):
    time_id = random.randint(1, NUM_RECORDS)
    loc_id = random.randint(1, NUM_RECORDS)
    weather_id = random.randint(1, NUM_RECORDS)
    event_type = random.choice(['Storm', 'Drought', 'Flood', 'Normal'])
    severity = random.randint(1, 5)
    impact = round(random.uniform(0, 99.9), 1)  # نور سخت: ماکس ۹۹.۹، چې round ته ۹۹.۹ شي
    event_data.append((time_id, loc_id, weather_id, event_type, severity, impact))
columns_event = ['time_id', 'location_id', 'weather_id', 'event_type', 'severity', 'impact_score']
for i in range(0, NUM_RECORDS, BATCH_SIZE):
    batch = event_data[i:i + BATCH_SIZE]
    insert_batch('Fact_WeatherEvents', columns_event, batch)
        
# ۹. Users
print("Generating Users data...")
users_data = [(fake.user_name(), random.choice(['Farmer', 'Admin', 'Analyst']), fake.email(), fake.date_this_decade()) for _ in range(NUM_RECORDS)]
columns_users = ['username', 'role', 'email', 'created_date']
for i in range(0, NUM_RECORDS, BATCH_SIZE):
    batch = users_data[i:i + BATCH_SIZE]
    insert_batch('Users', columns_users, batch)

# ۱۰. AuditLogs
print("Generating AuditLogs data...")
audit_data = [(random.randint(1, NUM_RECORDS), fake.sentence(nb_words=4), fake.date_time_this_decade(), random.choice(['Fact_AgriForecast', 'Users'])) for _ in range(NUM_RECORDS)]
columns_audit = ['user_id', 'action', 'timestamp', 'table_name']
for i in range(0, NUM_RECORDS, BATCH_SIZE):
    batch = audit_data[i:i + BATCH_SIZE]
    insert_batch('AuditLogs', columns_audit, batch)

# ۱۱. Metadata_Crops
print("Generating Metadata_Crops data...")
meta_data = [(random.randint(1, NUM_RECORDS), random.choice(['High Nitrogen', 'Low Phosphorus']), random.choice(['High', 'Medium', 'Low'])) for _ in range(NUM_RECORDS)]
columns_meta = ['crop_id', 'nutrient_level', 'pest_resistance']
for i in range(0, NUM_RECORDS, BATCH_SIZE):
    batch = meta_data[i:i + BATCH_SIZE]
    insert_batch('Metadata_Crops', columns_meta, batch)

# ۱۲. SensorData
print("Generating SensorData data...")
sensor_data = [(random.randint(1, NUM_RECORDS), random.randint(1, NUM_RECORDS), random.choice(['Temperature', 'Humidity', 'Soil']), round(random.uniform(0, 100), 1)) for _ in range(NUM_RECORDS)]
columns_sensor = ['location_id', 'time_id', 'sensor_type', 'reading_value']
for i in range(0, NUM_RECORDS, BATCH_SIZE):
    batch = sensor_data[i:i + BATCH_SIZE]
    insert_batch('SensorData', columns_sensor, batch)

# ۱۳. FarmerProfiles
print("Generating FarmerProfiles data...")
profile_data = [(random.randint(1, NUM_RECORDS), fake.name(), round(random.uniform(1, 100), 1), fake.text(max_nb_chars=200)) for _ in range(NUM_RECORDS)]
columns_profile = ['location_id', 'farmer_name', 'farm_size', 'crops_planted']
for i in range(0, NUM_RECORDS, BATCH_SIZE):
    batch = profile_data[i:i + BATCH_SIZE]
    insert_batch('FarmerProfiles', columns_profile, batch)

# ۱۴. IrrigationLogs
print("Generating IrrigationLogs data...")
irr_data = [(random.randint(1, NUM_RECORDS), random.randint(1, NUM_RECORDS), random.randint(1, NUM_RECORDS), round(random.uniform(100, 1000), 1), round(random.uniform(70, 95), 1)) for _ in range(NUM_RECORDS)]
columns_irr = ['time_id', 'location_id', 'crop_id', 'water_used', 'efficiency']
for i in range(0, NUM_RECORDS, BATCH_SIZE):
    batch = irr_data[i:i + BATCH_SIZE]
    insert_batch('IrrigationLogs', columns_irr, batch)

# ۱۵. PestAlerts
print("Generating PestAlerts data...")
alert_data = [(random.randint(1, NUM_RECORDS), random.randint(1, NUM_RECORDS), random.randint(1, NUM_RECORDS), random.choice(['Aphid', 'Locust', 'Fungus']), random.choice(['High', 'Low'])) for _ in range(NUM_RECORDS)]
columns_alert = ['time_id', 'location_id', 'crop_id', 'pest_type', 'alert_level']
for i in range(0, NUM_RECORDS, BATCH_SIZE):
    batch = alert_data[i:i + BATCH_SIZE]
    insert_batch('PestAlerts', columns_alert, batch)

cursor.close()
conn.close()
print("All 10K records inserted into AgriWeatherDB successfully!")