import mysql.connector
from faker import Faker
import random
from datetime import datetime, timedelta
import numpy as np

# MySQL تړل
config = {
    'user': 'root',
    'password': 'Moh123',  # ستاسو اصلي پاس ورډ واچوئ
    'host': 'localhost',
    'database': 'HealthWeatherDB'
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
    location_data.append((loc, province, country, lat, lon, pop_density))
columns_loc = ['city_name', 'province', 'country', 'latitude', 'longitude', 'population_density']
for i in range(0, NUM_RECORDS, BATCH_SIZE):
    batch = location_data[i:i + BATCH_SIZE]
    insert_batch('Location_Dim', columns_loc, batch)

# ۳. AirQuality_Dim
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

# ۵. HealthCondition_Dim
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

# ۶. Demographic_Dim
print("Generating Demographic data...")
demo_data = []
for _ in range(NUM_RECORDS):
    age_group = random.choice(['0-18', '19-40', '41-60', '60+'])
    gender = random.choice(['Male', 'Female', 'Other'])
    vuln_level = random.choice(['High', 'Medium', 'Low'])
    demo_data.append((age_group, gender, vuln_level))
columns_demo = ['age_group', 'gender', 'vulnerability_level']
for i in range(0, NUM_RECORDS, BATCH_SIZE):
    batch = demo_data[i:i + BATCH_SIZE]
    insert_batch('Demographic_Dim', columns_demo, batch)

# ۷. Fact_HealthRisk
print("Generating Fact HealthRisk data...")
fact_data = []
for _ in range(NUM_RECORDS):
    time_id = random.randint(1, NUM_RECORDS)
    loc_id = random.randint(1, NUM_RECORDS)
    air_id = random.randint(1, NUM_RECORDS)
    weather_id = random.randint(1, NUM_RECORDS)
    health_id = random.randint(1, NUM_RECORDS)
    demo_id = random.randint(1, NUM_RECORDS)
    aqi = round(random.uniform(0, 500), 1)
    poll_level = round(random.uniform(0, 100), 1)
    cases = random.randint(1, 1000)
    hum = round(random.uniform(30, 90), 1)
    risk_score = round(random.uniform(0, 100), 1)
    fact_data.append((time_id, loc_id, air_id, weather_id, health_id, demo_id, aqi, poll_level, cases, hum, risk_score))
columns_fact = ['time_id', 'location_id', 'air_id', 'weather_id', 'health_id', 'demo_id', 'aqi_value', 'pollution_level', 'reported_cases', 'avg_humidity', 'health_risk_score']
for i in range(0, NUM_RECORDS, BATCH_SIZE):
    batch = fact_data[i:i + BATCH_SIZE]
    insert_batch('Fact_HealthRisk', columns_fact, batch)

# ۸. Fact_IllnessCases
print("Generating Fact IllnessCases data...")
illness_data = []
for _ in range(NUM_RECORDS):
    time_id = random.randint(1, NUM_RECORDS)
    loc_id = random.randint(1, NUM_RECORDS)
    health_id = random.randint(1, NUM_RECORDS)
    demo_id = random.randint(1, NUM_RECORDS)
    case_count = random.randint(1, 500)
    recovery = round(random.uniform(70, 95), 1)
    mortality = round(random.uniform(0, 5), 1)
    illness_data.append((time_id, loc_id, health_id, demo_id, case_count, recovery, mortality))
columns_illness = ['time_id', 'location_id', 'health_id', 'demo_id', 'case_count', 'recovery_rate', 'mortality_rate']
for i in range(0, NUM_RECORDS, BATCH_SIZE):
    batch = illness_data[i:i + BATCH_SIZE]
    insert_batch('Fact_IllnessCases', columns_illness, batch)

# ۹. Fact_PollutionEvents (سم شوی – impact_score range نور هم سخت کړم)
print("Generating Fact PollutionEvents data...")
poll_event_data = []
for _ in range(NUM_RECORDS):
    time_id = random.randint(1, NUM_RECORDS)
    loc_id = random.randint(1, NUM_RECORDS)
    air_id = random.randint(1, NUM_RECORDS)
    event_type = random.choice(['Spike', 'Normal', 'Alert'])
    severity = random.randint(1, 5)
    impact = round(random.uniform(0, 99.9), 1)  # نور سخت: ماکس ۹۹.۹، چې round ته ۹۹.۹ شي
    poll_event_data.append((time_id, loc_id, air_id, event_type, severity, impact))
columns_poll = ['time_id', 'location_id', 'air_id', 'event_type', 'severity', 'impact_score']
for i in range(0, NUM_RECORDS, BATCH_SIZE):
    batch = poll_event_data[i:i + BATCH_SIZE]
    insert_batch('Fact_PollutionEvents', columns_poll, batch)
    
    
# ۱۰. Users
print("Generating Users data...")
users_data = [(fake.user_name(), random.choice(['Doctor', 'Patient', 'Admin']), fake.email(), fake.date_this_decade()) for _ in range(NUM_RECORDS)]
columns_users = ['username', 'role', 'email', 'created_date']
for i in range(0, NUM_RECORDS, BATCH_SIZE):
    batch = users_data[i:i + BATCH_SIZE]
    insert_batch('Users', columns_users, batch)

# ۱۱. AuditLogs
print("Generating AuditLogs data...")
audit_data = [(random.randint(1, NUM_RECORDS), fake.sentence(nb_words=4), fake.date_time_this_decade(), random.choice(['Fact_HealthRisk', 'Users'])) for _ in range(NUM_RECORDS)]
columns_audit = ['user_id', 'action', 'timestamp', 'table_name']
for i in range(0, NUM_RECORDS, BATCH_SIZE):
    batch = audit_data[i:i + BATCH_SIZE]
    insert_batch('AuditLogs', columns_audit, batch)

# ۱۲. Metadata_Health
print("Generating Metadata_Health data...")
meta_data = [(random.randint(1, NUM_RECORDS), random.choice(['Mask', 'Vaccine', 'Isolation']), random.choice(['Yes', 'No'])) for _ in range(NUM_RECORDS)]
columns_meta = ['health_id', 'prevention_measure', 'vaccine_availability']
for i in range(0, NUM_RECORDS, BATCH_SIZE):
    batch = meta_data[i:i + BATCH_SIZE]
    insert_batch('Metadata_Health', columns_meta, batch)

# ۱۳. PatientRecords
print("Generating PatientRecords data...")
patient_data = [(random.randint(1, NUM_RECORDS), random.randint(1, NUM_RECORDS), random.randint(1, NUM_RECORDS), fake.word(), round(random.uniform(100, 10000), 1)) for _ in range(NUM_RECORDS)]
columns_patient = ['location_id', 'time_id', 'demo_id', 'diagnosis', 'treatment_cost']
for i in range(0, NUM_RECORDS, BATCH_SIZE):
    batch = patient_data[i:i + BATCH_SIZE]
    insert_batch('PatientRecords', columns_patient, batch)

# ۱۴. PollutionLogs
print("Generating PollutionLogs data...")
poll_log_data = [(random.randint(1, NUM_RECORDS), random.randint(1, NUM_RECORDS), random.randint(1, NUM_RECORDS), round(random.uniform(300, 1000), 1), round(random.uniform(10, 100), 1), fake.word()) for _ in range(NUM_RECORDS)]
columns_poll_log = ['time_id', 'location_id', 'air_id', 'co2_level', 'no2_level', 'efficiency_measure']
for i in range(0, NUM_RECORDS, BATCH_SIZE):
    batch = poll_log_data[i:i + BATCH_SIZE]
    insert_batch('PollutionLogs', columns_poll_log, batch)

# ۱۵. HealthAlerts
print("Generating HealthAlerts data...")
alert_data = [(random.randint(1, NUM_RECORDS), random.randint(1, NUM_RECORDS), random.randint(1, NUM_RECORDS), random.choice(['Warning', 'Emergency']), random.choice(['High', 'Low'])) for _ in range(NUM_RECORDS)]
columns_alert = ['time_id', 'location_id', 'health_id', 'alert_type', 'alert_level']
for i in range(0, NUM_RECORDS, BATCH_SIZE):
    batch = alert_data[i:i + BATCH_SIZE]
    insert_batch('HealthAlerts', columns_alert, batch)

cursor.close()
conn.close()
print("All 10K records inserted into HealthWeatherDB successfully!")