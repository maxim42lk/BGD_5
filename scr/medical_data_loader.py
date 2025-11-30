import pandas as pd
import json
import time
from kafka import KafkaProducer
import logging
import os
from typing import List, Dict, Any
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MedicalDataLoader:
    def __init__(self, kafka_bootstrap_servers='kafka:9092'):
        self.producer = None
        self.dataset_path = '/app/data/hospital_readmissions_30k.csv'

        try:
            self.producer = KafkaProducer(
                bootstrap_servers=kafka_bootstrap_servers,
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                request_timeout_ms=5000,
                retries=3
            )
            logger.info("Kafka producer для медицинских данных создан")
        except Exception as e:
            logger.error(f"Ошибка создания Kafka producer: {e}")
            raise

    def load_medical_dataset(self):
        try:
            if not os.path.exists(self.dataset_path):
                logger.error(f"Медицинский датасет не найден: {self.dataset_path}")
                return None

            data = pd.read_csv(self.dataset_path)
            data = data.drop_duplicates()
            data = data.dropna()
            
            logger.info(f"Медицинский датасет загружен: {len(data)} записей")
            return data

        except Exception as e:
            logger.error(f"Ошибка загрузки медицинского датасета: {e}")
            return None

    def generate_medical_stream(self, records_per_second: int = 50, duration_minutes: int = 10):
        data = self.load_medical_dataset()
        if data is None:
            logger.error("Не удалось загрузить медицинский датасет")
            return

        logger.info(f"Запуск медицинского потока: {records_per_second} записей/сек, {duration_minutes} минут")

        total_records = records_per_second * 60 * duration_minutes
        used_indices = set()

        start_time = time.time()
        end_time = start_time + (duration_minutes * 60)
        record_count = 0

        while time.time() < end_time and record_count < total_records:
            batch_start = time.time()

            for _ in range(records_per_second):
                if record_count >= total_records:
                    break

                available_indices = set(range(len(data))) - used_indices
                if not available_indices:
                    used_indices = set()
                    available_indices = set(range(len(data)))

                idx = random.choice(list(available_indices))
                used_indices.add(idx)

                medical_record = data.iloc[idx]

                # Преобразуем данные в формат для обработки
                record_data = {
                    'id': f"medical_record_{medical_record['patient_id']}_{int(time.time() * 1000)}",
                    'patient_id': int(medical_record['patient_id']),
                    'age': int(medical_record['age']),
                    'gender': str(medical_record['gender']),
                    'systolic_bp': int(medical_record['blood_pressure'].split('/')[0]),
                    'diastolic_bp': int(medical_record['blood_pressure'].split('/')[1]),
                    'cholesterol': int(medical_record['cholesterol']),
                    'bmi': float(medical_record['bmi']),
                    'diabetes': 1 if medical_record['diabetes'] == 'Yes' else 0,
                    'hypertension': 1 if medical_record['hypertension'] == 'Yes' else 0,
                    'medication_count': int(medical_record['medication_count']),
                    'length_of_stay': int(medical_record['length_of_stay']),
                    'discharge_destination': str(medical_record['discharge_destination']),
                    'readmission_risk_raw': 1.0 if medical_record['readmitted_30_days'] == 'Yes' else 0.0,
                    'timestamp': int(time.time() * 1000)
                }

                self.producer.send('medical-records', record_data)
                record_count += 1

            batch_time = time.time() - batch_start
            if batch_time < 1.0:
                time.sleep(1.0 - batch_time)

            if record_count % 1000 == 0:
                logger.info(f"Отправлено медицинских записей: {record_count}")

        self.producer.flush()
        logger.info(f"Медицинский поток завершен. Всего отправлено: {record_count} записей")

    def send_specific_test_records(self, count: int = 1000):
        data = self.load_medical_dataset()
        if data is None:
            return

        logger.info(f"Отправка {count} тестовых медицинских записей для валидации")

        readmitted_records = data[data['readmitted_30_days'] == 'Yes'].head(count // 2)
        not_readmitted_records = data[data['readmitted_30_days'] == 'No'].head(count // 2)

        test_data = pd.concat([readmitted_records, not_readmitted_records])

        for idx, medical_record in test_data.iterrows():
            record_data = {
                'id': f"test_medical_{medical_record['patient_id']}",
                'patient_id': int(medical_record['patient_id']),
                'age': int(medical_record['age']),
                'gender': str(medical_record['gender']),
                'systolic_bp': int(medical_record['blood_pressure'].split('/')[0]),
                'diastolic_bp': int(medical_record['blood_pressure'].split('/')[1]),
                'cholesterol': int(medical_record['cholesterol']),
                'bmi': float(medical_record['bmi']),
                'diabetes': 1 if medical_record['diabetes'] == 'Yes' else 0,
                'hypertension': 1 if medical_record['hypertension'] == 'Yes' else 0,
                'medication_count': int(medical_record['medication_count']),
                'length_of_stay': int(medical_record['length_of_stay']),
                'discharge_destination': str(medical_record['discharge_destination']),
                'readmission_risk_raw': 1.0 if medical_record['readmitted_30_days'] == 'Yes' else 0.0,
                'timestamp': int(time.time() * 1000),
                'is_test': True
            }

            self.producer.send('medical-records', record_data)
            time.sleep(0.01)

        self.producer.flush()
        logger.info(f"Тестовые медицинские записи отправлены: {len(test_data)} записей")

if __name__ == "__main__":
    loader = MedicalDataLoader()
    
    # Запускаем поток медицинских данных
    loader.generate_medical_stream(records_per_second=50, duration_minutes=5)