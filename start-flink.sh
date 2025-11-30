#!/bin/bash

# Ожидаем доступности Kafka
echo "Ожидание доступности Kafka..."
while ! nc -z kafka 9092; do
  sleep 1
done

echo "Kafka доступна, запуск Flink..."

# Запускаем Flink JobManager или TaskManager в зависимости от конфигурации
if [ "$1" = "jobmanager" ]; then
    echo "Запуск Flink JobManager..."
    /opt/flink/bin/jobmanager.sh start-foreground
elif [ "$1" = "taskmanager" ]; then
    echo "Запуск Flink TaskManager..."
    /opt/flink/bin/taskmanager.sh start-foreground
else
    echo "Запуск Python приложения..."
    python /app/scr/main.py
fi