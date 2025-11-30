#!/bin/bash

set -e

echo "Проверка необходимых файлов..."

required_files=(
    "docker-compose.yml"
    "Dockerfile.flink"
    "Dockerfile.model-server"
    "Dockerfile.medical-data-loader"
    "hospital_readmissions_30k.csv"
    "prometheus.yml"
    "requirements-flink.txt"
    "requirements-model-server.txt"
    "requirements-medical.txt"
    "start-flink.sh"
)

missing_files=()
for file in "${required_files[@]}"; do
    if [ ! -f "$file" ]; then
        missing_files+=("$file")
    fi
done

if [ ${#missing_files[@]} -ne 0 ]; then
    echo "Отсутствуют необходимые файлы:"
    for file in "${missing_files[@]}"; do
        echo "   - $file"
    done
    exit 1
fi

echo "Все необходимые файлы присутствуют"
echo ""

# Проверяем JAR файлы
if [ ! -d "lib" ] || [ ! -f "lib/flink-connector-kafka-1.17.1.jar" ]; then
    echo "Внимание: JAR файлы Kafka connector не найдены в директории lib/"
    echo "Система может работать некорректно"
    echo "Рекомендуется выполнить: mkdir lib && cd lib && wget https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/1.17.1/flink-connector-kafka-1.17.1.jar"
    read -p "Продолжить без JAR файлов? (y/n): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

echo "Запуск медицинской аналитической системы..."
echo ""

# Очистка и запуск
docker-compose down 2>/dev/null || true
docker-compose build
docker-compose up -d

echo ""
echo "Система запускается..."
echo "Мониторинг доступен по адресам:"
echo "  - Grafana: http://localhost:3000 (admin/admin)"
echo "  - Flink UI: http://localhost:8081"
echo "  - Prometheus: http://localhost:9090"
echo "  - Model Server: http://localhost:8000"

echo ""
echo "Для просмотра логов выполните: docker-compose logs -f"
echo "Для остановки: docker-compose down"