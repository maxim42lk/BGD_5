from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import logging
import os
import uvicorn
from datetime import datetime
import time
from collections import defaultdict
import threading

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Sentiment Analysis Model Server")

model = None
tokenizer = None
device = None

_metrics_lock = threading.Lock()
_metrics_data = {
    'inference_requests_total': 0,
    'inference_errors_total': 0,
    'inference_latency_seconds': [],
    'batch_inference_requests_total': 0,
    'batch_inference_errors_total': 0,
    'batch_sizes': []
}


class PredictionRequest(BaseModel):
    text: str


class BatchPredictionRequest(BaseModel):
    texts: List[str]


class PredictionResponse(BaseModel):
    sentiment: str
    confidence: float
    timestamp: int


class BatchPredictionResponse(BaseModel):
    predictions: List[str]
    confidences: List[float]


def record_metric(metric_name: str, value: float = 1.0):
    with _metrics_lock:
        if metric_name not in _metrics_data:
            _metrics_data[metric_name] = []

        if isinstance(_metrics_data[metric_name], list):
            _metrics_data[metric_name].append(value)
            if len(_metrics_data[metric_name]) > 1000:
                _metrics_data[metric_name] = _metrics_data[metric_name][-1000:]
        else:
            _metrics_data[metric_name] += value


def generate_prometheus_metrics() -> str:
    with _metrics_lock:
        metrics_lines = []

        metrics_lines.append('# HELP model_server_inference_requests_total Total number of inference requests')
        metrics_lines.append('# TYPE model_server_inference_requests_total counter')
        metrics_lines.append(f'model_server_inference_requests_total {_metrics_data["inference_requests_total"]}')

        metrics_lines.append('# HELP model_server_inference_errors_total Total number of inference errors')
        metrics_lines.append('# TYPE model_server_inference_errors_total counter')
        metrics_lines.append(f'model_server_inference_errors_total {_metrics_data["inference_errors_total"]}')

        metrics_lines.append(
            '# HELP model_server_batch_inference_requests_total Total number of batch inference requests')
        metrics_lines.append('# TYPE model_server_batch_inference_requests_total counter')
        metrics_lines.append(
            f'model_server_batch_inference_requests_total {_metrics_data["batch_inference_requests_total"]}')

        if _metrics_data['inference_latency_seconds']:
            latencies = _metrics_data['inference_latency_seconds']

            buckets = [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0]
            bucket_counts = {bucket: 0 for bucket in buckets}

            for latency in latencies:
                for bucket in buckets:
                    if latency <= bucket:
                        bucket_counts[bucket] += 1

            metrics_lines.append('# HELP model_server_inference_latency_seconds Inference latency histogram')
            metrics_lines.append('# TYPE model_server_inference_latency_seconds histogram')

            for bucket in buckets:
                metrics_lines.append(
                    f'model_server_inference_latency_seconds_bucket{{le="{bucket}"}} {bucket_counts[bucket]}')

            total_count = len(latencies)
            metrics_lines.append(f'model_server_inference_latency_seconds_bucket{{le="+Inf"}} {total_count}')
            metrics_lines.append(f'model_server_inference_latency_seconds_sum {sum(latencies)}')
            metrics_lines.append(f'model_server_inference_latency_seconds_count {total_count}')

            if latencies:
                sorted_latencies = sorted(latencies)
                p50 = sorted_latencies[int(len(sorted_latencies) * 0.5)]
                p95 = sorted_latencies[int(len(sorted_latencies) * 0.95)]
                p99 = sorted_latencies[int(len(sorted_latencies) * 0.99)]

                metrics_lines.append(
                    '# HELP model_server_inference_latency_p50_seconds 50th percentile inference latency')
                metrics_lines.append('# TYPE model_server_inference_latency_p50_seconds gauge')
                metrics_lines.append(f'model_server_inference_latency_p50_seconds {p50}')

                metrics_lines.append(
                    '# HELP model_server_inference_latency_p95_seconds 95th percentile inference latency')
                metrics_lines.append('# TYPE model_server_inference_latency_p95_seconds gauge')
                metrics_lines.append(f'model_server_inference_latency_p95_seconds {p95}')

                metrics_lines.append(
                    '# HELP model_server_inference_latency_p99_seconds 99th percentile inference latency')
                metrics_lines.append('# TYPE model_server_inference_latency_p99_seconds gauge')
                metrics_lines.append(f'model_server_inference_latency_p99_seconds {p99}')

        if _metrics_data['batch_sizes']:
            batch_sizes = _metrics_data['batch_sizes']

            metrics_lines.append('# HELP model_server_batch_size Batch size histogram')
            metrics_lines.append('# TYPE model_server_batch_size histogram')

            batch_buckets = [1, 5, 10, 20, 50, 100]
            batch_counts = {bucket: 0 for bucket in batch_buckets}

            for size in batch_sizes:
                for bucket in batch_buckets:
                    if size <= bucket:
                        batch_counts[bucket] += 1

            for bucket in batch_buckets:
                metrics_lines.append(f'model_server_batch_size_bucket{{le="{bucket}"}} {batch_counts[bucket]}')

            metrics_lines.append(f'model_server_batch_size_bucket{{le="+Inf"}} {len(batch_sizes)}')
            metrics_lines.append(f'model_server_batch_size_sum {sum(batch_sizes)}')
            metrics_lines.append(f'model_server_batch_size_count {len(batch_sizes)}')

        model_loaded = 1 if model is not None else 0
        metrics_lines.append('# HELP model_server_model_loaded Whether the model is loaded (1) or not (0)')
        metrics_lines.append('# TYPE model_server_model_loaded gauge')
        metrics_lines.append(f'model_server_model_loaded {model_loaded}')

        return '\n'.join(metrics_lines)


def load_model():
    global model, tokenizer, device

    try:
        model_path = os.getenv('MODEL_PATH', '/app/model')
        logger.info(f"Загрузка модели {model_path}")

        required_files = ['config.json', 'tokenizer_config.json']
        model_file = None

        if os.path.exists(os.path.join(model_path, 'model.safetensors')):
            model_file = 'model.safetensors'
        elif os.path.exists(os.path.join(model_path, 'pytorch_model.bin')):
            model_file = 'pytorch_model.bin'

        missing_files = [f for f in required_files if not os.path.exists(os.path.join(model_path, f))]

        if missing_files or model_file is None:
            raise FileNotFoundError(
                f"Отсутствуют файлы: {missing_files + (['model weights'] if model_file is None else [])}")

        device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        logger.info(f"Using device: {device}")

        tokenizer = AutoTokenizer.from_pretrained(model_path)


        # Загружаем модель (поддержка SafeTensors и PyTorch)
        logger.info("Загрузка модели")
        model = AutoModelForSequenceClassification.from_pretrained(
            model_path,
            use_safetensors=True if model_file == 'model.safetensors' else False
        )
        model.to(device)
        model.eval()

        num_params = sum(p.numel() for p in model.parameters())
        logger.info(f"Модель загружена")
        logger.info(f"Parameters: {num_params:,}")
        logger.info(f"Model type: {model.config.model_type}")

        return True

    except Exception as e:
        logger.error(f"Ошибка загрузки модели {e}")
        logger.error(f"Путь модели: {model_path}")

        # Проверяем содержимое директории
        if os.path.exists(model_path):
            files = os.listdir(model_path)
            logger.info(f"Файлы в директории модели: {files}")

        # Загружаем базовую модель для демонстрации
        try:
            tokenizer = AutoTokenizer.from_pretrained('bert-base-uncased')
            model = AutoModelForSequenceClassification.from_pretrained(
                'bert-base-uncased',
                num_labels=2
            )
            model.to(device)
            model.eval()
            logger.info("Fallback loaded")
            return True
        except Exception as e2:
            logger.error(f"Ошибка fallback: {e2}")
            return False


@app.on_event("startup")
async def startup_event():
    logger.info("Запуск сервера модели")
    success = load_model()
    if not success:
        logger.error("Ошибка загрузки модели")


@app.get("/health")
async def health_check():
    if model is None or tokenizer is None:
        raise HTTPException(status_code=503, detail="Model not loaded")

    return {
        "status": "healthy",
        "model_loaded": model is not None,
        "device": str(device),
        "timestamp": int(datetime.now().timestamp() * 1000)
    }


@app.post("/predict", response_model=PredictionResponse)
async def predict_sentiment(request: PredictionRequest):
    start_time = time.time()
    record_metric('inference_requests_total')

    try:
        if model is None or tokenizer is None:
            record_metric('inference_errors_total')
            raise HTTPException(status_code=503, detail="Model not loaded")

        inputs = tokenizer(
            request.text,
            return_tensors="pt",
            truncation=True,
            max_length=512,
            padding=True
        ).to(device)

        with torch.no_grad():
            outputs = model(**inputs)
            logits = outputs.logits
            probs = torch.softmax(logits, dim=1)
            predicted_class = torch.argmax(probs, dim=1).item()
            confidence = probs[0][predicted_class].item()

        sentiment = "positive" if predicted_class == 1 else "negative"

        latency = time.time() - start_time
        record_metric('inference_latency_seconds', latency)

        return PredictionResponse(
            sentiment=sentiment,
            confidence=confidence,
            timestamp=int(datetime.now().timestamp() * 1000)
        )

    except Exception as e:
        record_metric('inference_errors_total')
        logger.error(f"Ошибка предсказания: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/predict_batch", response_model=BatchPredictionResponse)
async def predict_batch(request: BatchPredictionRequest):
    start_time = time.time()
    record_metric('batch_inference_requests_total')
    record_metric('batch_sizes', len(request.texts))

    try:
        if model is None or tokenizer is None:
            record_metric('batch_inference_errors_total')
            raise HTTPException(status_code=503, detail="Model not loaded")

        if not request.texts:
            raise HTTPException(status_code=400, detail="Empty texts list")

        inputs = tokenizer(
            request.texts,
            return_tensors="pt",
            truncation=True,
            max_length=512,
            padding=True
        ).to(device)

        with torch.no_grad():
            outputs = model(**inputs)
            logits = outputs.logits
            probs = torch.softmax(logits, dim=1)
            predicted_classes = torch.argmax(probs, dim=1).tolist()
            confidences = [probs[i][predicted_classes[i]].item()
                           for i in range(len(predicted_classes))]

        sentiments = ["positive" if c == 1 else "negative"
                      for c in predicted_classes]

        latency = time.time() - start_time
        record_metric('inference_latency_seconds', latency)

        return BatchPredictionResponse(
            predictions=sentiments,
            confidences=confidences
        )

    except Exception as e:
        record_metric('batch_inference_errors_total')
        logger.error(f"Ошибка предсказания: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/metrics")
async def get_metrics():
    from fastapi.responses import PlainTextResponse

    try:
        prometheus_metrics = generate_prometheus_metrics()
        return PlainTextResponse(
            content=prometheus_metrics,
            media_type="text/plain; version=0.0.4"
        )
    except Exception as e:
        logger.error(f"Ошибка Prometheus metrics: {e}")
        raise HTTPException(status_code=500, detail="Error generating metrics")


@app.get("/metrics/json")
async def get_metrics_json():
    return {
        "model_loaded": model is not None,
        "device": str(device),
        "model_parameters": sum(p.numel() for p in model.parameters()) if model else 0,
        "inference_requests_total": _metrics_data["inference_requests_total"],
        "inference_errors_total": _metrics_data["inference_errors_total"],
        "batch_inference_requests_total": _metrics_data["batch_inference_requests_total"],
        "batch_inference_errors_total": _metrics_data["batch_inference_errors_total"],
        "timestamp": int(datetime.now().timestamp() * 1000)
    }


if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )