FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    build-essential \
    default-libmysqlclient-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY worker.py .
COPY README.md .
COPY req.txt .

RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r req.txt

CMD ["python", "worker.py"]
