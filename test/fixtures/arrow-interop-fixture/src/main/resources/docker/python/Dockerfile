FROM python:3.13-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY fetch_arrow.py .
RUN useradd --create-home arrow
USER arrow

ENTRYPOINT ["python", "/app/fetch_arrow.py"]
