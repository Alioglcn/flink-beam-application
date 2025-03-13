FROM python:3.9

WORKDIR /app

# Gerekli Python paketlerini yükle
RUN pip install kafka-python

COPY producer.py /app/producer.py

CMD ["python", "/app/producer.py"]
