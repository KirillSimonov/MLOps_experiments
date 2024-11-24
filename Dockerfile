FROM apache/airflow:2.10.3-python3.10

WORKDIR /app

COPY requirements.txt /app/requirements.txt

RUN pip install apache-airflow-providers-amazon
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
