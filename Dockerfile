FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN groupadd -r compactor && useradd -m -g compactor compactor

WORKDIR /app

USER compactor

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

RUN python -c "import duckdb; con = duckdb.connect(); con.execute('INSTALL httpfs; INSTALL aws;')"

COPY compactor.py /app/compactor.py

CMD ["python", "compactor.py"]
