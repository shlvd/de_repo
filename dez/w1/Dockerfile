FROM python:3.9.1

RUN pip install pandas sqlalchemy psycopg2 pyarrow

WORKDIR /app
COPY . . 

ENTRYPOINT [ "python", "pipeline.py" ]