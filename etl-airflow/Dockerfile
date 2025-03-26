FROM puckel/docker-airflow:1.10.9
RUN pip install requests
RUN pip install pandas

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt