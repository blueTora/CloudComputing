FROM python:3.11.0a6-alpine3.15

LABEL maintainer="Negar Karami  <n.karami@aut.ac.ir>"

WORKDIR .

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

CMD python main.py