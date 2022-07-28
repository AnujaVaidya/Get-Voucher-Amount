FROM python:3.8

COPY src/ /code
WORKDIR /code

COPY requirements.txt .

RUN pip install -r requirements.txt
ENV FLASK_APP=main.py

CMD [ "python3", "-m" , "flask", "run", "--host=0.0.0.0"]