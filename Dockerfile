FROM python:3.8-slim
LABEL maintainer="[https://github.com/sesam-community | sesam-community]"
RUN pip3 install --upgrade pip
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
COPY ./service/service.py /service/
WORKDIR /service
EXPOSE 5000/tcp
ENTRYPOINT ["python"]
CMD ["service.py"]
