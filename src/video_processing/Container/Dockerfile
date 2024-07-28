FROM python:2

WORKDIR /src/video_processing

COPY ../../requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY ../../Main.py .
EXPOSE 8000

CMD [ "python", "Main.py" ]
