FROM python:3.11

LABEL Maintainer="vishwa.me17"

WORKDIR /app

COPY . /app

# Install requirements
RUN pip install -r requirements.txt
#RUN pip install pandas
#RUN pip install requests
#RUN pip install pandasql
#RUN pip install aiohttp
#RUN pip install matplotlib
CMD [ "python", "./sales_data_pipeline.py"]
