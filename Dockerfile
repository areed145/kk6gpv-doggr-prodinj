FROM python:3.7-slim-buster

LABEL maintainer="areed145@gmail.com"

WORKDIR /doggr-prodinj

COPY . /doggr-prodinj

# We copy just the requirements.txt first to leverage Docker cache
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# EXPOSE 80

#ENV MONGODB_CLIENT 'mongodb://kk6gpv:kk6gpv@mongo-mongodb-replicaset-0.mongo-mongodb-replicaset.default.svc.cluster.local,mongo-mongodb-replicaset-1.mongo-mongodb-replicaset.default.svc.cluster.local,mongo-mongodb-replicaset-2.mongo-mongodb-replicaset.default.svc.cluster.local/?replicaSet=db'
ENV MONGODB_CLIENT 'mongodb+srv://kk6gpv:kk6gpv@cluster0-kglzh.azure.mongodb.net/test?retryWrites=true&w=majority'

CMD ["python", "doggr-prodinj.py"]
