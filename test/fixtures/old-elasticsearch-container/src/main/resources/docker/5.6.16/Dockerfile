FROM eclipse-temurin:8-jdk

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl unzip \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /tmp
RUN curl -fsSL -o elasticsearch.zip https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-5.6.16.zip \
 && unzip elasticsearch.zip \
 && mv elasticsearch-5.6.16 /usr/share/elasticsearch \
 && rm -f elasticsearch.zip

RUN chmod -R +x /usr/share/elasticsearch/bin \
 && echo 'http.host: 0.0.0.0' >> /usr/share/elasticsearch/config/elasticsearch.yml \
 && echo 'transport.host: 127.0.0.1' >> /usr/share/elasticsearch/config/elasticsearch.yml \
 && addgroup --system elasticsearch \
 && adduser --system --ingroup elasticsearch --home /usr/share/elasticsearch --no-create-home elasticsearch \
 && chown -R elasticsearch:elasticsearch /usr/share/elasticsearch

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

USER elasticsearch
WORKDIR /usr/share/elasticsearch
EXPOSE 9200 9300
ENTRYPOINT ["/entrypoint.sh"]
