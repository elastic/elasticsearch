# This file describes how to build ElasticSearch into a runnable linux container with all dependencies installed
# To build:
# 1) Install docker (http://docker.io)
# 2) Build: docker build - < Dockerfile
# 3) Run: docker run -d <imageid> OR docker run -d <imageid> elasticsearch-0.90.1/bin/elasticsearch -f <parameters>
#
# VERSION 0.90.1
# DOCKER-VERSION 0.4.0
#
# start ElasticSearch with `docker run <imageid>` for the default parameters
# OR `docker run <imageid> elasticsearch-0.90.1/bin/elasticsearch -f <your parameters>` if you need to customize

FROM    base:ubuntu-12.10
MAINTAINER Victor Vieux <victor@vvieux.com>

# Install Java 7
RUN apt-get install software-properties-common -y
RUN apt-add-repository ppa:webupd8team/java -y
RUN apt-get update
RUN echo oracle-java7-installer shared/accepted-oracle-license-v1-1 select true | sudo /usr/bin/debconf-set-selections
RUN apt-get install oracle-java7-installer -y

# Download ElasticSearch
RUN apt-get install wget -y
RUN wget https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-0.90.1.tar.gz
RUN tar -xf elasticsearch-0.90.1.tar.gz
RUN rm elasticsearch-0.90.1.tar.gz

# Expose ElasticSearch's port
EXPOSE :9200

# Start ElasticSearch
CMD elasticsearch-0.90.1/bin/elasticsearch -f
