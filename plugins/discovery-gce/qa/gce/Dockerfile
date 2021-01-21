FROM ubuntu:18.04

RUN apt-get update -qqy
RUN apt-get install -qqy openjdk-11-jre-headless

ARG port
ARG workingDir
ARG configDir

ENV GCE_FIXTURE_PORT=${port}
ENV GCE_FIXTURE_WORKING_DIR=${workingDir}
ENV GCE_FIXTURE_CONFIG_DIR=${configDir}

ENTRYPOINT exec java -classpath "/fixture/shared/*" \
    org.elasticsearch.cloud.gce.GCEFixture "$URL_FIXTURE_WORKING_DIR" "$GCE_FIXTURE_CONFIG_DIR"

EXPOSE $port