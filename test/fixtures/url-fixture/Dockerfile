FROM ubuntu:18.04

RUN apt-get update -qqy
RUN apt-get install -qqy openjdk-11-jre-headless

ARG port
ARG workingDir
ARG repositoryDir

ENV URL_FIXTURE_PORT=${port}
ENV URL_FIXTURE_WORKING_DIR=${workingDir}
ENV URL_FIXTURE_REPO_DIR=${repositoryDir}

ENTRYPOINT exec java -classpath "/fixture/shared/*" \
    fixture.url.URLFixture "$URL_FIXTURE_PORT" "$URL_FIXTURE_WORKING_DIR" "$URL_FIXTURE_REPO_DIR"

EXPOSE $port
