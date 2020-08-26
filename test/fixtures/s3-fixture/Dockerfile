FROM ubuntu:18.04

RUN apt-get update -qqy
RUN apt-get install -qqy openjdk-11-jre-headless

ARG fixtureClass
ARG port
ARG bucket
ARG basePath
ARG accessKey
ARG sessionToken

ENV S3_FIXTURE_CLASS=${fixtureClass}
ENV S3_FIXTURE_PORT=${port}
ENV S3_FIXTURE_BUCKET=${bucket}
ENV S3_FIXTURE_BASE_PATH=${basePath}
ENV S3_FIXTURE_ACCESS_KEY=${accessKey}
ENV S3_FIXTURE_SESSION_TOKEN=${sessionToken}

ENTRYPOINT exec java -classpath "/fixture/shared/*" \
    $S3_FIXTURE_CLASS 0.0.0.0 "$S3_FIXTURE_PORT" "$S3_FIXTURE_BUCKET" "$S3_FIXTURE_BASE_PATH" "$S3_FIXTURE_ACCESS_KEY" "$S3_FIXTURE_SESSION_TOKEN"

EXPOSE $port
