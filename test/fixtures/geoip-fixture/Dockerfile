FROM ubuntu:18.04

RUN apt-get update -qqy
RUN apt-get install -qqy openjdk-11-jre-headless

ARG fixtureClass
ARG port

ENV GEOIP_FIXTURE_CLASS=${fixtureClass}
ENV GEOIP_FIXTURE_PORT=${port}

ENTRYPOINT exec java -classpath "/fixture/shared/*" \
    $GEOIP_FIXTURE_CLASS 0.0.0.0 "$GEOIP_FIXTURE_PORT"

EXPOSE $port
