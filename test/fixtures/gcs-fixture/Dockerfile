FROM openjdk:17.0.2

ARG port
ARG bucket
ARG token

ENV GCS_FIXTURE_PORT=${port}
ENV GCS_FIXTURE_BUCKET=${bucket}
ENV GCS_FIXTURE_TOKEN=${token}

ENTRYPOINT exec java -classpath "/fixture/shared/*" \
    fixture.gcs.GoogleCloudStorageHttpFixture 0.0.0.0 "$GCS_FIXTURE_PORT" "$GCS_FIXTURE_BUCKET" "$GCS_FIXTURE_TOKEN"

EXPOSE $port
