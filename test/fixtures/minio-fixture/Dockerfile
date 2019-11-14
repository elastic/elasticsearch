FROM minio/minio:RELEASE.2019-01-23T23-18-58Z

ARG bucket
ARG accessKey
ARG secretKey

RUN mkdir -p /minio/data/${bucket}
ENV MINIO_ACCESS_KEY=${accessKey}
ENV MINIO_SECRET_KEY=${secretKey}
