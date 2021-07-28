FROM minio/minio:RELEASE.2021-03-01T04-20-55Z

ARG bucket
ARG accessKey
ARG secretKey

RUN mkdir -p /minio/data/${bucket}
ENV MINIO_ACCESS_KEY=${accessKey}
ENV MINIO_SECRET_KEY=${secretKey}
