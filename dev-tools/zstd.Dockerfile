FROM centos:7
ARG ZSTD_VERSION

RUN yum install -y git gcc gcc-c++ make
RUN git clone --depth 1 --branch v${ZSTD_VERSION} https://github.com/facebook/zstd.git
WORKDIR zstd
RUN make lib

ENV ZSTD_VERSION=${ZSTD_VERSION}

CMD cat lib/libzstd.so.${ZSTD_VERSION}
