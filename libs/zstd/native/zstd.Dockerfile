FROM redhat/ubi9
ARG ZSTD_VERSION

RUN yum install -y git gcc-toolset-14-gcc gcc-toolset-14-binutils make
RUN git clone --depth 1 --branch v${ZSTD_VERSION} https://github.com/facebook/zstd.git
WORKDIR zstd
RUN source /opt/rh/gcc-toolset-14/enable && \
    CC=gcc CFLAGS="-O3 -flto" LDFLAGS="-flto" \
    make -C lib lib-release-nomt && \
    strip --strip-unneeded lib/libzstd.so.${ZSTD_VERSION}

ENV ZSTD_VERSION=${ZSTD_VERSION}

CMD cat lib/libzstd.so.${ZSTD_VERSION}
