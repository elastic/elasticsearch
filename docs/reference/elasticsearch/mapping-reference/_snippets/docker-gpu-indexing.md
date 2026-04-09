```plaintext
FROM docker.elastic.co/elasticsearch/elasticsearch:9.3.0

USER root

# See https://gitlab.com/nvidia/container-images/cuda/-/blob/master/dist/12.9.1/ubi9/base/Dockerfile?ref_type=heads
# and https://gitlab.com/nvidia/container-images/cuda/-/blob/master/dist/12.9.1/ubi9/devel/Dockerfile?ref_type=heads
# We are installing nvidia/cuda drivers/libraries the same way that nvidia does in their images

ENV CUVS_VERSION=25.12.0

ENV NVARCH=x86_64
ENV NVIDIA_REQUIRE_CUDA="cuda>=12.9 brand=unknown,driver>=535,driver<536 brand=grid,driver>=535,driver<536 brand=tesla,driver>=535,driver<536 brand=nvidia,driver>=535,driver<536 brand=quadro,driver>=535,driver<536 brand=quadrortx,driver>=535,driver<536 brand=nvidiartx,driver>=535,driver<536 brand=vapps,driver>=535,driver<536 brand=vpc,driver>=535,driver<536 brand=vcs,driver>=535,driver<536 brand=vws,driver>=535,driver<536 brand=cloudgaming,driver>=535,driver<536 brand=unknown,driver>=550,driver<551 brand=grid,driver>=550,driver<551 brand=tesla,driver>=550,driver<551 brand=nvidia,driver>=550,driver<551 brand=quadro,driver>=550,driver<551 brand=quadrortx,driver>=550,driver<551 brand=nvidiartx,driver>=550,driver<551 brand=vapps,driver>=550,driver<551 brand=vpc,driver>=550,driver<551 brand=vcs,driver>=550,driver<551 brand=vws,driver>=550,driver<551 brand=cloudgaming,driver>=550,driver<551 brand=unknown,driver>=560,driver<561 brand=grid,driver>=560,driver<561 brand=tesla,driver>=560,driver<561 brand=nvidia,driver>=560,driver<561 brand=quadro,driver>=560,driver<561 brand=quadrortx,driver>=560,driver<561 brand=nvidiartx,driver>=560,driver<561 brand=vapps,driver>=560,driver<561 brand=vpc,driver>=560,driver<561 brand=vcs,driver>=560,driver<561 brand=vws,driver>=560,driver<561 brand=cloudgaming,driver>=560,driver<561 brand=unknown,driver>=565,driver<566 brand=grid,driver>=565,driver<566 brand=tesla,driver>=565,driver<566 brand=nvidia,driver>=565,driver<566 brand=quadro,driver>=565,driver<566 brand=quadrortx,driver>=565,driver<566 brand=nvidiartx,driver>=565,driver<566 brand=vapps,driver>=565,driver<566 brand=vpc,driver>=565,driver<566 brand=vcs,driver>=565,driver<566 brand=vws,driver>=565,driver<566 brand=cloudgaming,driver>=565,driver<566 brand=unknown,driver>=570,driver<571 brand=grid,driver>=570,driver<571 brand=tesla,driver>=570,driver<571 brand=nvidia,driver>=570,driver<571 brand=quadro,driver>=570,driver<571 brand=quadrortx,driver>=570,driver<571 brand=nvidiartx,driver>=570,driver<571 brand=vapps,driver>=570,driver<571 brand=vpc,driver>=570,driver<571 brand=vcs,driver>=570,driver<571 brand=vws,driver>=570,driver<571 brand=cloudgaming,driver>=570,driver<571"
ENV NV_CUDA_CUDART_VERSION=12.9.79-1
ENV CUDA_VERSION=12.9.1

ENV NV_CUDA_LIB_VERSION=12.9.1-1
ENV NV_NVPROF_VERSION=12.9.79-1
ENV NV_NVPROF_DEV_PACKAGE=cuda-nvprof-12-9-${NV_NVPROF_VERSION}
ENV NV_CUDA_CUDART_DEV_VERSION=12.9.79-1
ENV NV_NVML_DEV_VERSION=12.9.79-1
ENV NV_LIBCUBLAS_DEV_VERSION=12.9.1.4-1
ENV NV_LIBNPP_DEV_VERSION=12.4.1.87-1
ENV NV_LIBNPP_DEV_PACKAGE=libnpp-devel-12-9-${NV_LIBNPP_DEV_VERSION}
ENV NV_LIBNCCL_DEV_PACKAGE_NAME=libnccl-devel
ENV NV_LIBNCCL_DEV_PACKAGE_VERSION=2.27.3-1
ENV NCCL_VERSION=2.27.3
ENV NV_LIBNCCL_DEV_PACKAGE=${NV_LIBNCCL_DEV_PACKAGE_NAME}-${NV_LIBNCCL_DEV_PACKAGE_VERSION}+cuda12.9
ENV NV_CUDA_NSIGHT_COMPUTE_VERSION=12.9.1-1
ENV NV_CUDA_NSIGHT_COMPUTE_DEV_PACKAGE=cuda-nsight-compute-12-9-${NV_CUDA_NSIGHT_COMPUTE_VERSION}

ENV NV_NVTX_VERSION=12.9.79-1
ENV NV_LIBNPP_VERSION=12.4.1.87-1
ENV NV_LIBNPP_PACKAGE=libnpp-12-9-${NV_LIBNPP_VERSION}
ENV NV_LIBCUBLAS_VERSION=12.9.1.4-1
ENV NV_LIBNCCL_PACKAGE_NAME=libnccl
ENV NV_LIBNCCL_PACKAGE_VERSION=2.27.3-1
ENV NV_LIBNCCL_VERSION=2.27.3
ENV NCCL_VERSION=2.27.3
ENV NV_LIBNCCL_PACKAGE=${NV_LIBNCCL_PACKAGE_NAME}-${NV_LIBNCCL_PACKAGE_VERSION}+cuda12.9

ENV NVIDIA_VISIBLE_DEVICES=all
ENV NVIDIA_DRIVER_CAPABILITIES=compute,utility
ENV RAFT_DEBUG_LOG_FILE=/dev/null

# Install nvidia drivers
RUN microdnf install -y dnf
RUN dnf install -y 'dnf-command(config-manager)'
RUN dnf config-manager --add-repo https://developer.download.nvidia.com/compute/cuda/repos/rhel9/x86_64/cuda-rhel9.repo

RUN dnf upgrade -y && dnf install -y \
    cuda-cudart-12-9-${NV_CUDA_CUDART_VERSION} \
    cuda-compat-12-9 \
    && dnf clean all \
    && rm -rf /var/cache/yum/*

# Set up env vars for various libraries (cuda, libcuvs)
RUN echo "/usr/local/cuda/lib64" >> /etc/ld.so.conf.d/nvidia.conf
ENV PATH=/usr/local/nvidia/bin:/usr/local/cuda/bin:${PATH}
ENV LIBCUVS_DIR="/opt/cuvs"
ENV LD_LIBRARY_PATH=${LIBCUVS_DIR}:/usr/local/nvidia/lib:/usr/local/nvidia/lib64:/usr/local/cuda/lib64

# Install other required nvidia and cuda libraries, as well as tar and gzip
RUN dnf install -y \
    cuda-libraries-12-9-${NV_CUDA_LIB_VERSION} \
    cuda-nvtx-12-9-${NV_NVTX_VERSION} \
    ${NV_LIBNPP_PACKAGE} \
    libcublas-12-9-${NV_LIBCUBLAS_VERSION} \
    ${NV_LIBNCCL_PACKAGE} \
    tar gzip \
    && dnf clean all \
    && rm -rf /var/cache/yum/*

# Grab the libcuvs library from Elastic's gcs archive
# These are tarballs that contain only the libraries necessary from nvidia's libcuvs builds in conda
# Note: this is temporary until nvidia begins publishing minimal libcuvs tarballs along with their releases
RUN mkdir -p "$LIBCUVS_DIR" && \
    chmod 775 "$LIBCUVS_DIR" && \
    cd "$LIBCUVS_DIR" && \
    CUVS_ARCHIVE="libcuvs-$CUVS_VERSION.tar.gz" && \
    curl -fO "https://storage.googleapis.com/elasticsearch-cuvs-snapshots/libcuvs/$CUVS_ARCHIVE" && \
    tar -xzf "$CUVS_ARCHIVE" && \
    rm -f "$CUVS_ARCHIVE" && \
    if [[ -d "$CUVS_VERSION" ]]; then mv "$CUVS_VERSION/*" ./; fi

# Reset the user back to elasticsearch
USER 1000:0
```
