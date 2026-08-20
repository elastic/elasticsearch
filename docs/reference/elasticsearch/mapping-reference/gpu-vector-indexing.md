---
applies_to:
  stack: preview 9.3, ga 9.4
navigation_title: "GPU vector indexing"
---

# GPU accelerated vector indexing

{{es}} can use GPU acceleration to significantly speed up the indexing of
dense vectors. GPU indexing is based on the
[Nvidia cuVS library](https://developer.nvidia.com/cuvs) and leverages the
parallel processing capabilities of graphics processing units to accelerate
the construction of HNSW vector search indexes. GPU accelerated vector
indexing is particularly beneficial for large-scale vector datasets and
high-throughput indexing scenarios, freeing up CPU resources for other tasks.

## Requirements

GPU vector indexing requires the following:

* An [Enterprise subscription](https://www.elastic.co/subscriptions)
* A supported NVIDIA GPU (Ampere architecture or better, compute capability
  \>= 8.0) with a minimum 8GB of GPU memory
* GPU driver, CUDA and
  [cuVS runtime libraries](https://docs.rapids.ai/api/cuvs/stable/build/)
  installed on the node. Refer to the
  [Elastic support matrix](https://www.elastic.co/support/matrix) for
  supported CUDA and cuVS versions.
* `LD_LIBRARY_PATH` environment variable configured to include the cuVS
  libraries path and its dependencies (CUDA, rmm, etc.)
* Supported platform: Linux x86_64 only, Java 22 or higher
* Supported dense vector configurations: `hnsw` and `int8_hnsw`; `float`
  element type only


## Configuration

GPU vector indexing is controlled by the
[`vectors.indexing.use_gpu`](/reference/elasticsearch/configuration-reference/node-settings.md#gpu-vector-indexing-settings)
node-level setting.

## Elasticsearch Docker image with GPU support

An example Dockerfile is provided that extends the official {{es}} Docker image
to add the dependencies required for GPU support.

::::{warning}
This Dockerfile serves as an example implementation, and is not fully supported
like our official Docker images.
::::

::::{dropdown} Example Dockerfile
:::{include} _snippets/docker-gpu-indexing.md
:::
::::

::::{note}
The libcuvs version must match the `cuvs-java` version bundled with your
{{es}} release (`modules/gpu/cuvs-java-<version>.jar`): {{es}} 9.5 and later
bundle cuvs-java 26.02 and require libcuvs 26.02 or later (26.04 is the tested
version, as in the Dockerfile above). {{es}} 9.3 and 9.4 bundle cuvs-java 25.12
and require libcuvs 25.12, available from
`https://storage.googleapis.com/elasticsearch-cuvs-snapshots/libcuvs/libcuvs-25.12.0.tar.gz`
(replace the libcuvs download step in the example Dockerfile with a download and extraction of that archive
into `$LIBCUVS_DIR`).
::::

### Requirements

The host machine running the Docker container needs
[NVIDIA Container Toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html)
installed and configured.

### Build it

```sh
docker build -t es-gpu .
```

### Run it

```sh
docker run \
  -p 9200:9200 \
  -p 9300:9300 \
  -e "discovery.type=single-node" \
  -e "xpack.security.enabled=false" \
  -e "xpack.license.self_generated.type=trial" \
  -e "vectors.indexing.use_gpu=true" \
  --user elasticsearch \
  --gpus all \
  --rm -it es-gpu
```

## Monitoring
```{applies_to}
stack: ga 9.3.2
```

Use the `GET _xpack/usage` API to monitor GPU vector indexing status and usage
across all nodes in the cluster:

```console
GET _xpack/usage?filter_path=gpu_vector_indexing
```
% TEST[skip:Requires GPU hardware]

```console-result
{
  "gpu_vector_indexing": {
    "available": true, <1>
    "enabled": true, <2>
    "index_build_count": 30, <3>
    "nodes_with_gpu": 3, <4>
    "nodes": [ <5>
      { "type": "NVIDIA L4", "memory_in_bytes": 24000000000,
        "enabled": true, "index_build_count": 10 },
      { "type": "NVIDIA L4", "memory_in_bytes": 24000000000,
        "enabled": true, "index_build_count": 10 },
      { "type": "NVIDIA A100", "memory_in_bytes": 80000000000,
        "enabled": true, "index_build_count": 10 }
    ]
  }
}
```
1. Whether the current license permits GPU indexing.
2. Whether at least one node has GPU hardware configured and has not disabled it via `vectors.indexing.use_gpu=false`.
3. Total number of GPU index builds across the cluster.
4. Number of data nodes with GPU support.
5. Per-node GPU details including type, memory, enabled status, and build count.

## Troubleshooting
By default, {{es}} uses GPU indexing for supported vector types if a
compatible GPU and required libraries are detected.
Check server logs for messages indicating whether {{es}} has detected a GPU.

If you see a message like the following, a GPU was successfully detected and
GPU indexing will be used:
```
[o.e.x.g.GPUSupport ] [elasticsearch-0] Found compatible GPU [NVIDIA L4] (id: [0])
```
If you don't see this message, look for warning messages explaining why GPU
indexing is not being used, such as an unsupported environment, missing
libraries, or an incompatible GPU.


### GPU indexing is unavailable with a `Version mismatch: outdated libcuvs_c` warning

If the node logs a warning like:

```
GPU based vector indexing is not supported on this platform; Cannot create JDKProvider:
Version mismatch: outdated libcuvs_c (libcuvs_c [25.12.0], cuvs-java version [26.02.0])
```

the libcuvs library on `LD_LIBRARY_PATH` is older than the `cuvs-java`
version bundled with {{es}}. Install a libcuvs version that is at least the
bundled `cuvs-java` version (see the note under the example Dockerfile) and
restart the node. With `vectors.indexing.use_gpu: auto` (the default) the node
starts and silently falls back to CPU indexing in this case. Use
`vectors.indexing.use_gpu: true` to make the node fail to start instead.

### Node fails to start with `vectors.indexing.use_gpu: true`

To enforce GPU indexing, set `vectors.indexing.use_gpu: true` in
`elasticsearch.yml`.
The node will fail to start if GPU indexing is not available, e.g. if a GPU
is not detected by {{es}}, or if the runtime is not supported, or if the
necessary dependencies are not correctly configured, etc.

If the node fails to start, check:
* A supported NVIDIA GPU is present
* CUDA runtime libraries and drivers are installed (check with `nvidia-smi`)
* `LD_LIBRARY_PATH` includes paths to the cuVS libraries and to their
  dependencies (e.g. CUDA)
* Supported platform: Linux x86_64 with Java 22 or higher


### Performance not improved with GPU indexing

If you are sure that GPU indexing is enabled but don't see performance
improvement, check the following:

* Ensure supported vector index types and element type are used
* Ensure the dataset is large enough to benefit from GPU acceleration
* Check if there are different bottlenecks affecting the indexing process:
  using GPU indexing accelerates the HNSW graph building, but speedups can be
  limited by other factors.
   * Indexing throughput depends on how fast you can get data into {{es}}.
     Check network speed and client performance. Use multiple clients if
     needed.
   * JSON parsing could dominate the computation: use base64 encoded vectors
     as opposed to json arrays
   * Storage speed is also important: as the GPU is able to process lots of
     data, you need a storage solution that is able to keep up. Avoid using
     network attached storage, and prefer fast NVMe to extract the most
     performance
* Consider monitoring CPU usage to demonstrate offloading to GPU
* Consider monitoring GPU usage (e.g. with `nvidia-smi`)
