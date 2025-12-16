---
applies_to:
  stack:
navigation_title: "GPU vector indexing"
---

# GPU accelerated vector indexing
```{applies_to}
stack: preview 9.3
```

{{es}} can use GPU acceleration to significantly speed up the indexing of dense vectors. GPU indexing is based on the [Nvidia cuVS library](https://developer.nvidia.com/cuvs) and leverages the parallel processing capabilities of graphics processing units to accelerate the construction of HNSW vector search indexes. GPU accelerated vector indexing is particularly beneficial for large-scale vector datasets and high-throughput indexing scenarios, freeing up CPU resources for other tasks.

## Requirements

GPU vector indexing requires the following:

* An [Enterprise subscription](https://www.elastic.co/subscriptions)
* A supported NVIDIA GPU (Ampere architecture or better, compute capability >= 8.0) with a minimum 8GB of GPU memory
* GPU driver, CUDA and cuVS runtime libraries installed on the node
* `LD_LIBRARY_PATH` environment variable configured to include the cuVS libraries path and its dependencies (CUDA, rmm, etc.)
* Supported platform: Linux x86_64 only, Java 22 or higher
* Supported dense vector configurations: `hnsw` and `int8_hnsw`; `float` element type only


## Configuration

GPU vector indexing is controlled by the [`vectors.indexing.use_gpu`](/reference/elasticsearch/configuration-reference/node-settings.md#gpu-vector-indexing-settings) node-level setting.

## Troubleshooting
By default, {{es}} uses GPU indexing for supported vector types if a compatible GPU and required libraries are detected.
Check server logs for messages indicating whether {{es}} has detected GPU.

If you see a message like the following, GPU was successfully detected and GPU indexing will be used:
```
[o.e.x.g.GPUSupport ] [elasticsearch-0] Found compatible GPU [NVIDIA L4] (id: [0])
```
If you don't see this message, look for warning messages explaining why GPU
indexing is not being used, such as not supported environment, missing libraries, or incompatible GPU.


### Node fails to start with `vectors.indexing.use_gpu: true`

To enforce GPU indexing, set `vectors.indexing.use_gpu: true` in `elasticsearch.yml`.
The node will fail to start if GPU is not detected by {{es}}.

If the node fails to start, check:
* Supported NVIDIA GPU is present
* CUDA runtime libraries and drivers are installed (check with `nvidia-smi`)
* `LD_LIBRARY_PATH` includes paths to CUDA and cuVS libraries
* Supported platform: Linux x86_64 with Java 22 or higher


### Performance not improved with GPU indexing

If you are sure that GPU indexing is enabled but don't see performance improvement, check the following:

* Ensure supported vector index types and element type are used
* Ensure the dataset is large enough to benefit from GPU acceleration
* Use base64 encoded vectors as opposed to json arrays
* Consider monitoring CPU usage to demonstrate offloading to GPU
* Consider monitoring GPU usage (e.g. with `nvidia-smi`)
