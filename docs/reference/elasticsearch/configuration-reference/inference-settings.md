---
navigation_title: "Inference settings"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/inference-settings.html
applies_to:
  deployment:
    self:
---

# Inference API settings in {{es}} [inference-settings]


$$$inference-settings-description$$$
You do not need to configure any settings to use the {{infer}} APIs. Each setting has a default.


### Inference API logging settings [xpack-inference-logging]

When certain failures occur, a log message is emitted. In the case of a reoccurring failure the logging throttler restricts repeated messages from being logged.

`xpack.inference.logging.reset_interval`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) Specifies the interval for when a cleanup thread will clear an internal cache of the previously logged messages. Defaults to one day (`1d`).

`xpack.inference.logging.wait_duration`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) Specifies the amount of time to wait after logging a message before that message can be logged again. Defaults to one hour (`1h`).

## {{infer-cap}} API HTTP settings [xpack-inference-http-settings]

`xpack.inference.http.max_response_size`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) Specifies the maximum size in bytes an HTTP response is allowed to have, defaults to `50mb`, the maximum configurable value is `100mb`.

`xpack.inference.http.max_total_connections`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) Specifies the maximum number of connections the internal connection pool can lease. Defaults to `50`.

`xpack.inference.http.max_route_connections`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) Specifies the maximum number of connections a single route can lease from the internal connection pool. If this setting is set to a value equal to or greater than `xpack.inference.http.max_total_connections`, then a single third party service could lease all available connections and other third party services would be unable to lease connections. Defaults to `20`.

`xpack.inference.http.connection_eviction_interval`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) Specifies the interval that an eviction thread will run to remove expired and stale connections from the internal connection pool. Decreasing this time value can help improve throughput if multiple third party service are contending for the available connections in the pool. Defaults to one minute (`1m`).

`xpack.inference.http.connection_eviction_max_idle_time`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) Specifies the maximum duration a connection can be unused before it is marked as idle and can be closed and removed from the shared connection pool. Defaults to one minute (`1m`).

`xpack.inference.http.request_executor.queue_capacity`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) Specifies the size of the internal queue for requests waiting to be sent. If the queue is full and a request is sent to the {{infer}} API, it will be rejected. Defaults to `2000`.


## {{infer-cap}} API HTTP Retry settings [xpack-inference-http-retry-settings]

When a third-party service returns a transient failure code (for example, 429), the request is retried by the {{infer}} API. These settings govern the retry behavior. When a request is retried, exponential backoff is used.

`xpack.inference.http.retry.initial_delay`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) Specifies the initial delay before retrying a request. Defaults to one second (`1s`).

`xpack.inference.http.retry.max_delay_bound`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) Specifies the maximum delay for a request. Defaults to five seconds (`5s`).

`xpack.inference.http.retry.timeout`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) Specifies the maximum amount of time a request can be retried. Once the request exceeds this time, the request will no longer be retried and a failure will be returned. Defaults to 30 seconds (`30s`).


## {{infer-cap}} API Input text [xpack-inference-input-text]

For certain third-party service integrations, when the service returns an error indicating that the request input was too large, the input will be truncated and the request is retried. These settings govern how the truncation is performed.

`xpack.inference.truncator.reduction_percentage`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) Specifies the percentage to reduce the input text by if the 3rd party service responds with an error indicating it is too long. Defaults to 50 percent (`0.5`).


