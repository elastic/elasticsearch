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


## {{infer-cap}} logging settings [xpack-inference-logging]

When certain failures occur, a log message is emitted. In the case of a reoccurring failure the logging throttler restricts repeated messages from being logged.

`xpack.inference.logging.reset_interval`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Specifies the interval for when a cleanup thread will clear an internal cache of the previously logged messages. Defaults to one day (`1d`).

`xpack.inference.logging.wait_duration`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Specifies the amount of time to wait after logging a message before that message can be logged again. Defaults to one hour (`1h`).

## {{infer-cap}} API HTTP settings [xpack-inference-http-settings]

`xpack.inference.http.max_response_size`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Specifies the maximum size in bytes an HTTP response is allowed to have, defaults to `50mb`, the maximum configurable value is `100mb`.

`xpack.inference.http.max_total_connections`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Specifies the maximum number of connections the internal connection pool can lease. Defaults to `50`.

`xpack.inference.http.max_route_connections`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Specifies the maximum number of connections a single route can lease from the internal connection pool. If this setting is set to a value equal to or greater than `xpack.inference.http.max_total_connections`, then a single third party service could lease all available connections and other third party services would be unable to lease connections. Defaults to `20`.

`xpack.inference.http.connection_eviction_interval`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Specifies the interval that an eviction thread will run to remove expired and stale connections from the internal connection pool. Decreasing this time value can help improve throughput if multiple third party service are contending for the available connections in the pool. Defaults to one minute (`1m`).

`xpack.inference.http.connection_eviction_max_idle_time`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Specifies the maximum duration a connection can be unused before it is marked as idle and can be closed and removed from the shared connection pool. Defaults to one minute (`1m`).

`xpack.inference.http.request_executor.queue_capacity`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Specifies the size of the internal queue for requests waiting to be sent. If the queue is full and a request is sent to the {{infer}} API, it will be rejected. Defaults to `2000`.


## {{infer-cap}} API HTTP Retry settings [xpack-inference-http-retry-settings]

When a third-party service returns a transient failure code (for example, 429), the request is retried by the {{infer}} API. These settings govern the retry behavior. When a request is retried, exponential backoff is used.

`xpack.inference.http.retry.initial_delay`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Specifies the initial delay before retrying a request. Defaults to one second (`1s`).

`xpack.inference.http.retry.max_delay_bound`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Specifies the maximum delay for a request. Defaults to five seconds (`5s`).

`xpack.inference.http.retry.timeout`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Specifies the maximum amount of time a request can be retried. Once the request exceeds this time, the request will no longer be retried and a failure will be returned. Defaults to 30 seconds (`30s`).


## {{infer-cap}} API Input text [xpack-inference-input-text]

For certain third-party service integrations, when the service returns an error indicating that the request input was too large, the input will be truncated and the request is retried. These settings govern how the truncation is performed.

`xpack.inference.truncator.reduction_percentage`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Specifies the percentage to reduce the input text by if the 3rd party service responds with an error indicating it is too long. Defaults to 50 percent (`0.5`).

## {{infer-cap}} API Cache settings [xpack-inference-cache-settings]

### Inference endpoint cache settings [xpack-inference-endpoint-cache-settings]

The inference endpoint cache stores assembled inference endpoint configurations for reuse across requests. Entries are invalidated cluster-wide whenever an endpoint is updated or deleted.

`xpack.inference.endpoint.cache.enabled`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Specifies whether the inference endpoint cache is enabled. Defaults to `true`.

`xpack.inference.endpoint.cache.weight`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Specifies the maximum number of inference endpoints to store in the cache. Defaults to `25`.

`xpack.inference.endpoint.cache.expiry_time`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Specifies the maximum duration an inference endpoint is kept in the cache after it was last written. Must be between one minute (`1m`) and one hour (`1h`). Defaults to 15 minutes (`15m`).

### OAuth2 token cache settings [xpack-inference-oauth2-token-cache-settings]

The OAuth2 token cache stores bearer tokens on each node for reuse across requests to third-party services that require OAuth2 authentication. Tokens are kept in heap memory only and are never persisted.

`xpack.inference.oauth2.token_cache.enabled`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Specifies whether the OAuth2 token cache is enabled. Defaults to `true`.

`xpack.inference.oauth2.token_cache.weight`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Specifies the maximum number of OAuth2 tokens to store in the cache. Defaults to `25`.

`xpack.inference.oauth2.token_cache.expiry_time`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Specifies the maximum duration an OAuth2 token is kept in the cache after it was last written. Must be between one minute (`1m`) and 24 hours (`24h`). Defaults to one hour (`1h`). A token can expire separately based on the exiry time set by the upstream server. In that situation, the token is automatically refereshed and will overwrite the entry in the cache if it already existed.

### CCM cache settings [xpack-inference-ccm-cache-settings]

The CCM (Cloud Connected Mode) cache stores whether CCM is enabled for the Elastic Inference Service for this cluster and the associated key. CCM is only applicable for on-prem clusters.

`xpack.inference.ccm.cache.weight`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Specifies the maximum number of entries to store in the CCM cache. Defaults to `1`. Only one key is required to connect a cluster to Elastic Inference Service through Cloud Connect. Increasing this setting will not have any affect.

`xpack.inference.ccm.cache.expiry_time`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Specifies the maximum duration an entry is kept in the cache after it was last written. Must be between one minute (`1m`) and one hour (`1h`). Defaults to 15 minutes (`15m`).
