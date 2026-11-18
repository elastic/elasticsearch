# repository-gcs: `NetHttpTransport` → `Apache5HttpTransport`

Analysis of switching the HTTP client used by the GCS repository from the JDK `HttpURLConnection` based
`NetHttpTransport` to `com.google.api.client.http.apache.v5.Apache5HttpTransport` (Apache HttpClient 5.6.4 /
HttpCore 5.4.3, the versions already shipped by `workload-identity` and `x-pack:plugin:core`).

This document is a review aid and is not meant to be committed.

## 1. What changed

### Dependencies and packaging

| File | Change |
|---|---|
| `modules/repository-gcs/build.gradle` | `implementation`: `google-http-client-apache-v5:2.2.0`, `httpclient5`, `httpcore5`. `runtimeOnly`: `httpcore5-h2` (httpclient5 loads `HttpVersionPolicy` from it even for the blocking client), `log4j-slf4j2-impl` (httpclient5 logs through slf4j; same bridge as repository-s3/azure). `thirdPartyAudit`: the optional brotli/zstd/commons-compress classes referenced by httpclient5. Licence mappings for `httpcore5*` → `httpclient5` and `log4j*` → `log4j`. |
| `build-tools-internal/.../ComponentMetadataRulesPlugin.java` | `google-http-client-apache-v5` gets `ExcludeAllTransitivesRule` like every other Google HTTP client artifact (it declares guava in compile scope, which the build forbids on the compile classpath). |
| `gradle/verification-metadata.xml` | sha256 of `google-http-client-apache-v5-2.2.0.jar` (sha1 cross-checked against Maven Central). |
| `modules/repository-gcs/licenses/` | `httpclient5-*` (copied from workload-identity), `log4j-*` (copied from repository-s3). |
| `entitlement-policy.yaml` | `set_https_connection_properties` removed. It only covers `HttpsURLConnection#setSSLSocketFactory/setHostnameVerifier`, which `NetHttpTransport` called to install the pinned trust store. Nothing in the module calls them any more. `outbound_network` is all the blocking HttpClient needs (no threads, no inbound sockets). |

### Production code

| File | Change |
|---|---|
| `GoogleCloudStorageService` | `createHttpTransport` builds the `Apache5HttpTransport`: pinned `google.jks` trust store through the same `SslUtils` calls `NetHttpTransport.Builder#trustCertificates` used, `https.protocols`/`https.cipherSuites` honoured, HttpClient default hostname verifier, `PoolingHttpClientConnectionManager` bounded by the new `max_connections` setting, HTTP proxy via `setProxy`, SOCKS proxy via `SocketConfig#setSocksProxyAddress`, `SystemDefaultRoutePlanner(ProxySelector.getDefault())` when no proxy is configured, redirects/retries/content compression/cookies disabled. `createClient` shuts the transport down if building the storage client fails. Service account credentials are rebuilt with the client's transport factory so token refreshes share the pool, trust store and proxy. `isRetryableHttpClientException` classifies the HttpClient exception types the SDK does not know; the retry decorator uses it. `ClientsHolder#closeRepositoryClients` closes the removed client. |
| `GoogleCloudStorageClientSettings` | New `gcs.client.*.max_connections` (default 50, min 1). `loadCredential` no longer builds a `NetHttpTransport` per settings load; the proxy parameter is gone. |
| `GoogleCloudStoragePlugin` | Registers the new setting. |
| `MeteredStorage` | `Closeable`; owns the `HttpTransport` and shuts it down on `close()`. |
| `GoogleCloudStorageRetryingInputStream` | The `OPEN` retry decision also consults `isRetryableHttpClientException` (section 2.2 explains why). |
| `DiscardConnectionOnEarlyCloseExec` (new) | HttpClient exec-chain handler installed first in the chain. Closing a response body (or the response) before it was read to the end discards the connection instead of draining the remainder. Section 3.2 explains why this turned out to be necessary. |
| `docs/.../gcs-repository-settings.md` | Documents `max_connections`. |

## 2. Behavioural differences

### 2.1 Timeouts, limits and other defaults

Values the SDK passes down are unchanged: `HttpTransportOptions` → `HttpRequest#setConnectTimeout/setReadTimeout`
→ `LowLevelHttpRequest#setTimeout(connect, read)`. The ES settings map as before (`0`/unset → SDK default of 20 s,
`-1` → infinite). What differs is what the transport does with them and what it does on its own.

| Aspect | `NetHttpTransport` (`HttpURLConnection`) | `Apache5HttpTransport` (this change) |
|---|---|---|
| Connect timeout | `URLConnection#setConnectTimeout`; 0 = infinite | `RequestConfig#connectTimeout` built per request by the adapter; it takes precedence over the connection manager default (3 min). `Timeout` of 0 is "disabled" = infinite, so `-1` keeps meaning infinite. |
| Read timeout | `URLConnection#setReadTimeout` = `SO_TIMEOUT` | `RequestConfig#responseTimeout`, applied as `SO_TIMEOUT` to the connection for the duration of the request. Same semantics (per read call, not per response). |
| Wait for a free connection | n/a, connections were unbounded | **New.** `connectionRequestTimeout` is set by the adapter to the **connect timeout**. Default 20 s; `connect_timeout: -1` makes the wait unbounded. Expires with `ConnectionRequestTimeoutException`. |
| Concurrent connections | unbounded | **`max_connections`, default 50**, applied as both total and per-route limit (every request of a client goes to the same host, plus occasionally the token endpoint). Matches S3 and Azure. |
| Idle connections | JDK keep-alive cache: at most 5 idle connections per host (`http.maxConnections`), closed after 5 s idle or the server's `Keep-Alive: timeout` | Up to `max_connections` idle connections kept **indefinitely**. No idle evictor (it would need a thread and the `manage_threads` entitlement). A connection idle for more than 2 s (`validateAfterInactivity` default) is checked for staleness before reuse. |
| TLS handshake timeout | bounded by the read timeout (JDK sets `SO_TIMEOUT` before the handshake) | bounded by the **connect** timeout (`TlsConfig#handshakeTimeout` defaults to it). Same 20 s by default; differs only when the two settings differ. |
| TLS protocols / ciphers | JDK defaults, filtered by `https.protocols` / `https.cipherSuites` | Same system properties via `HttpsSupport`. When unset, HttpClient additionally excludes "weak" protocols (`SSL*`, `TLSv1`, `TLSv1.1`) and ciphers (NULL, anon, export, DES, RC4, ...). All of these are already disabled by the JDK 25 `jdk.tls.disabledAlgorithms` default, so no effective change. |
| Hostname verification | JSSE endpoint identification (`HTTPS`) | JSSE endpoint identification **and** HttpClient's `DefaultHostnameVerifier` (RFC 2818 + public suffix list). Strictly stricter, both accept the Google certificates. |
| Trust store | pinned `google.jks` | identical `SSLContext` construction (`SslUtils.initSslContext(getTlsSslContext(), trustStore, getPkixTrustManagerFactory())`). FIPS story unchanged. |
| HTTP proxy | `url.openConnection(proxy)`: `CONNECT` tunnel for https, absolute-URI requests for http | `DefaultProxyRoutePlanner` (what `HttpClientBuilder#setProxy` installs): same wire behaviour, no proxy authentication in either. |
| SOCKS proxy | `url.openConnection(proxy)` → `new Socket(proxy)` | `SocketConfig#socksProxyAddress` → `new Socket(proxy)` inside `DefaultHttpClientConnectionOperator`. Both resolve the target host locally before connecting. |
| JVM proxy properties (no proxy configured) | honoured by `URL#openConnection()`: `http(s).proxyHost/Port`, `http.nonProxyHosts`, `socksProxyHost` | `SystemDefaultRoutePlanner(ProxySelector.getDefault())` honours `http(s).proxyHost/Port` and `http.nonProxyHosts`. **`socksProxyHost` is not honoured** (route planners only know HTTP proxies). Use `proxy.type: socks` instead. |
| Redirects | `NetHttpTransport` disables them; the SDK follows redirects itself | disabled at the client and per request by the adapter; the SDK still follows them. |
| Automatic retries | `HttpURLConnection` transparently retries a request once when a reused keep-alive connection turns out to be dead | **None** at the HttpClient level (`disableAutomaticRetries`). Dead pooled connections are mostly caught by the 2 s staleness check; a race surfaces as `NoHttpResponseException`, retried by ES (2.2). |
| gzip | no transparent decoding; the SDK asks for and decodes gzip | `disableContentCompression`: same. (Transparent decoding would strip `Content-Length`, which `ContentLengthValidatingInputStream` and the SDK's size checks rely on.) |
| `Expect: 100-continue` | no | no (`RequestConfig` default) |
| Chunked uploads | `setChunkedStreamingMode` when the length is unknown | `Apache5ContentEntity` uses chunked encoding when the length is `-1`. Same. |
| `Upgrade: TLS/1.2` header | never | HttpClient's `RequestUpgrade` interceptor adds `Upgrade: TLS/1.2` + `Connection: Upgrade` to GET/HEAD/OPTIONS requests over **plain http** (`RequestConfig#protocolUpgradeEnabled` defaults to true and the adapter builds the `RequestConfig` itself, so it cannot be turned off from the builder). Irrelevant for https (production). Plain-http endpoints (test fixture, GCS emulators) ignore the header. |
| `User-Agent` | set by the SDK | set by the SDK; HttpClient only adds its own when absent |
| Cookies / auth cache | n/a | cookie management disabled; auth caching unused (no auth schemes configured) |
| Close of a partially read body | at most 512 KB are drained on a background thread, otherwise the socket is closed; the caller never blocks and `close()` does not throw | HttpClient would drain the remainder synchronously on the closing thread and throw from `close()` if that fails. `DiscardConnectionOnEarlyCloseExec` restores the JDK-like behaviour: the connection is closed immediately (no drain, no exception) and not reused. A fully read body releases its connection to the pool as usual. |
| Shutdown | `NetHttpTransport#shutdown` is a no-op | `CloseableHttpClient#close(GRACEFUL)`: the pool is closed and **leased connections are discarded too**, in-flight requests fail. Later use fails with `IllegalStateException("Connection pool shut down")`. |
| Logging | none | httpclient5 logs through slf4j, now bridged into log4j. Wire/header logging is available with `logger.org.apache.hc.client5.http.wire: DEBUG` / `...headers`. The Google SDK's slf4j usage (gax, opentelemetry) is bridged as well instead of going to the NOP logger. |

### 2.2 Exception types

The SDK classifies `IOException`s with `instanceof` checks (`SocketTimeoutException`, `SocketException`,
`SSLHandshakeException`, plus a few message matches such as `Premature EOF`/`Connection closed prematurely`).
HttpClient reports some failures with its own types. What happens to each:

| Failure | `HttpURLConnection` reported | HttpClient 5 reports | Handling |
|---|---|---|---|
| connect timeout | `SocketTimeoutException("connect timed out")` | `ConnectTimeoutException` **extends `SocketTimeoutException`** | SDK and ES decorator already retry |
| connection refused / unreachable | `ConnectException` | `HttpHostConnectException` **extends `ConnectException`** | SDK and ES decorator already retry |
| read timeout | `SocketTimeoutException("Read timed out")` | `SocketTimeoutException` | unchanged |
| stale keep-alive connection | transparently retried, or `SocketException("Unexpected end of file from server")` | `NoHttpResponseException` (plain `IOException`) | **added** to the ES decorator and to the stream `OPEN` classification |
| body shorter than `Content-Length` | `IOException("Premature EOF")` → SDK `connectionClosedPrematurely`, retryable | `ConnectionClosedException("Premature end of Content-Length delimited message body ...")` (plain `IOException`) | **added** to the ES decorator and `OPEN` classification. Mid-body it is also caught by `ContentLengthValidatingInputStream`/`RetryingInputStream` which retry any `StorageException` on `READ`. |
| stale connection detected while sending | n/a | `RequestNotExecutedException` extends `ConnectionClosedException` | covered by the above |
| no free pooled connection | n/a | `ConnectionRequestTimeoutException` (extends `InterruptedIOException`, not `SocketTimeoutException`) | **added**: treated like a connect timeout (transient congestion). Debatable, see section 5. |
| TLS handshake failure | `SSLHandshakeException` | `SSLHandshakeException` | unchanged |
| pool shut down | n/a | `IllegalStateException("Connection pool shut down")` | not retried (`RuntimeException`), only after `MeteredStorage#close` |

`GoogleCloudStorageRetryingInputStream` decides `OPEN` retries with `BaseService.EXCEPTION_HANDLER` (SDK classification
only; the ES decorator is not involved on that path). Without the added `isRetryableHttpClientException` check, a stale
pooled connection at open time would turn a previously transparent JDK retry into a failed read. The check adds only the
three HttpClient types; the other ES decorator additions (`UnknownHostException`, `SSLException`) are deliberately not
applied to the `OPEN` path, so its behaviour stays as close to before as possible.

### 2.3 Plumbing and lifecycle

* **One transport per client, shared with the credentials.** Before, each settings load built a throw-away
  `NetHttpTransport` (with the proxy but **without** the pinned trust store) for token refreshes, and the storage client
  had its own. Now the credentials are rebuilt in `createStorageOptions` with the client's transport factory. Consequences:
  token refreshes use the pooled connections, the proxy and the pinned Google trust store. `oauth2.googleapis.com` is
  covered by that store. A custom `token_uri` pointing at a non-Google TLS endpoint would now fail trust validation
  (previously it used the JVM default trust store); the setting is documented as a testing aid.
* **Application Default Credentials are unchanged.** When no `credentials_file` is configured the ADC path still uses the
  SDK default `NetHttpTransport` (no pinned trust store, no explicit proxy, as before). That transport never calls
  `setSSLSocketFactory`, so it does not need the removed entitlement.
* **Clients are closed when their repository closes.** `GoogleCloudStorageBlobStore#close` →
  `closeRepositoryClients` now closes the removed `MeteredStorage`, releasing its sockets. Requests still in flight on it
  fail. This mirrors `S3Service#onBlobStoreClose`. Clients dropped by `refreshAndClearCache` (secure settings reload) or by
  `applyClusterState` (project secrets change) are **not** closed: nothing reference-counts them, and in-flight resumable
  uploads or reads would fail. Their pools are reclaimed by GC (socket file descriptors are closed by the socket cleaner).
  Each dropped client holds at most `max_connections` idle sockets until then. Reloads are rare, so this is accepted rather
  than adding reference counting.
* `getDefaultProjectId` (metadata server fallback) still uses `HttpURLConnection` over plain http. Unchanged.

## 3. Things to be aware of

### 3.1 The connection ceiling is new

Every thread that issues a GCS request now competes for `max_connections` sockets. With the JDK client, N concurrent
requests opened N sockets. The default of 50 matches S3 and Azure. Operations that hold a connection for a long time are
`readBlob` streams (held until closed) and resumable upload chunks. Sizing: the stateless connection pool work (PR 164174)
wants to derive this from the thread pool sizes; this setting is the hook it needs for GCS.

If `connect_timeout` is set to `-1`, waits for a pooled connection become unbounded. A deadlock is possible in principle
if `max_connections` is smaller than the number of streams a single operation keeps open at the same time while opening
another one. With the default (20 s) such a situation fails with a retryable `ConnectionRequestTimeoutException` instead.

### 3.2 Closing a partially read response: drain vs. discard

HttpClient keeps a connection reusable by reading the rest of the body **synchronously, on the closing thread** when a
response is closed before EOF. Two code paths do that: `EofSensorInputStream#close` (the body stream) and, independently,
`CloseableHttpResponse#close` → `ResponseEntityProxy#close` (the response). `Apache5ResponseContent#close` calls both.
The JDK client drained at most 512 KB on a background thread and otherwise just closed the socket.

The initial plan was to accept the drain because streams are rarely closed early (in stateless every read is sized to what
it consumes, so it only happens on the retry path, bounded by the range size). The first test run showed the part that is not
acceptable: when a read has failed, HttpClient has already aborted the body stream, but the *response* close still tries to
drain the raw entity on the now closed connection and **throws from `close()`** (`SocketException`/`ConnectionClosedException`).
`AbstractBlobContainerRetriesTestCase#testReadBlobWithPrematureConnectionClose` and `#testReadBlobWithReadTimeouts` caught
this as one extra suppressed exception on the final failure (the try-with-resources attached the `close()` failure). Callers
that close a failed stream in a plain `finally` block would have had the original exception replaced by the close failure.
On a stuck connection the same drain blocks for a second read timeout before throwing.

`DiscardConnectionOnEarlyCloseExec` fixes both cases at the HttpClient level rather than in the SDK adapter (whose
`Apache5ResponseContent` hides the request and the `EofSensorInputStream`, so an S3-style `abort()` is not reachable from
outside): it wraps every response entity, counts the bytes read, and when the body stream or the entity is closed before
EOF it calls `ExecRuntime#discardEndpoint()` (close the socket, return the pool slot) and skips the delegate close that
would drain. A fully read body goes through the normal release path. The cost is that an early-closed connection is not
reused, which is what the S3 repository and, for anything over 512 KB, the JDK client did too.

This is tested directly in `GoogleCloudStorageServiceTests#testClosingPartiallyReadResponseDiscardsConnection` (connection
reuse observed through the client port seen by the server) and indirectly by the two retries tests above.

### 3.3 Retries got slightly wider

`NoHttpResponseException`, `ConnectionClosedException` and `ConnectionRequestTimeoutException` are retried by the ES
decorator for both idempotent and non-idempotent operations, in line with how the decorator already treats
`SocketException`/`SocketTimeoutException`. The first two are the HttpClient spellings of failures that were retried
before; the third is new behaviour with no previous equivalent.

### 3.4 Idle connections are kept

Up to `max_connections` idle sockets per client stay open indefinitely. GCS closes idle connections server-side after a
while; the 2 s staleness check catches that before reuse in the common case. There is no evictor thread (that would need
`manage_threads`). With the JDK client at most 5 idle sockets per host survived, and only for 5 s.

### 3.5 Shutdown is not graceful towards in-flight requests

`CloseableHttpClient#close(GRACEFUL)` closes leased connections as well. `MeteredStorage#close` is therefore only called
from `closeRepositoryClients`, i.e. after the repository is closed.

### 3.6 `socksProxyHost` system property

No longer picked up when no proxy is configured in the client settings. `http(s).proxyHost` and `http.nonProxyHosts`
still are. Use `gcs.client.*.proxy.type: socks` for SOCKS.

### 3.7 Logging now reaches log4j

httpclient5 (and the slf4j usage of the Google SDK) log through `log4j-slf4j2-impl`. Default levels keep them quiet.
`org.apache.hc.client5.http.wire` at DEBUG dumps request and response bodies.

### 3.8 Version skew

google-http-client 2.2.0 was built against httpclient5 5.3.1 / httpcore5 5.2.5; ES uses 5.6.4 / 5.4.3. The adapter only
uses `HttpClient#executeOpen`, `RoutingSupport#determineHost`, `HttpUriRequestBase` and `RequestConfig`, all stable since
5.2. `thirdPartyAudit` verifies the binary compatibility.

## 4. Tests

### 4.1 Test changes and why they are legitimate

| Test | Change | Reason |
|---|---|---|
| `GoogleCloudStorageClientSettingsTests` | constructor calls, `copySettings`/`mutateSettings`/`randomClient` gain `maxConnections`; `loadCredential` loses the proxy argument; `testLoad` asserts `max_connections`; `testRefreshCredentialsAccessTokenWithProxy` removed | signature changes for the new setting. The proxy test asserted that token refreshes go through the proxy; the transport is now attached in `GoogleCloudStorageService`, so the test moved there with the same assertions (request line seen by the proxy, token value). |
| `GoogleCloudStorageServiceTests` | `testClientInitializer` also asserts the transport type and `max_connections`; new `testMaxConnectionsBoundsConnectionPool` (pool of 1, second request fails with `ConnectionRequestTimeoutException` after the connect timeout, pool recovers after the first request completes); new `testClosingPartiallyReadResponseDiscardsConnection`; new `testRefreshCredentialsAccessTokenWithProxy` (moved); new `testRetryStrategyRetriesHttpClientTransportFailures` | cover the new behaviour. The class is annotated `@SuppressForbidden(reason = "use a http server")` for the JDK `HttpServer` used by the new tests. |
| `GoogleCloudStoragePluginTests` | expected settings list | new setting |
| `GoogleCloudStorageBlobContainerStatsTests` | constructor call | new setting |

No test expectation about exception types, messages, retries or timeouts was changed. The test proxy servers
(`MockHttpProxyServer`, `WebProxyServer`) keep using HttpClient 4.x from test scope; HttpClient 4 is not on the plugin's
runtime classpath, so they are not affected by the plugin entitlement policy.

### 4.2 Verification runs

| Run | Result |
|---|---|
| `:modules:repository-gcs:precommit` (spotless, forbidden APIs, third-party audit, licences, entitlement/plugin checks) | pass |
| `:modules:repository-gcs:test` | 81 tests, pass (incl. `GoogleCloudStorageBlobContainerRetriesTests`, which failed before `DiscardConnectionOnEarlyCloseExec` was added) |
| `:modules:repository-gcs:test --tests GoogleCloudStorageServiceTests -Dtests.iters=3` | 36 runs, pass |
| `:modules:repository-gcs:internalClusterTest` (incl. the HTTP proxy integration tests) | 25 tests, pass |
| `:modules:repository-gcs:yamlRestTest` (GCS fixture) | 42 tests, pass |
| `:x-pack:plugin:stateless:internalClusterTest` `GoogleObjectStoreTests`, `GcsTranslogBlobReplacedDuringRecoveryIT`, `BlobStoreSyncDirectoryIT` | 6 tests, pass |
| `:x-pack:plugin:stateless:test` `ObjectStoreServiceTests`, `StaleIndicesGCServiceTests` | 42 tests, pass |

Not run: `gcsThirdPartyTest` (needs a real bucket). It is the only end-to-end exercise of real TLS through the new transport
and should be run before merging. `GoogleCloudStorageAdcTests` fails on this workstation before and after the change
(a gcloud ADC file in the home directory), so it is not a signal either way.

The first full run also confirmed that without the `log4j-slf4j2-impl` bridge httpclient5 prints the slf4j
"No SLF4J providers were found" notice to stderr on first use; with the bridge it is gone.

## 5. Open points for review

1. **`max_connections` default of 50** (S3/Azure parity) and the fact that the pool wait is bounded by `connect_timeout`
   rather than a dedicated setting.
2. **Retrying `ConnectionRequestTimeoutException`.** Pool exhaustion is local overload; retrying with the SDK backoff
   (up to `max_retries`) hides it for a while instead of failing fast. The alternative is to leave it non-retryable so the
   operation fails and the misconfiguration surfaces.
3. **Closing clients on repository close but not on settings reload.** Consistent with S3 only for the first half.
4. **Credentials now share the pinned trust store.** Affects only non-Google `token_uri` overrides.
5. **`DiscardConnectionOnEarlyCloseExec`** couples the module to HttpClient's exec-chain API (`ExecChainHandler`,
   `ExecRuntime#discardEndpoint`). Both are public, documented extension points, and `thirdPartyAudit` checks binary
   compatibility on upgrades, but it is the one piece that relies on HttpClient internals behaving as described in 3.2.
