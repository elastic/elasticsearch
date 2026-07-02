/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.workloadidentity;

import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.workloadidentity.spi.WorkloadIdentityIssuerClient;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * mTLS HTTPS transport for the workload-identity-issuer {@code POST /token} endpoint, built on
 * the Apache HttpComponents async client managed by {@link WorkloadIdentityHttpClientManager}.
 *
 * <h2>Request shape</h2>
 * <pre>{@code
 * POST <issuer-url>/token  HTTP/1.1
 * Content-Type: application/json
 *
 * { "aud": "<aud-value>" }
 * }</pre>
 * The {@code aud} value is opaque to the issuer and is copied verbatim into the JWT's {@code aud}
 * claim; the customer's CSP validates it against their trust policy. The region the issuer uses to
 * select a signing key and embed in the {@code iss} claim is derived from the SNI hostname the
 * node connects to (see {@link WorkloadIdentityIssuerSettings#ISSUER_URL_SETTING}) and is not
 * carried in the request body.
 *
 * <h2>Response shape</h2>
 * <pre>{@code
 * 200 OK
 * Content-Type: application/json
 *
 * { "token": "<jwt>", "expires_at": 1716003600 }
 * }</pre>
 * The {@code expires_at} field is parsed as epoch seconds.
 */
public final class HttpsWorkloadIdentityIssuerClient implements WorkloadIdentityIssuerClient {

    // log4j Logger (rather than the org.elasticsearch.logging facade used in the rest of the
    // workload-identity module) because RetryableAction's constructor accepts the log4j API
    // directly. Matches the convention at every other RetryableAction host class in the repo.
    private static final Logger logger = LogManager.getLogger(HttpsWorkloadIdentityIssuerClient.class);

    private static final String TOKEN_PATH = "/token";

    /**
     * Soft ceiling on the number of cached entries. The cardinality of {@link IssueTokenRequest}
     * keys ({@code audience}) is structurally bounded today by node-scope
     * configuration (data sources / credentials), and entries self-evict at
     * {@code expires_at - refresh_before_expiry}, so production traffic should never approach this
     * cap. It exists as defense-in-depth: any future caller that lets user-controlled values flow
     * into a {@link IssueTokenRequest} would otherwise grow the map without bound. Once the cap is
     * reached, new (uncached) request keys bypass the cache and dispatch a one-shot HTTP fetch
     * rather than failing the listener; existing entries continue to serve cache hits normally.
     *
     * <p>The cap is enforced by a non-atomic {@code size()} check immediately before
     * {@code putIfAbsent}, so under contention the map size may briefly exceed this value by up to
     * O(concurrent cache missers). The transient overshoot is bounded by request concurrency and
     * is acceptable for a defense-in-depth bound; it is not relied upon for correctness.
     */
    static final int MAX_CACHE_ENTRIES = 1024;

    /**
     * Sanity ceiling on the lifetime of an issued token. A response whose {@code expires_at} is
     * more than this far in the future is rejected as a parse failure. Guards against
     * response-encoding bugs (e.g. epoch-millis read as epoch-seconds) and against values that
     * would overflow {@link Instant#toEpochMilli()} downstream. Not a policy bound on issuer
     * lifetime.
     */
    static final long MAX_TOKEN_LIFETIME_SECONDS = TimeValue.timeValueDays(365).seconds();

    private static final ParseField TOKEN_FIELD = new ParseField("token");
    private static final ParseField EXPIRES_AT_FIELD = new ParseField("expires_at");

    /**
     * Parses the workload-identity-issuer JSON token response. Unknown fields are tolerated so the
     * issuer can introduce additive fields without breaking clients. Required-field absence and
     * type mismatches are surfaced by the parser as {@code XContentParseException} (a subclass of
     * {@link IllegalArgumentException}); the {@link IssueTokenResponse} canonical constructor
     * performs field validation (non-empty token, non-null expiresAt) and may itself throw
     * {@link IllegalArgumentException}. Both are rewrapped in {@link WorkloadIdentityIssuerException}
     * at the call site.
     */
    private static final ConstructingObjectParser<IssueTokenResponse, Void> RESPONSE_PARSER = new ConstructingObjectParser<>(
        "workload_identity_issuer_response",
        true,
        args -> new IssueTokenResponse((String) args[0], Instant.ofEpochSecond((Long) args[1]))
    );
    static {
        RESPONSE_PARSER.declareString(constructorArg(), TOKEN_FIELD);
        RESPONSE_PARSER.declareLong(constructorArg(), EXPIRES_AT_FIELD);
    }

    private final WorkloadIdentityHttpClientManager httpClientManager;
    private final URI tokenEndpoint;
    private final RequestConfig requestConfig;
    private final long maxResponseSizeBytes;
    private final ThreadPool threadPool;
    private final long refreshBeforeExpiryMillis;
    private final int maxCacheEntries;
    private final TimeValue retryInitialDelay;
    private final TimeValue retryMaxDelayBound;
    private final TimeValue retryTimeout;
    private final int retryMaxAttempts;

    /**
     * Tokens cached, keyed by request shape. The value is a {@link SubscribableListener} so that
     * concurrent missing callers for the same key share a single in-flight HTTP fetch. Each entry
     * is removed by a scheduled task at {@code expiresAt - refresh_before_expiry}, so a hit
     * against this map is always fresh by construction. Bounded above by {@link #maxCacheEntries};
     * see {@link #MAX_CACHE_ENTRIES}.
     */
    private final ConcurrentMap<IssueTokenRequest, SubscribableListener<IssueTokenResponse>> tokens = ConcurrentCollections
        .newConcurrentMap();

    /**
     * Single-shot guard so that the WARN log in {@link #issueToken} fires at most once per
     * "saturation episode": flipped to {@code true} the first time the cap is hit, reset to
     * {@code false} the next time a successful caching insertion proves there is room again. A
     * sustained-saturation period therefore yields one log line, not one per request.
     */
    private final AtomicBoolean cacheAtCapacityLogged = new AtomicBoolean();

    public HttpsWorkloadIdentityIssuerClient(
        Settings settings,
        WorkloadIdentityHttpClientManager httpClientManager,
        ThreadPool threadPool
    ) {
        this(settings, httpClientManager, threadPool, MAX_CACHE_ENTRIES);
    }

    /**
     * Package-private constructor that exposes the cache cap as an explicit parameter so unit tests
     * can drive the saturation path without minting {@link #MAX_CACHE_ENTRIES} distinct tokens. Not
     * a public extension point: production callers should always use the {@code Settings}-based
     * constructor, which pins the cap to {@link #MAX_CACHE_ENTRIES}.
     */
    HttpsWorkloadIdentityIssuerClient(
        Settings settings,
        WorkloadIdentityHttpClientManager httpClientManager,
        ThreadPool threadPool,
        int maxCacheEntries
    ) {
        if (maxCacheEntries < 1) {
            throw new IllegalArgumentException("maxCacheEntries must be >= 1, got [" + maxCacheEntries + "]");
        }
        this.httpClientManager = Objects.requireNonNull(httpClientManager, "httpClientManager");
        this.threadPool = Objects.requireNonNull(threadPool, "threadPool");
        this.tokenEndpoint = resolveTokenEndpoint(WorkloadIdentityIssuerSettings.ISSUER_URL_SETTING.get(settings));

        final TimeValue connect = WorkloadIdentityHttpSettings.CONNECT_TIMEOUT.get(settings);
        final TimeValue request = WorkloadIdentityHttpSettings.REQUEST_TIMEOUT.get(settings);
        this.requestConfig = RequestConfig.custom()
            .setConnectTimeout(Math.toIntExact(connect.millis()))
            .setConnectionRequestTimeout(Math.toIntExact(connect.millis()))
            .setSocketTimeout(Math.toIntExact(request.millis()))
            .build();

        final ByteSizeValue maxResponseSize = WorkloadIdentityHttpSettings.MAX_RESPONSE_SIZE.get(settings);
        this.maxResponseSizeBytes = maxResponseSize.getBytes();

        this.refreshBeforeExpiryMillis = WorkloadIdentityIssuerSettings.TOKEN_CACHE_REFRESH_BEFORE_EXPIRY.get(settings).millis();
        this.maxCacheEntries = maxCacheEntries;

        this.retryInitialDelay = WorkloadIdentityHttpSettings.RETRY_INITIAL_DELAY.get(settings);
        this.retryMaxDelayBound = WorkloadIdentityHttpSettings.RETRY_MAX_DELAY_BOUND.get(settings);
        this.retryTimeout = WorkloadIdentityHttpSettings.RETRY_TIMEOUT.get(settings);
        this.retryMaxAttempts = WorkloadIdentityHttpSettings.RETRY_MAX_ATTEMPTS.get(settings);
        // A single attempt must fit within the retry budget; otherwise the first attempt would
        // always exceed RETRY_TIMEOUT and the retry layer would degenerate to "one attempt that
        // also has to lose a race against the wall clock". Validate at construction so the
        // misconfiguration surfaces at node start.
        if (retryTimeout.millis() < request.millis()) {
            throw new IllegalArgumentException(
                "setting ["
                    + WorkloadIdentityHttpSettings.RETRY_TIMEOUT.getKey()
                    + "] ("
                    + retryTimeout
                    + ") must be greater than or equal to setting ["
                    + WorkloadIdentityHttpSettings.REQUEST_TIMEOUT.getKey()
                    + "] ("
                    + request
                    + ")"
            );
        }
        // RetryableAction enforces this same invariant, but only when run() is first invoked. Mirror
        // the check here so the misconfiguration surfaces at node start alongside the RETRY_TIMEOUT
        // vs REQUEST_TIMEOUT check above, rather than at the first token request.
        if (retryMaxDelayBound.millis() < retryInitialDelay.millis()) {
            throw new IllegalArgumentException(
                "setting ["
                    + WorkloadIdentityHttpSettings.RETRY_MAX_DELAY_BOUND.getKey()
                    + "] ("
                    + retryMaxDelayBound
                    + ") must be greater than or equal to setting ["
                    + WorkloadIdentityHttpSettings.RETRY_INITIAL_DELAY.getKey()
                    + "] ("
                    + retryInitialDelay
                    + ")"
            );
        }
    }

    // Package-private for direct unit testing in ResolveTokenEndpointTests.
    static URI resolveTokenEndpoint(String issuerUrl) {
        if (issuerUrl == null || issuerUrl.isEmpty()) {
            throw new IllegalArgumentException(
                "setting [" + WorkloadIdentityIssuerSettings.ISSUER_URL_SETTING.getKey() + "] must be configured"
            );
        }
        final URI baseUri;
        try {
            baseUri = new URI(issuerUrl);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("workload-identity issuer URL [" + issuerUrl + "] is not a valid URI", e);
        }
        if ("https".equalsIgnoreCase(baseUri.getScheme()) == false) {
            throw new IllegalArgumentException("workload-identity issuer URL [" + issuerUrl + "] must use the https scheme");
        }
        // Catches the three host-less inputs that otherwise parse cleanly with scheme=https:
        // the opaque form `https:relative/path`, the empty-authority form `https:///path`, and
        // the port-without-host form `https://:8443/path`. Without this guard, Apache HC only
        // surfaces the failure at the first dispatched request rather than at node startup.
        if (Strings.isNullOrEmpty(baseUri.getHost())) {
            throw new IllegalArgumentException("workload-identity issuer URL [" + issuerUrl + "] must include a host");
        }
        if (baseUri.getRawQuery() != null || baseUri.getRawFragment() != null) {
            throw new IllegalArgumentException(
                "workload-identity issuer URL [" + issuerUrl + "] must not include a query string or fragment"
            );
        }
        // Append textually to preserve the raw encoding of the configured path. The 7-arg URI
        // constructor would treat the path as decoded and double-encode any literal "%" (e.g.
        // /api%2Fv1 -> /api%252Fv1). Query and fragment were rejected above.
        final String baseString = stripTrailingSlash(baseUri.toString());
        try {
            return new URI(baseString + TOKEN_PATH);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("cannot construct workload-identity token endpoint from [" + issuerUrl + "]", e);
        }
    }

    private static String stripTrailingSlash(String path) {
        int end = path.length();
        while (end > 0 && path.charAt(end - 1) == '/') {
            end--;
        }
        return path.substring(0, end);
    }

    @Override
    public void issueToken(IssueTokenRequest request, ActionListener<IssueTokenResponse> listener) {
        Objects.requireNonNull(request, "request must not be null");
        Objects.requireNonNull(listener, "listener must not be null");
        // Cache hit (resolved or in-flight): attach and return.
        SubscribableListener<IssueTokenResponse> existing = tokens.get(request);
        if (existing != null) {
            existing.addListener(listener);
            return;
        }
        // Saturation guard: rather than grow the map without bound (or reject the caller), bypass
        // the cache and dispatch a one-shot uncached fetch. The size check is intentionally racy
        // with respect to the putIfAbsent below, so the cap is a soft ceiling that may be exceeded
        // by O(concurrent missers) under contention; for a defense-in-depth bound that is fine.
        if (tokens.size() >= maxCacheEntries) {
            if (cacheAtCapacityLogged.compareAndSet(false, true)) {
                logger.warn("workload-identity token cache is at capacity [{}]; bypassing cache for new request keys", maxCacheEntries);
            }
            fetchToken(request, listener);
            return;
        }
        SubscribableListener<IssueTokenResponse> fresh = new SubscribableListener<>();
        SubscribableListener<IssueTokenResponse> raced = tokens.putIfAbsent(request, fresh);
        if (raced != null) {
            raced.addListener(listener);
            return;
        }
        // A successful insertion proves the cap is not pinned, so the next time the cap is hit we
        // want to surface another WARN. Idempotent reset: cheap on the steady-state cache-hit path
        // (we only get here on the cache-miss path).
        cacheAtCapacityLogged.set(false);
        startFetch(request, fresh);
        fresh.addListener(listener);
    }

    private void startFetch(IssueTokenRequest request, SubscribableListener<IssueTokenResponse> listener) {
        fetchToken(request, ActionListener.wrap(response -> {
            listener.onResponse(response);
            scheduleEviction(request, listener, response);
        }, failure -> {
            // Don't cache failures; let the next caller retry.
            tokens.remove(request, listener);
            listener.onFailure(failure);
        }));
    }

    private void scheduleEviction(IssueTokenRequest request, SubscribableListener<IssueTokenResponse> entry, IssueTokenResponse response) {
        // toEpochMilli() is safe here by construction: every cached response has passed
        // validateExpiry, which caps expiresAt within MAX_TOKEN_LIFETIME_SECONDS of now.
        final long delayMillis = Math.max(0L, response.expiresAt().toEpochMilli() - System.currentTimeMillis() - refreshBeforeExpiryMillis);
        try {
            threadPool.schedule(() -> tokens.remove(request, entry), TimeValue.timeValueMillis(delayMillis), threadPool.generic());
        } catch (Exception e) {
            // If scheduling fails (e.g. shutting-down thread pool), evict immediately rather than
            // leaving a stale entry behind. The next caller will refetch.
            tokens.remove(request, entry);
            logger.debug("failed to schedule workload-identity cache eviction; evicting immediately", e);
        }
    }

    /**
     * Wraps a single token request in an {@link IssueTokenRetrier} so transient failures (5xx,
     * 429, IOException) are retried with jittered exponential backoff, bounded by
     * {@link WorkloadIdentityHttpSettings#RETRY_MAX_ATTEMPTS} and
     * {@link WorkloadIdentityHttpSettings#RETRY_TIMEOUT}. The retrier sits below {@link #startFetch}
     * so concurrent callers continue to share a single in-flight (now retrying) fetch via the
     * cache's {@link SubscribableListener}.
     */
    private void fetchToken(IssueTokenRequest request, ActionListener<IssueTokenResponse> listener) {
        new IssueTokenRetrier(request, listener).run();
    }

    /**
     * Dispatches a single token request and routes every outcome through {@code listener}. The
     * outer try/catch is contract enforcement: callers ({@link IssueTokenRetrier#tryAction}) rely
     * on completion happening exclusively via the listener for retry-lifecycle correctness, so
     * any exception that escapes the synchronous setup path (request rendering, Apache HC client
     * retrieval, request construction, async submission) is funneled into
     * {@link ActionListener#onFailure} rather than being allowed to propagate. The
     * {@link FutureCallback} handles the asynchronous outcomes.
     */
    private void dispatchTokenRequest(IssueTokenRequest request, ActionListener<IssueTokenResponse> listener) {
        try {
            final byte[] body = renderRequestBody(request);

            final HttpPost httpPost = new HttpPost(tokenEndpoint);
            httpPost.setEntity(new ByteArrayEntity(body, ContentType.APPLICATION_JSON));
            httpPost.setHeader(HttpHeaders.ACCEPT, ContentType.APPLICATION_JSON.getMimeType());
            httpPost.setConfig(requestConfig);

            final CloseableHttpAsyncClient httpClient = httpClientManager.getHttpClient();

            logger.trace("dispatching workload-identity token request to [{}]", tokenEndpoint);
            httpClient.execute(httpPost, new FutureCallback<>() {
                @Override
                public void completed(HttpResponse response) {
                    try {
                        listener.onResponse(handleResponse(response, maxResponseSizeBytes));
                    } catch (Exception e) {
                        listener.onFailure(e);
                    } finally {
                        EntityUtils.consumeQuietly(response.getEntity());
                    }
                }

                @Override
                public void failed(Exception ex) {
                    listener.onFailure(ex);
                }

                @Override
                public void cancelled() {
                    listener.onFailure(new CancellationException("workload-identity token request was cancelled"));
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private static byte[] renderRequestBody(IssueTokenRequest request) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            builder.field("aud", request.audience());
            builder.endObject();
            return Strings.toString(builder).getBytes(StandardCharsets.UTF_8);
        }
    }

    private static IssueTokenResponse handleResponse(HttpResponse response, long maxResponseSizeBytes) throws IOException {
        final int status = response.getStatusLine().getStatusCode();
        final byte[] body = readBody(response, maxResponseSizeBytes);
        if (status < 200 || status >= 300) {
            // Keep the body out of the exception message so callers and downstream log sites see only
            // the status code; surface the body at DEBUG for operators investigating issuer failures.
            if (body.length > 0 && logger.isDebugEnabled()) {
                logger.debug(
                    () -> Strings.format(
                        "workload-identity-issuer returned HTTP %d with response body: %s",
                        status,
                        new String(body, StandardCharsets.UTF_8)
                    )
                );
            }
            throw new WorkloadIdentityIssuerException("workload-identity-issuer returned HTTP " + status, status);
        }
        return parseSuccess(body, status);
    }

    private static byte[] readBody(HttpResponse response, long maxResponseSizeBytes) throws IOException {
        if (response.getEntity() == null) {
            return new byte[0];
        }
        try (InputStream input = new SizeLimitInputStream(maxResponseSizeBytes, response.getEntity().getContent())) {
            return input.readAllBytes();
        }
    }

    private static IssueTokenResponse parseSuccess(byte[] body, int status) throws IOException {
        if (body.length == 0) {
            throw new WorkloadIdentityIssuerException("empty response body from workload-identity-issuer", status);
        }
        final IssueTokenResponse response;
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, body)) {
            response = RESPONSE_PARSER.parse(parser, null);
        } catch (IllegalArgumentException e) {
            // Covers XContentParseException (parser-level: missing field, wrong type), any
            // IllegalArgumentException raised by the IssueTokenResponse canonical constructor, and
            // (via the XContentParseException wrapper) DateTimeException raised by Instant.ofEpochSecond
            // when the parsed epoch-seconds value is outside the representable Instant range.
            throw new WorkloadIdentityIssuerException("failed to parse workload-identity-issuer response: " + e.getMessage(), status, e);
        }
        validateExpiry(response, status);
        return response;
    }

    /**
     * Retry policy applied by {@link IssueTokenRetrier}. Retryable failures are those that a
     * subsequent attempt could plausibly succeed past: transport-level errors and the
     * issuer-reported transient statuses (429 plus any 5xx). Configuration / credential errors
     * (4xx other than 429), parse failures, validation failures (all of which surface with
     * {@link WorkloadIdentityIssuerException#statusCode()} either set to the 4xx code or to the
     * sentinel {@code -1}), and explicit {@link CancellationException}s are not retried.
     */
    // Package-private for direct unit testing of the retry policy without a real HTTP server.
    static boolean isRetryable(Exception e) {
        if (e instanceof CancellationException) {
            return false;
        }
        if (e instanceof WorkloadIdentityIssuerException issuerEx) {
            final int status = issuerEx.statusCode();
            return status == 429 || (status >= 500 && status <= 599);
        }
        // ConnectionClosedException, SocketTimeoutException, SSLException, generic IO failures.
        return e instanceof IOException;
    }

    /**
     * Per-call retrier that wraps {@link #dispatchTokenRequest} in
     * {@link RetryableAction}'s jittered exponential-backoff schedule. Stops retrying when either
     * the attempt count reaches {@link #retryMaxAttempts} or the wall-clock budget
     * {@link #retryTimeout} is exhausted; whichever fires first surfaces the most recent failure
     * to the caller, with earlier failures attached as suppressed exceptions by the base class.
     */
    private final class IssueTokenRetrier extends RetryableAction<IssueTokenResponse> {

        private final IssueTokenRequest request;
        private final AtomicInteger attempts = new AtomicInteger();

        IssueTokenRetrier(IssueTokenRequest request, ActionListener<IssueTokenResponse> listener) {
            super(logger, threadPool, retryInitialDelay, retryMaxDelayBound, retryTimeout, listener, threadPool.generic());
            this.request = request;
        }

        @Override
        public void tryAction(ActionListener<IssueTokenResponse> listener) {
            attempts.incrementAndGet();
            dispatchTokenRequest(request, listener);
        }

        @Override
        public boolean shouldRetry(Exception e) {
            return attempts.get() < retryMaxAttempts && isRetryable(e);
        }
    }

    /**
     * Sanity-checks {@code expires_at} for encoding bugs (e.g. epoch-millis read as epoch-seconds),
     * not freshness.
     */
    private static void validateExpiry(IssueTokenResponse response, int status) {
        final long expiresAtSeconds = response.expiresAt().getEpochSecond();
        final long nowSeconds = System.currentTimeMillis() / 1000;
        if (expiresAtSeconds < nowSeconds) {
            throw new WorkloadIdentityIssuerException(
                "workload-identity-issuer returned a token whose expires_at [" + response.expiresAt() + "] is in the past",
                status
            );
        }
        if (expiresAtSeconds - nowSeconds > MAX_TOKEN_LIFETIME_SECONDS) {
            throw new WorkloadIdentityIssuerException(
                "workload-identity-issuer returned a token whose expires_at ["
                    + response.expiresAt()
                    + "] exceeds the maximum acceptable lifetime of "
                    + TimeValue.timeValueSeconds(MAX_TOKEN_LIFETIME_SECONDS),
                status
            );
        }
    }

}
