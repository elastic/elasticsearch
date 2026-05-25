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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
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
 * { "audience": "<aud-value>", "region": "<region>" }
 * }</pre>
 * The {@code region} field is omitted when {@code null}.
 *
 * <h2>Response shape</h2>
 * <pre>{@code
 * 200 OK
 * Content-Type: application/json
 *
 * { "token": "<jwt>", "issued_at_epoch_seconds": 1716000000, "expires_at_epoch_seconds": 1716003600 }
 * }</pre>
 */
public final class HttpsWorkloadIdentityIssuerClient implements WorkloadIdentityIssuerClient {

    private static final Logger logger = LogManager.getLogger(HttpsWorkloadIdentityIssuerClient.class);

    private static final String TOKEN_PATH = "/token";

    private static final ParseField TOKEN_FIELD = new ParseField("token");
    private static final ParseField ISSUED_AT_FIELD = new ParseField("issued_at_epoch_seconds");
    private static final ParseField EXPIRES_AT_FIELD = new ParseField("expires_at_epoch_seconds");

    /**
     * Parses the workload-identity-issuer JSON token response. Unknown fields are tolerated so the
     * issuer can introduce additive fields without breaking clients. Required-field absence and
     * type mismatches are surfaced by the parser as {@code XContentParseException} (a subclass of
     * {@link IllegalArgumentException}); the {@link IssueTokenResponse} canonical constructor
     * performs cross-field validation ({@code expiresAt >= issuedAt}, non-empty token) and may
     * itself throw {@link IllegalArgumentException}. Both are rewrapped in
     * {@link WorkloadIdentityIssuerException} at the call site.
     */
    private static final ConstructingObjectParser<IssueTokenResponse, Void> RESPONSE_PARSER = new ConstructingObjectParser<>(
        "workload_identity_issuer_response",
        true,
        args -> new IssueTokenResponse((String) args[0], Instant.ofEpochSecond((Long) args[1]), Instant.ofEpochSecond((Long) args[2]))
    );
    static {
        RESPONSE_PARSER.declareString(constructorArg(), TOKEN_FIELD);
        RESPONSE_PARSER.declareLong(constructorArg(), ISSUED_AT_FIELD);
        RESPONSE_PARSER.declareLong(constructorArg(), EXPIRES_AT_FIELD);
    }

    private final WorkloadIdentityHttpClientManager httpClientManager;
    private final URI tokenEndpoint;
    private final RequestConfig requestConfig;
    private final long maxResponseSizeBytes;
    private final ThreadPool threadPool;
    private final long refreshBeforeExpiryMillis;

    /**
     * Tokens cached, keyed by request shape. The value is a {@link SubscribableListener} so that
     * concurrent missing callers for the same key share a single in-flight HTTP fetch. Each entry
     * is removed by a scheduled task at {@code expiresAt - refresh_before_expiry}, so a hit
     * against this map is always fresh by construction.
     */
    private final ConcurrentMap<IssueTokenRequest, SubscribableListener<IssueTokenResponse>> tokens = ConcurrentCollections
        .newConcurrentMap();

    public HttpsWorkloadIdentityIssuerClient(
        Settings settings,
        WorkloadIdentityHttpClientManager httpClientManager,
        ThreadPool threadPool
    ) {
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
        SubscribableListener<IssueTokenResponse> fresh = new SubscribableListener<>();
        SubscribableListener<IssueTokenResponse> existing = tokens.putIfAbsent(request, fresh);
        if (existing != null) {
            existing.addListener(listener);
            return;
        }
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
     * Dispatches the token request and routes every outcome through {@code listener}. The outer
     * try/catch is contract enforcement: callers ({@link #startFetch}) rely on completion happening
     * exclusively via the listener for cache lifecycle correctness, so any exception that escapes
     * the synchronous setup path (request rendering, Apache HC client retrieval, request
     * construction, async submission) is funneled into {@link ActionListener#onFailure} rather than
     * being allowed to propagate. The {@link FutureCallback} handles the asynchronous outcomes.
     */
    private void fetchToken(IssueTokenRequest request, ActionListener<IssueTokenResponse> listener) {
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
            builder.field("audience", request.audience());
            if (request.region() != null) {
                builder.field("region", request.region());
            }
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
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, body)) {
            return RESPONSE_PARSER.parse(parser, null);
        } catch (IllegalArgumentException e) {
            // Covers both XContentParseException (parser-level: missing field, wrong type) and any
            // IllegalArgumentException raised by the IssueTokenResponse canonical constructor.
            throw new WorkloadIdentityIssuerException("failed to parse workload-identity-issuer response: " + e.getMessage(), status, e);
        }
    }

}
