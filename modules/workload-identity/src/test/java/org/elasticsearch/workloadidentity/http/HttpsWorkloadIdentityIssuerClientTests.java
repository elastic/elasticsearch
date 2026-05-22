/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.workloadidentity.http;

import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsExchange;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.PemKeyConfig;
import org.elasticsearch.common.ssl.PemTrustConfig;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.workloadidentity.WorkloadIdentityIssuerClient.IssueTokenRequest;
import org.elasticsearch.workloadidentity.WorkloadIdentityIssuerClient.IssueTokenResponse;
import org.elasticsearch.workloadidentity.WorkloadIdentityIssuerClient.WorkloadIdentityIssuerException;
import org.elasticsearch.workloadidentity.WorkloadIdentityIssuerSettings;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/**
 * Network-transport tests for {@link HttpsWorkloadIdentityIssuerClient}. A mock HTTPS server is
 * stood up with {@link MockHttpServer#createHttps} and configured to require a client certificate,
 * so each test exercises the mTLS handshake the production transport relies on.
 *
 * <p>The PEM material is the same set used by {@code modules/reindex}'s SSL tests: {@code ca.crt}
 * signs both the server cert ({@code issuer/issuer.crt}, SANs cover localhost + 127.0.0.1) and the
 * client cert ({@code node/node.crt}, subject {@code CN=client}). See the README in this test
 * resources directory for regeneration instructions.
 */
@SuppressForbidden(reason = "use the JDK http server as a controllable mTLS workload-identity-issuer mock")
public class HttpsWorkloadIdentityIssuerClientTests extends ESTestCase {

    private static HttpsServer server;
    private static volatile Consumer<HttpsExchange> handler = exchange -> {};

    @BeforeClass
    public static void setupHttpServer() throws Exception {
        final InetSocketAddress address = new InetSocketAddress(InetAddress.getLoopbackAddress().getHostAddress(), 0);
        final SSLContext sslContext = buildServerSslContext();
        server = MockHttpServer.createHttps(address, 0);
        server.setHttpsConfigurator(new ClientAuthHttpsConfigurator(sslContext));
        server.createContext("/", exchange -> {
            if (exchange instanceof HttpsExchange httpsExchange) {
                try {
                    handler.accept(httpsExchange);
                } finally {
                    exchange.close();
                }
            } else {
                exchange.close();
                throw new IllegalStateException("expected an HttpsExchange, got " + exchange.getClass());
            }
        });
        server.start();
    }

    @AfterClass
    public static void shutdownHttpServer() {
        if (server != null) {
            server.stop(0);
            server = null;
        }
        handler = exchange -> {};
    }

    private static SSLContext buildServerSslContext() throws Exception {
        final SSLContext sslContext = SSLContext.getInstance("TLS");
        final char[] password = "http-password".toCharArray();
        final Path cert = getResourceDataPath(HttpsWorkloadIdentityIssuerClientTests.class, "issuer/issuer.crt");
        final Path key = getResourceDataPath(HttpsWorkloadIdentityIssuerClientTests.class, "issuer/issuer.key");
        final Path configPath = cert.getParent().getParent();
        final PemKeyConfig keyConfig = new PemKeyConfig(cert.toString(), key.toString(), password, configPath);
        final X509ExtendedKeyManager keyManager = keyConfig.createKeyManager();

        final Path ca = getResourceDataPath(HttpsWorkloadIdentityIssuerClientTests.class, "ca.crt");
        final X509ExtendedTrustManager trustManager = new PemTrustConfig(Collections.singletonList(ca.toString()), configPath)
            .createTrustManager();

        sslContext.init(new KeyManager[] { keyManager }, new TrustManager[] { trustManager }, null);
        return sslContext;
    }

    /**
     * End-to-end success path: client trusts the server CA, presents the expected client cert,
     * sends a well-formed POST /token JSON body, and parses the JWT response into an
     * {@link IssueTokenResponse}.
     */
    public void testSuccessfulTokenIssuance() throws Exception {
        final AtomicReference<String> capturedBody = new AtomicReference<>();
        final AtomicReference<Certificate[]> capturedClientCerts = new AtomicReference<>();
        handler = exchange -> {
            try {
                capturedClientCerts.set(exchange.getSSLSession().getPeerCertificates());
            } catch (SSLPeerUnverifiedException e) {
                capturedClientCerts.set(null);
            }
            try {
                capturedBody.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
                final String body = """
                    {
                      "token": "header.payload.sig",
                      "issued_at_epoch_seconds": 1716000000,
                      "expires_at_epoch_seconds": 1716003600
                    }
                    """;
                final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, bytes.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(bytes);
                }
            } catch (IOException e) {
                throw new AssertionError("failed writing mock response", e);
            }
        };

        try (ClientHarness harness = new ClientHarness(clientSettingsWithIssuerUrl().build())) {
            final IssueTokenResponse response = awaitToken(harness, new IssueTokenRequest("arn:aws:iam::1:role/r", "us-east-1"));
            assertEquals("header.payload.sig", response.token());
            assertEquals(1716000000L, response.issuedAt().getEpochSecond());
            assertEquals(1716003600L, response.expiresAt().getEpochSecond());
        }

        // The handler ran on a server-side thread; both captures must be visible because the
        // future completes only after the server has fully written its response.
        assertThat(capturedBody.get(), containsString("\"audience\":\"arn:aws:iam::1:role/r\""));
        assertThat(capturedBody.get(), containsString("\"region\":\"us-east-1\""));

        final Certificate[] certs = capturedClientCerts.get();
        assertNotNull("client cert chain must reach the server (mTLS handshake)", certs);
        assertThat(certs, arrayWithSize(1));
        assertThat(certs[0], instanceOf(X509Certificate.class));
        final X509Certificate clientCert = (X509Certificate) certs[0];
        assertEquals("CN=client", clientCert.getSubjectX500Principal().getName());
    }

    /**
     * The {@code region} field is omitted from the JSON body when the caller does not supply one.
     */
    public void testRegionIsOmittedWhenNotProvided() throws Exception {
        final AtomicReference<String> capturedBody = new AtomicReference<>();
        handler = exchange -> {
            try {
                capturedBody.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
                final byte[] bytes = """
                    {
                      "token": "abc.def.ghi",
                      "issued_at_epoch_seconds": 100,
                      "expires_at_epoch_seconds": 200
                    }
                    """.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, bytes.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(bytes);
                }
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        };

        try (ClientHarness harness = new ClientHarness(clientSettingsWithIssuerUrl().build())) {
            awaitToken(harness, new IssueTokenRequest("some-audience"));
        }

        assertThat(capturedBody.get(), containsString("\"audience\":\"some-audience\""));
        assertThat("region must not leak into the wire format when null", capturedBody.get(), not(containsString("region")));
    }

    /**
     * Non-2xx responses surface as {@link WorkloadIdentityIssuerException} via the listener with the
     * HTTP status preserved. The exception message is bounded to the status code; the response body
     * is reachable at DEBUG only ({@link #testServerErrorBodyIsLoggedAtDebug()}).
     */
    public void testServerErrorIsReportedToListener() throws Exception {
        handler = exchange -> {
            try {
                final byte[] body = "{\"error\":\"region not configured\"}".getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(400, body.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(body);
                }
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        };

        try (ClientHarness harness = new ClientHarness(clientSettingsWithIssuerUrl().build())) {
            final PlainActionFuture<IssueTokenResponse> future = new PlainActionFuture<>();
            harness.client.issueToken(new IssueTokenRequest("aud", "us-east-1"), future);
            final ExecutionException ex = expectThrows(ExecutionException.class, () -> future.get(10, TimeUnit.SECONDS));
            assertThat(ex.getCause(), instanceOf(WorkloadIdentityIssuerException.class));
            final WorkloadIdentityIssuerException cause = (WorkloadIdentityIssuerException) ex.getCause();
            assertEquals(400, cause.statusCode());
            assertThat(cause.getMessage(), containsString("HTTP 400"));
            // Regression guard: the response body must not leak into the exception message.
            assertThat(cause.getMessage(), not(containsString("region not configured")));
        }
    }

    /**
     * Operators investigating an issuer failure can opt in to the response body via DEBUG logging on
     * {@link HttpsWorkloadIdentityIssuerClient}. This is the only place the body is exposed.
     */
    public void testServerErrorBodyIsLoggedAtDebug() throws Exception {
        handler = exchange -> {
            try {
                final byte[] body = "{\"error\":\"region not configured\"}".getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(400, body.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(body);
                }
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        };

        final Logger clientLogger = LogManager.getLogger(HttpsWorkloadIdentityIssuerClient.class);
        final Level previousLevel = clientLogger.getLevel();
        Loggers.setLevel(clientLogger, Level.DEBUG);
        try (ClientHarness harness = new ClientHarness(clientSettingsWithIssuerUrl().build())) {
            MockLog.assertThatLogger(() -> {
                final PlainActionFuture<IssueTokenResponse> future = new PlainActionFuture<>();
                harness.client.issueToken(new IssueTokenRequest("aud", "us-east-1"), future);
                final ExecutionException ex = expectThrows(ExecutionException.class, () -> future.get(10, TimeUnit.SECONDS));
                assertThat(ex.getCause(), instanceOf(WorkloadIdentityIssuerException.class));
            },
                HttpsWorkloadIdentityIssuerClient.class,
                new MockLog.SeenEventExpectation(
                    "issuer error body at DEBUG",
                    HttpsWorkloadIdentityIssuerClient.class.getCanonicalName(),
                    Level.DEBUG,
                    "*returned HTTP 400 with response body: *region not configured*"
                )
            );
        } finally {
            Loggers.setLevel(clientLogger, previousLevel);
        }
    }

    /**
     * 200 OK with no response body collapses to a parse failure inside {@code parseSuccess},
     * which guards against issuers that drop the JSON payload while still signalling success.
     */
    public void testEmptyOkBodyFailsListener() throws Exception {
        handler = exchange -> {
            try {
                exchange.sendResponseHeaders(200, -1);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        };

        try (ClientHarness harness = new ClientHarness(clientSettingsWithIssuerUrl().build())) {
            final WorkloadIdentityIssuerException cause = awaitFailure(harness, new IssueTokenRequest("aud", "us-east-1"));
            assertEquals(200, cause.statusCode());
            assertThat(cause.getMessage(), containsString("empty response body"));
        }
    }

    /**
     * A 200 OK body that is not valid JSON surfaces via {@link WorkloadIdentityIssuerException}
     * with the parser exception preserved as the cause and the HTTP status carried through.
     */
    public void testMalformedJsonFailsListener() throws Exception {
        respondWith200JsonBody("not json {{".getBytes(StandardCharsets.UTF_8));

        try (ClientHarness harness = new ClientHarness(clientSettingsWithIssuerUrl().build())) {
            final WorkloadIdentityIssuerException cause = awaitFailure(harness, new IssueTokenRequest("aud", "us-east-1"));
            assertEquals(200, cause.statusCode());
            assertThat(cause.getMessage(), containsString("failed to parse"));
            assertThat(cause.getCause(), instanceOf(IllegalArgumentException.class));
        }
    }

    /**
     * A well-formed JSON body that omits a required field (here {@code token}) is rejected by
     * the {@code ConstructingObjectParser}; the parser exception is caught by {@code parseSuccess}
     * and rewrapped as a {@link WorkloadIdentityIssuerException}.
     */
    public void testMissingRequiredFieldFailsListener() throws Exception {
        respondWith200JsonBody("""
            {
              "issued_at_epoch_seconds": 100,
              "expires_at_epoch_seconds": 200
            }
            """.getBytes(StandardCharsets.UTF_8));

        try (ClientHarness harness = new ClientHarness(clientSettingsWithIssuerUrl().build())) {
            final WorkloadIdentityIssuerException cause = awaitFailure(harness, new IssueTokenRequest("aud", "us-east-1"));
            assertEquals(200, cause.statusCode());
            assertThat(cause.getMessage(), containsString("failed to parse"));
            assertThat(cause.getCause(), instanceOf(IllegalArgumentException.class));
        }
    }

    /**
     * Cross-field validation in the {@link IssueTokenResponse} canonical constructor (here
     * {@code expiresAt < issuedAt}) is caught by {@code parseSuccess} alongside parser exceptions.
     * {@code ConstructingObjectParser} wraps the constructor failure in one or more
     * {@code XContentParseException}s, so we walk the cause chain to find the original
     * {@link IllegalArgumentException} rather than asserting a fixed nesting depth.
     */
    public void testExpiresBeforeIssuedFailsListener() throws Exception {
        respondWith200JsonBody("""
            {
              "token": "header.payload.sig",
              "issued_at_epoch_seconds": 200,
              "expires_at_epoch_seconds": 100
            }
            """.getBytes(StandardCharsets.UTF_8));

        try (ClientHarness harness = new ClientHarness(clientSettingsWithIssuerUrl().build())) {
            final WorkloadIdentityIssuerException cause = awaitFailure(harness, new IssueTokenRequest("aud", "us-east-1"));
            assertEquals(200, cause.statusCode());
            assertThat(cause.getMessage(), containsString("failed to parse"));
            assertThat(cause.getCause(), instanceOf(IllegalArgumentException.class));
            assertThat(rootCauseMessage(cause), containsString("must not be before"));
        }
    }

    /**
     * The mock server is configured with {@code needClientAuth=true} and {@code wantClientAuth=true};
     * a client missing key material must fail at the TLS handshake. The exact exception type is
     * sensitive to TLS stack + JDK version, so we assert via the {@link IOException} surface.
     */
    public void testHandshakeFailsWithoutClientCert() throws Exception {
        final Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .put(WorkloadIdentityIssuerSettings.ISSUER_URL_SETTING.getKey(), issuerBaseUrl())
            .putList(WorkloadIdentitySslConfig.SETTING_PREFIX + "certificate_authorities", getDataPath("ca.crt").toString())
            .build();

        try (ClientHarness harness = new ClientHarness(settings)) {
            final PlainActionFuture<IssueTokenResponse> future = new PlainActionFuture<>();
            harness.client.issueToken(new IssueTokenRequest("aud"), future);
            final ExecutionException ex = expectThrows(ExecutionException.class, () -> future.get(10, TimeUnit.SECONDS));
            assertThat(
                "expected IO/SSL failure when the client cannot complete mTLS, got: " + ex.getCause(),
                ex.getCause(),
                instanceOf(IOException.class)
            );
        }
    }

    /**
     * Without trusting the CA, the client's hostname/cert verification must reject the server
     * during the handshake.
     */
    public void testClientRejectsUntrustedServer() throws Exception {
        final Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .put(WorkloadIdentityIssuerSettings.ISSUER_URL_SETTING.getKey(), issuerBaseUrl())
            // Deliberately omit certificate_authorities so the server cert is not trusted.
            .put(WorkloadIdentitySslConfig.SETTING_PREFIX + "certificate", getDataPath("node/node.crt").toString())
            .put(WorkloadIdentitySslConfig.SETTING_PREFIX + "key", getDataPath("node/node.key").toString())
            .build();

        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(WorkloadIdentitySslConfig.SETTING_PREFIX + "secure_key_passphrase", "client-password");
        final Settings withSecure = Settings.builder().put(settings).setSecureSettings(secureSettings).build();

        try (ClientHarness harness = new ClientHarness(withSecure)) {
            final PlainActionFuture<IssueTokenResponse> future = new PlainActionFuture<>();
            harness.client.issueToken(new IssueTokenRequest("aud"), future);
            final ExecutionException ex = expectThrows(ExecutionException.class, () -> future.get(10, TimeUnit.SECONDS));
            assertThat(ex.getCause(), instanceOf(IOException.class));
        }
    }

    /**
     * Two issuances of the same request collapse onto a single HTTP fetch while the cached entry
     * is still fresh.
     */
    public void testCachedTokenIsReusedAcrossCallsWithSameRequest() throws Exception {
        final AtomicInteger callCount = new AtomicInteger();
        final long farFutureEpochSecond = (System.currentTimeMillis() / 1000) + 3_600;

        try (ClientHarness harness = new ClientHarness(clientSettingsWithIssuerUrl().build())) {
            installCountingHandlerOver(callCount, farFutureEpochSecond);
            final IssueTokenRequest request = new IssueTokenRequest("aud", "us-east-1");
            final IssueTokenResponse first = awaitToken(harness, request);
            final IssueTokenResponse second = awaitToken(harness, request);
            assertEquals("second issuance must come from the cache", 1, callCount.get());
            assertEquals(first, second);
        }
    }

    /**
     * Distinct {@link IssueTokenRequest} values do not alias in the cache; each one drives its own
     * HTTP fetch.
     */
    public void testDistinctRequestsAreNotCacheAliased() throws Exception {
        final AtomicInteger callCount = new AtomicInteger();
        final long farFutureEpochSecond = (System.currentTimeMillis() / 1000) + 3_600;

        try (ClientHarness harness = new ClientHarness(clientSettingsWithIssuerUrl().build())) {
            installCountingHandlerOver(callCount, farFutureEpochSecond);
            awaitToken(harness, new IssueTokenRequest("aud", "us-east-1"));
            awaitToken(harness, new IssueTokenRequest("aud", "eu-west-1"));
            awaitToken(harness, new IssueTokenRequest("aud", null));
            assertEquals("each (audience, region) tuple is a distinct cache key", 3, callCount.get());
        }
    }

    /**
     * A short-lived token is evicted at {@code expiresAt - refresh_before_expiry}, after which
     * the next caller pays the cost of a fresh HTTP fetch.
     */
    public void testCachedTokenIsEvictedNearExpiry() throws Exception {
        final AtomicInteger callCount = new AtomicInteger();

        try (
            ClientHarness harness = new ClientHarness(
                clientSettingsWithIssuerUrl()
                    // Aggressive refresh window so the test runs in ~1 second of wall-clock time.
                    .put(WorkloadIdentityIssuerSettings.TOKEN_CACHE_REFRESH_BEFORE_EXPIRY.getKey(), TimeValue.timeValueMillis(50))
                    .build()
            )
        ) {
            handler = exchange -> respondWithToken(exchange, (System.currentTimeMillis() / 1000) + 1, callCount);
            final IssueTokenRequest request = new IssueTokenRequest("aud", "us-east-1");
            awaitToken(harness, request);
            assertEquals(1, callCount.get());
            assertBusy(() -> {
                awaitToken(harness, request);
                assertEquals("eviction must have fired before this issuance", 2, callCount.get());
            }, 5, TimeUnit.SECONDS);
        }
    }

    /**
     * Failed fetches are not cached: the next caller re-issues and may succeed.
     */
    public void testFailedFetchIsNotCached() throws Exception {
        final AtomicInteger callCount = new AtomicInteger();
        final long farFutureEpochSecond = (System.currentTimeMillis() / 1000) + 3_600;
        handler = exchange -> {
            final int n = callCount.incrementAndGet();
            try {
                if (n == 1) {
                    final byte[] body = "{\"error\":\"transient\"}".getBytes(StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(500, body.length);
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(body);
                    }
                } else {
                    respondWithToken(exchange, farFutureEpochSecond);
                }
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        };

        try (ClientHarness harness = new ClientHarness(clientSettingsWithIssuerUrl().build())) {
            final IssueTokenRequest request = new IssueTokenRequest("aud", "us-east-1");
            final PlainActionFuture<IssueTokenResponse> firstAttempt = new PlainActionFuture<>();
            harness.client.issueToken(request, firstAttempt);
            expectThrows(ExecutionException.class, () -> firstAttempt.get(10, TimeUnit.SECONDS));

            final IssueTokenResponse second = awaitToken(harness, request);
            assertNotNull(second.token());
            assertEquals("the failure must not have been cached", 2, callCount.get());
        }
    }

    /**
     * Synchronous failures inside {@code fetchToken} (here, {@code getHttpClient()} throws because
     * the HTTP client manager has been closed) must not poison the cache: each {@code issueToken}
     * call must re-enter {@code fetchToken} and surface a fresh failure rather than replay a
     * cached one from a previous attempt.
     *
     * <p>The synchronous path is the historically-fragile one. An in-line failure handler runs on
     * the calling thread, very close to the cache mutation that publishes the entry, so a
     * regression in how the entry's lifecycle is managed can leave a failed entry stuck in
     * {@code tokens} and force every later caller for the same key to replay the same cached
     * failure.
     *
     * <p>The behavioural check is {@code assertNotSame} on the two exception causes: a correct
     * implementation produces a fresh {@link IllegalStateException} for each call, whereas a
     * caching regression would deliver the same cached instance to both callers.
     *
     * <p>Complements {@link #testFailedFetchIsNotCached}, which exercises the analogous invariant
     * on the asynchronous failure path (HTTP 500 from the issuer).
     */
    public void testSynchronousFetchFailureIsNotCached() throws Exception {
        try (ClientHarness harness = new ClientHarness(clientSettingsWithIssuerUrl().build())) {
            // Closing the manager forces getHttpClient() to throw synchronously inside fetchToken,
            // exercising the sync-failure path. (Idempotent close(): the harness's own close() is
            // a no-op the second time around.)
            harness.manager.close();

            final IssueTokenRequest request = new IssueTokenRequest("aud", "us-east-1");

            final PlainActionFuture<IssueTokenResponse> first = new PlainActionFuture<>();
            harness.client.issueToken(request, first);
            final ExecutionException ex1 = expectThrows(ExecutionException.class, () -> first.get(10, TimeUnit.SECONDS));

            final PlainActionFuture<IssueTokenResponse> second = new PlainActionFuture<>();
            harness.client.issueToken(request, second);
            final ExecutionException ex2 = expectThrows(ExecutionException.class, () -> second.get(10, TimeUnit.SECONDS));

            assertNotSame("synchronous failures must not be cached", ex1.getCause(), ex2.getCause());
        }
    }

    /**
     * Covers the defensive catch in {@code HttpsWorkloadIdentityIssuerClient#scheduleEviction}: when
     * the {@link ThreadPool} rejects the eviction schedule (e.g. the pool is shutting down), the
     * client must evict the cached entry inline so the next caller pays for a fresh fetch rather
     * than reading a stale, never-evicted token.
     *
     * <p>{@link ThrowingScheduleThreadPool} is flipped into the rejecting state after
     * {@link ClientHarness} has finished starting the connection evictor (which uses
     * {@code scheduleWithFixedDelay(...)}, a different overload), so only the eviction schedule for
     * the issued token sees the rejection.
     */
    public void testEvictionScheduleFailureEvictsImmediately() throws Exception {
        final ThrowingScheduleThreadPool throwingPool = new ThrowingScheduleThreadPool(
            HttpsWorkloadIdentityIssuerClientTests.class.getSimpleName()
        );
        final AtomicInteger callCount = new AtomicInteger();
        final long farFutureEpochSecond = (System.currentTimeMillis() / 1000) + 3_600;

        try (ClientHarness harness = new ClientHarness(clientSettingsWithIssuerUrl().build(), throwingPool)) {
            installCountingHandlerOver(callCount, farFutureEpochSecond);
            // Flip into the rejecting state only after the harness has finished starting up, so
            // the connection evictor's scheduleWithFixedDelay(...) is unaffected; from here on,
            // the eviction schedule in HttpsWorkloadIdentityIssuerClient#scheduleEviction will be
            // the only call to the 3-arg schedule(...) overload and will hit the catch.
            throwingPool.rejecting.set(true);
            final IssueTokenRequest request = new IssueTokenRequest("aud", "us-east-1");

            awaitToken(harness, request);
            // The catch block runs tokens.remove(...) on the IO reactor thread after the future
            // has already completed, so the second caller may briefly observe the still-cached
            // entry. assertBusy retries until the inline eviction is visible and the next
            // issueToken drives a fresh HTTP fetch.
            assertBusy(() -> {
                awaitToken(harness, request);
                assertEquals("inline eviction must have removed the cached entry", 2, callCount.get());
            }, 5, TimeUnit.SECONDS);
        }
    }

    private static void installCountingHandlerOver(AtomicInteger callCount, long expiresAtEpochSeconds) {
        handler = exchange -> respondWithToken(exchange, expiresAtEpochSeconds, callCount);
    }

    private static void respondWithToken(HttpsExchange exchange, long expiresAtEpochSeconds) {
        respondWithToken(exchange, expiresAtEpochSeconds, null);
    }

    private static void respondWithToken(HttpsExchange exchange, long expiresAtEpochSeconds, AtomicInteger callCount) {
        if (callCount != null) {
            callCount.incrementAndGet();
        }
        try {
            final long issuedAtEpochSeconds = System.currentTimeMillis() / 1000;
            final String body = "{\"token\":\"header.payload.sig\",\"issued_at_epoch_seconds\":"
                + issuedAtEpochSeconds
                + ",\"expires_at_epoch_seconds\":"
                + expiresAtEpochSeconds
                + "}";
            final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        } catch (IOException e) {
            throw new AssertionError("failed writing mock response", e);
        }
    }

    /**
     * Convenience: build a {@link Settings.Builder} preloaded with the trust + key material the
     * happy-path tests need so they only need to add the issuer URL.
     */
    private Settings.Builder clientSettings() throws Exception {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(WorkloadIdentitySslConfig.SETTING_PREFIX + "secure_key_passphrase", "client-password");
        return Settings.builder()
            .put("path.home", createTempDir())
            .putList(WorkloadIdentitySslConfig.SETTING_PREFIX + "certificate_authorities", getDataPath("ca.crt").toString())
            .put(WorkloadIdentitySslConfig.SETTING_PREFIX + "certificate", getDataPath("node/node.crt").toString())
            .put(WorkloadIdentitySslConfig.SETTING_PREFIX + "key", getDataPath("node/node.key").toString())
            .setSecureSettings(secureSettings);
    }

    /**
     * {@link #clientSettings()} preloaded with the issuer URL. Returns a {@link Settings.Builder}
     * so tests that need to layer additional settings on top can keep chaining {@code .put(...)}.
     */
    private Settings.Builder clientSettingsWithIssuerUrl() throws Exception {
        return clientSettings().put(WorkloadIdentityIssuerSettings.ISSUER_URL_SETTING.getKey(), issuerBaseUrl());
    }

    private static String issuerBaseUrl() {
        // Server cert SANs include "localhost"; Apache HC's DefaultHostnameVerifier performs
        // hostname verification against the cert, so use "localhost" rather than the numeric
        // loopback IP.
        return "https://localhost:" + server.getAddress().getPort();
    }

    private static IssueTokenResponse awaitToken(ClientHarness harness, IssueTokenRequest request) throws Exception {
        final PlainActionFuture<IssueTokenResponse> future = new PlainActionFuture<>();
        harness.client.issueToken(request, future);
        return future.get(10, TimeUnit.SECONDS);
    }

    /**
     * Awaits a token request that is expected to fail, asserts the cause is a
     * {@link WorkloadIdentityIssuerException}, and returns it for further assertions.
     */
    private static WorkloadIdentityIssuerException awaitFailure(ClientHarness harness, IssueTokenRequest request) throws Exception {
        final PlainActionFuture<IssueTokenResponse> future = new PlainActionFuture<>();
        harness.client.issueToken(request, future);
        final ExecutionException ex = expectThrows(ExecutionException.class, () -> future.get(10, TimeUnit.SECONDS));
        assertThat(ex.getCause(), instanceOf(WorkloadIdentityIssuerException.class));
        return (WorkloadIdentityIssuerException) ex.getCause();
    }

    /**
     * Walks the cause chain of {@code throwable} and returns the deepest non-null message. Used to
     * assert on the original {@link IllegalArgumentException} from the {@code IssueTokenResponse}
     * canonical constructor without being coupled to how many {@code XContentParseException}
     * wrappers {@code ConstructingObjectParser} happens to add along the way.
     */
    private static String rootCauseMessage(Throwable throwable) {
        Throwable current = throwable;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current.getMessage();
    }

    /**
     * Installs a handler that responds 200 OK with the given JSON body. Used by the
     * {@code parseSuccess} negative-path tests to inject malformed or shape-violating payloads.
     */
    private static void respondWith200JsonBody(byte[] body) {
        handler = exchange -> {
            try {
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, body.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(body);
                }
            } catch (IOException e) {
                throw new AssertionError("failed writing mock response", e);
            }
        };
    }

    /**
     * Encapsulates the full client wiring for a single test: SSL config (with a no-op resource
     * watcher, since file watches are not exercised), HTTP client manager, and the
     * {@link HttpsWorkloadIdentityIssuerClient} under test. Closing the harness releases the
     * HTTP client and stops the background eviction task.
     */
    private static final class ClientHarness implements AutoCloseable {
        final ThreadPool threadPool;
        final WorkloadIdentitySslConfig sslConfig;
        final WorkloadIdentityHttpClientManager manager;
        final HttpsWorkloadIdentityIssuerClient client;

        ClientHarness(Settings settings) {
            this(settings, new TestThreadPool(HttpsWorkloadIdentityIssuerClientTests.class.getSimpleName()));
        }

        ClientHarness(Settings settings, ThreadPool threadPool) {
            this.threadPool = threadPool;
            final Environment environment = TestEnvironment.newEnvironment(settings);
            this.sslConfig = new WorkloadIdentitySslConfig(settings, environment);
            this.manager = new WorkloadIdentityHttpClientManager(settings, sslConfig, threadPool);
            // Apache HC's async client and the connection evictor must be started explicitly
            // before requests are dispatched (mirrors the production WorkloadIdentityPlugin
            // lifecycle, which starts the manager from createComponents).
            this.manager.start();
            this.client = new HttpsWorkloadIdentityIssuerClient(settings, manager, threadPool);
        }

        @Override
        public void close() {
            try {
                manager.close();
            } finally {
                ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
            }
        }
    }

    /**
     * Test {@link ThreadPool} whose 3-argument {@link ThreadPool#schedule(Runnable, TimeValue, Executor)
     * schedule} overload conditionally rejects with an {@link EsRejectedExecutionException}. Only that
     * overload is intercepted; {@code scheduleWithFixedDelay(...)} (used by
     * {@link org.elasticsearch.workloadidentity.common.HttpConnectionEvictor}) is left alone so the
     * connection-evictor startup in {@link ClientHarness} is unaffected. Used to exercise the catch
     * branch in {@code HttpsWorkloadIdentityIssuerClient#scheduleEviction}.
     */
    private static final class ThrowingScheduleThreadPool extends TestThreadPool {
        final AtomicBoolean rejecting = new AtomicBoolean(false);

        ThrowingScheduleThreadPool(String name) {
            super(name);
        }

        @Override
        public Scheduler.ScheduledCancellable schedule(Runnable command, TimeValue delay, Executor executor) {
            if (rejecting.get()) {
                throw new EsRejectedExecutionException("scheduler rejecting (test)", true);
            }
            return super.schedule(command, delay, executor);
        }
    }

    @SuppressForbidden(reason = "use http server")
    private static class ClientAuthHttpsConfigurator extends HttpsConfigurator {
        ClientAuthHttpsConfigurator(SSLContext sslContext) {
            super(sslContext);
        }

        @Override
        public void configure(HttpsParameters params) {
            // Match the production "mTLS required" contract: reject connections without a valid client cert.
            params.setNeedClientAuth(true);
        }
    }
}
