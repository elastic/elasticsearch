/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.workloadidentity;

import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsExchange;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;

import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
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
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.workloadidentity.spi.WorkloadIdentityIssuerClient.IssueTokenRequest;
import org.elasticsearch.workloadidentity.spi.WorkloadIdentityIssuerClient.IssueTokenResponse;
import org.elasticsearch.workloadidentity.spi.WorkloadIdentityIssuerClient.WorkloadIdentityIssuerException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
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
    }

    @After
    public void resetHandler() {
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
        final long expiresAtSeconds = (System.currentTimeMillis() / 1000) + 3_600;
        handler = exchange -> {
            try {
                capturedClientCerts.set(exchange.getSSLSession().getPeerCertificates());
            } catch (SSLPeerUnverifiedException e) {
                capturedClientCerts.set(null);
            }
            try {
                capturedBody.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
                final byte[] bytes = """
                    {
                      "token": "header.payload.sig",
                      "expires_at": %s
                    }
                    """.formatted(expiresAtSeconds).getBytes(StandardCharsets.UTF_8);
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
            final IssueTokenResponse response = awaitToken(harness, new IssueTokenRequest("arn:aws:iam::1:role/r"));
            assertEquals("header.payload.sig", response.token());
            assertEquals(expiresAtSeconds, response.expiresAt().getEpochSecond());
        }

        // The handler ran on a server-side thread; the capture must be visible because the
        // future completes only after the server has fully written its response.
        assertThat(capturedBody.get(), containsString("\"aud\":\"arn:aws:iam::1:role/r\""));

        final Certificate[] certs = capturedClientCerts.get();
        assertNotNull("client cert chain must reach the server (mTLS handshake)", certs);
        assertThat(certs, arrayWithSize(1));
        assertThat(certs[0], instanceOf(X509Certificate.class));
        final X509Certificate clientCert = (X509Certificate) certs[0];
        assertEquals("CN=client", clientCert.getSubjectX500Principal().getName());
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
            harness.client.issueToken(new IssueTokenRequest("aud"), future);
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
                harness.client.issueToken(new IssueTokenRequest("aud"), future);
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
            final WorkloadIdentityIssuerException cause = awaitFailure(harness, new IssueTokenRequest("aud"));
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
            final WorkloadIdentityIssuerException cause = awaitFailure(harness, new IssueTokenRequest("aud"));
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
              "expires_at": 200
            }
            """.getBytes(StandardCharsets.UTF_8));

        try (ClientHarness harness = new ClientHarness(clientSettingsWithIssuerUrl().build())) {
            final WorkloadIdentityIssuerException cause = awaitFailure(harness, new IssueTokenRequest("aud"));
            assertEquals(200, cause.statusCode());
            assertThat(cause.getMessage(), containsString("failed to parse"));
            assertThat(cause.getCause(), instanceOf(IllegalArgumentException.class));
        }
    }

    /**
     * A response whose {@code expires_at} is clearly in the past is rejected by
     * {@code validateExpiry}.
     */
    public void testExpiresAtInThePastFailsListener() throws Exception {
        final long pastEpochSeconds = (System.currentTimeMillis() / 1000) - 60;
        respondWith200JsonBody("""
            {
              "token": "header.payload.sig",
              "expires_at": %s
            }
            """.formatted(pastEpochSeconds).getBytes(StandardCharsets.UTF_8));

        try (ClientHarness harness = new ClientHarness(clientSettingsWithIssuerUrl().build())) {
            final WorkloadIdentityIssuerException cause = awaitFailure(harness, new IssueTokenRequest("aud"));
            assertEquals(200, cause.statusCode());
            assertThat(cause.getMessage(), containsString("is in the past"));
        }
    }

    /**
     * A response whose {@code expires_at} is more than {@link HttpsWorkloadIdentityIssuerClient#MAX_TOKEN_LIFETIME_SECONDS}
     * in the future is rejected by the sanity ceiling in {@code validateExpiry}. Primarily a guard
     * against response-encoding bugs (e.g. epoch-millis read as epoch-seconds) and arithmetic-overflow
     * values in downstream eviction scheduling.
     */
    public void testExpiresAtTooFarInFutureFailsListener() throws Exception {
        final long farFutureEpochSeconds = (System.currentTimeMillis() / 1000)
            + HttpsWorkloadIdentityIssuerClient.MAX_TOKEN_LIFETIME_SECONDS + 60;
        respondWith200JsonBody("""
            {
              "token": "header.payload.sig",
              "expires_at": %s
            }
            """.formatted(farFutureEpochSeconds).getBytes(StandardCharsets.UTF_8));

        try (ClientHarness harness = new ClientHarness(clientSettingsWithIssuerUrl().build())) {
            final WorkloadIdentityIssuerException cause = awaitFailure(harness, new IssueTokenRequest("aud"));
            assertEquals(200, cause.statusCode());
            assertThat(cause.getMessage(), containsString("exceeds the maximum acceptable lifetime"));
        }
    }

    /**
     * A response whose {@code expires_at} parses to a valid {@code long} but is outside the range
     * accepted by {@link java.time.Instant#ofEpochSecond(long)} surfaces as
     * {@link WorkloadIdentityIssuerException} with the {@link java.time.DateTimeException} preserved
     * in the cause chain. The lambda's {@code DateTimeException} is wrapped by
     * {@code ConstructingObjectParser} as an {@code XContentParseException} (which extends
     * {@code IllegalArgumentException}), so the test walks the cause chain rather than asserting on
     * the immediate cause type.
     */
    public void testExpiresAtBeyondInstantRangeFailsListener() throws Exception {
        // Long.MAX_VALUE - 999 is well beyond Instant.MAX.getEpochSecond() (~3.15e16), but well
        // within long range, so the JSON parser accepts it and Instant.ofEpochSecond rejects it.
        respondWith200JsonBody("""
            {
              "token": "header.payload.sig",
              "expires_at": 9223372036854774808
            }
            """.getBytes(StandardCharsets.UTF_8));

        try (ClientHarness harness = new ClientHarness(clientSettingsWithIssuerUrl().build())) {
            final WorkloadIdentityIssuerException cause = awaitFailure(harness, new IssueTokenRequest("aud"));
            assertEquals(200, cause.statusCode());
            assertThat(cause.getMessage(), containsString("failed to parse"));
            assertTrue(
                "DateTimeException must appear somewhere in the cause chain",
                ExceptionsHelper.unwrapCausesAndSuppressed(cause, t -> t instanceof java.time.DateTimeException).isPresent()
            );
        }
    }

    /**
     * The response parser is configured with {@code ignoreUnknownFields=true} so the issuer can
     * introduce additive fields without breaking older clients. This test pins that contract by
     * injecting a payload with an extra top-level field and asserting the known fields still parse
     * cleanly.
     */
    public void testUnknownFieldsInSuccessResponseAreIgnored() throws Exception {
        final long expiresAtSeconds = (System.currentTimeMillis() / 1000) + 3_600;
        respondWith200JsonBody("""
            {
              "token": "header.payload.sig",
              "expires_at": %s,
              "future_field": "added by a newer issuer",
              "nested_future_field": {"k": 1}
            }
            """.formatted(expiresAtSeconds).getBytes(StandardCharsets.UTF_8));

        try (ClientHarness harness = new ClientHarness(clientSettingsWithIssuerUrl().build())) {
            final IssueTokenResponse response = awaitToken(harness, new IssueTokenRequest("aud"));
            assertEquals("header.payload.sig", response.token());
            assertEquals(expiresAtSeconds, response.expiresAt().getEpochSecond());
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
            final IssueTokenRequest request = new IssueTokenRequest("aud");
            final IssueTokenResponse first = awaitToken(harness, request);
            final IssueTokenResponse second = awaitToken(harness, request);
            assertEquals("second issuance must come from the cache", 1, callCount.get());
            assertEquals(first, second);
        }
    }

    /**
     * Concurrent in-flight callers for the same {@link IssueTokenRequest} share a single HTTP
     * fetch via the cache's {@link SubscribableListener}: the first caller wins the
     * {@code putIfAbsent} and dispatches the request; subsequent callers attach to the same
     * unresolved listener and receive the same {@link IssueTokenResponse} instance when it
     * resolves.
     *
     * <p>Complements {@link #testCachedTokenIsReusedAcrossCallsWithSameRequest}, which covers
     * the post-resolution cache-hit path. Here we hold the mock server inside the handler so
     * that all callers enter {@code issueToken} while the fetch is still in flight, then release
     * the server and assert (a) every future resolves to the same response instance and (b) the
     * server saw exactly one request.
     */
    public void testConcurrentCallersForSameRequestShareSingleHttpFetch() throws Exception {
        final AtomicInteger callCount = new AtomicInteger();
        final CountDownLatch serverEntered = new CountDownLatch(1);
        final CountDownLatch releaseServer = new CountDownLatch(1);
        final long farFutureEpochSecond = (System.currentTimeMillis() / 1000) + 3_600;

        handler = exchange -> {
            callCount.incrementAndGet();
            serverEntered.countDown();
            try {
                if (releaseServer.await(10, TimeUnit.SECONDS) == false) {
                    throw new AssertionError("test did not release the server within 10s");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted while waiting for test to release the server", e);
            }
            respondWithToken(exchange, farFutureEpochSecond);
        };

        try (ClientHarness harness = new ClientHarness(clientSettingsWithIssuerUrl().build())) {
            final IssueTokenRequest request = new IssueTokenRequest("aud");

            final int concurrency = randomIntBetween(4, 8);
            final List<PlainActionFuture<IssueTokenResponse>> futures = new ArrayList<>(concurrency);
            try {
                for (int i = 0; i < concurrency; i++) {
                    final PlainActionFuture<IssueTokenResponse> future = new PlainActionFuture<>();
                    harness.client.issueToken(request, future);
                    futures.add(future);
                }
                // Confirm the first request has actually reached the server before releasing it;
                // by this point, callers 2..N have all run putIfAbsent against the entry the
                // winning caller published, so they must be attached to the same listener.
                assertTrue("server handler must have been entered by the in-flight fetch", serverEntered.await(10, TimeUnit.SECONDS));
            } finally {
                // Ensure the server thread is released even if the assertions above fail, so
                // harness close() does not hang waiting for outstanding HTTP requests.
                releaseServer.countDown();
            }

            final IssueTokenResponse first = futures.get(0).get(10, TimeUnit.SECONDS);
            for (int i = 1; i < concurrency; i++) {
                assertSame(
                    "all concurrent callers must observe the same IssueTokenResponse instance",
                    first,
                    futures.get(i).get(10, TimeUnit.SECONDS)
                );
            }
            assertEquals("concurrent callers must collapse onto a single HTTP fetch", 1, callCount.get());
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
            awaitToken(harness, new IssueTokenRequest("aud-a"));
            awaitToken(harness, new IssueTokenRequest("aud-b"));
            awaitToken(harness, new IssueTokenRequest("aud-c"));
            assertEquals("each distinct audience is a distinct cache key", 3, callCount.get());
        }
    }

    /**
     * Defense-in-depth bound on the token cache: once {@code MAX_CACHE_ENTRIES} keys are resident,
     * new (uncached) request keys must bypass the cache and dispatch a one-shot HTTP fetch rather
     * than grow the map without bound. Existing cached keys must continue to serve cache hits, and
     * the saturation WARN must fire at most once per saturation episode.
     *
     * <p>The harness here is built with the package-private constructor that exposes the cap so the
     * test can drive saturation at a tiny cap value (cap=1) without minting the full
     * {@link HttpsWorkloadIdentityIssuerClient#MAX_CACHE_ENTRIES} distinct tokens.
     */
    public void testCacheBypassWhenAtCapacity() throws Exception {
        final AtomicInteger callCount = new AtomicInteger();
        final long farFutureEpochSecond = (System.currentTimeMillis() / 1000) + 3_600;
        final int cap = 1;

        try (ClientHarness harness = new ClientHarness(clientSettingsWithIssuerUrl().build(), cap)) {
            installCountingHandlerOver(callCount, farFutureEpochSecond);
            final IssueTokenRequest cached = new IssueTokenRequest("aud-1");
            awaitToken(harness, cached);
            assertEquals("first issuance fills the single-entry cache", 1, callCount.get());

            // First overflow: bypass cache, dispatch a fresh fetch, and emit a one-shot WARN.
            final IssueTokenRequest overflow = new IssueTokenRequest("aud-2");
            MockLog.assertThatLogger(
                () -> awaitTokenUnchecked(harness, overflow),
                HttpsWorkloadIdentityIssuerClient.class,
                new MockLog.SeenEventExpectation(
                    "cache-at-capacity warn",
                    HttpsWorkloadIdentityIssuerClient.class.getCanonicalName(),
                    Level.WARN,
                    "*workload-identity token cache is at capacity*"
                )
            );
            assertEquals("overflow request must drive a network fetch", 2, callCount.get());

            // Re-issuing the overflowed key must drive another fetch (it was not cached) and the
            // WARN must NOT fire again within the same saturation episode.
            MockLog.assertThatLogger(
                () -> awaitTokenUnchecked(harness, overflow),
                HttpsWorkloadIdentityIssuerClient.class,
                new MockLog.UnseenEventExpectation(
                    "cache-at-capacity warn (suppressed)",
                    HttpsWorkloadIdentityIssuerClient.class.getCanonicalName(),
                    Level.WARN,
                    "*workload-identity token cache is at capacity*"
                )
            );
            assertEquals("overflowed entries must not be cached; second issuance must refetch", 3, callCount.get());

            // The entry cached before saturation must still serve cache hits.
            awaitToken(harness, cached);
            assertEquals("entries cached before saturation must continue to serve hits", 3, callCount.get());
        }
    }

    /**
     * A token is evicted at {@code expiresAt - refresh_before_expiry}, after which the next
     * caller pays the cost of a fresh HTTP fetch.
     *
     * <p>Eviction timing is decoupled from token lifetime: the token is long-lived so
     * {@code validateExpiry}'s {@code expiresAt < now} check cannot race a slow CI worker, and
     * {@code refresh_before_expiry} sits just below the lifetime so eviction still fires ~50ms
     * after the response.
     */
    public void testCachedTokenIsEvictedNearExpiry() throws Exception {
        final AtomicInteger callCount = new AtomicInteger();
        final int tokenTtlSeconds = 60;
        final long expiresAtSeconds = (System.currentTimeMillis() / 1000) + tokenTtlSeconds;

        try (
            ClientHarness harness = new ClientHarness(
                clientSettingsWithIssuerUrl().put(
                    WorkloadIdentityIssuerSettings.TOKEN_CACHE_REFRESH_BEFORE_EXPIRY.getKey(),
                    TimeValue.timeValueMillis(tokenTtlSeconds * 1000L - 50)
                ).build()
            )
        ) {
            // Capture expiresAt once so a slow second handler call can't race validateExpiry either.
            handler = exchange -> respondWithToken(exchange, expiresAtSeconds, callCount);
            final IssueTokenRequest request = new IssueTokenRequest("aud");
            awaitToken(harness, request);
            assertEquals(1, callCount.get());
            assertBusy(() -> {
                awaitToken(harness, request);
                assertEquals("eviction must have fired before this issuance", 2, callCount.get());
            }, 5, TimeUnit.SECONDS);
        }
    }

    /**
     * Failed fetches are not cached: the next caller re-issues and may succeed. Pinned to
     * {@code max_attempts=1} so the retrier collapses to "one attempt then surface the failure";
     * the inter-attempt retry behavior is exercised separately by {@link #testRetriesOn5xxUntilSuccess()}.
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

        try (
            ClientHarness harness = new ClientHarness(
                clientSettingsWithIssuerUrl().put(WorkloadIdentityHttpSettings.RETRY_MAX_ATTEMPTS.getKey(), 1).build()
            )
        ) {
            final IssueTokenRequest request = new IssueTokenRequest("aud");
            final PlainActionFuture<IssueTokenResponse> firstAttempt = new PlainActionFuture<>();
            harness.client.issueToken(request, firstAttempt);
            expectThrows(ExecutionException.class, () -> firstAttempt.get(10, TimeUnit.SECONDS));

            final IssueTokenResponse second = awaitToken(harness, request);
            assertNotNull(second.token());
            assertEquals("the failure must not have been cached", 2, callCount.get());
        }
    }

    /**
     * Transient 5xx responses are retried with jittered exponential backoff; once the issuer
     * recovers (here on the second attempt) the listener resolves with the successful token and
     * the cache entry is established normally.
     */
    public void testRetriesOn5xxUntilSuccess() throws Exception {
        final AtomicInteger callCount = new AtomicInteger();
        final long farFutureEpochSecond = (System.currentTimeMillis() / 1000) + 3_600;
        handler = exchange -> {
            final int n = callCount.incrementAndGet();
            try {
                if (n == 1) {
                    exchange.sendResponseHeaders(503, -1);
                } else {
                    respondWithToken(exchange, farFutureEpochSecond);
                }
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        };

        try (ClientHarness harness = new ClientHarness(retryEnabledSettings(3).build())) {
            final IssueTokenResponse response = awaitToken(harness, new IssueTokenRequest("aud"));
            assertEquals("header.payload.sig", response.token());
            assertEquals("first attempt 503, second 200; both must hit the server", 2, callCount.get());
        }
    }

    /**
     * 429 Too Many Requests is treated as transient, mirroring the 5xx path: the retrier sleeps
     * and re-attempts until success.
     */
    public void testRetriesOn429UntilSuccess() throws Exception {
        final AtomicInteger callCount = new AtomicInteger();
        final long farFutureEpochSecond = (System.currentTimeMillis() / 1000) + 3_600;
        handler = exchange -> {
            final int n = callCount.incrementAndGet();
            try {
                if (n == 1) {
                    exchange.sendResponseHeaders(429, -1);
                } else {
                    respondWithToken(exchange, farFutureEpochSecond);
                }
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        };

        try (ClientHarness harness = new ClientHarness(retryEnabledSettings(3).build())) {
            final IssueTokenResponse response = awaitToken(harness, new IssueTokenRequest("aud"));
            assertEquals("header.payload.sig", response.token());
            assertEquals(2, callCount.get());
        }
    }

    /**
     * Persistent transient failures eventually exhaust the per-call attempt cap. The most recent
     * failure surfaces to the listener; earlier attempts attach as suppressed exceptions, as
     * arranged by {@link org.elasticsearch.action.support.RetryableAction}'s base class. The
     * server must be called exactly {@code max_attempts} times.
     */
    public void testRetryExhaustionOnPersistent5xxFailsListener() throws Exception {
        final AtomicInteger callCount = new AtomicInteger();
        handler = exchange -> {
            callCount.incrementAndGet();
            try {
                exchange.sendResponseHeaders(500, -1);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        };

        final int maxAttempts = 3;
        try (ClientHarness harness = new ClientHarness(retryEnabledSettings(maxAttempts).build())) {
            final WorkloadIdentityIssuerException cause = awaitFailure(harness, new IssueTokenRequest("aud"));
            assertEquals(500, cause.statusCode());
            assertEquals("retrier must exhaust exactly max_attempts attempts", maxAttempts, callCount.get());
            // The two earlier attempts must be visible on the cause chain as suppressed exceptions.
            assertEquals("earlier attempts must attach as suppressed exceptions", maxAttempts - 1, cause.getSuppressed().length);
        }
    }

    /**
     * 4xx responses other than 429 are configuration / credential errors that no amount of
     * retrying can fix; the retrier must fail fast on the first attempt.
     */
    public void testDoesNotRetryOn4xxOtherThan429() throws Exception {
        final AtomicInteger callCount = new AtomicInteger();
        handler = exchange -> {
            callCount.incrementAndGet();
            try {
                exchange.sendResponseHeaders(403, -1);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        };

        try (ClientHarness harness = new ClientHarness(retryEnabledSettings(5).build())) {
            final WorkloadIdentityIssuerException cause = awaitFailure(harness, new IssueTokenRequest("aud"));
            assertEquals(403, cause.statusCode());
            assertEquals("4xx (non-429) must not be retried", 1, callCount.get());
            assertEquals(0, cause.getSuppressed().length);
        }
    }

    /**
     * Parse / validation failures (carrying the {@code -1} sentinel status when raised inside
     * {@code parseSuccess}, or a 2xx status when raised by {@code validateExpiry}) are
     * deterministic and must not be retried.
     */
    public void testDoesNotRetryOnParseFailure() throws Exception {
        final AtomicInteger callCount = new AtomicInteger();
        handler = exchange -> {
            callCount.incrementAndGet();
            try {
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                final byte[] body = "not json {{".getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, body.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(body);
                }
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        };

        try (ClientHarness harness = new ClientHarness(retryEnabledSettings(5).build())) {
            final WorkloadIdentityIssuerException cause = awaitFailure(harness, new IssueTokenRequest("aud"));
            assertThat(cause.getMessage(), containsString("failed to parse"));
            assertEquals("parse failures must not be retried", 1, callCount.get());
        }
    }

    /**
     * Pure unit test of the retry-policy predicate. Encodes the policy without touching the
     * network so the matrix of "retryable vs not" is easy to read at a glance.
     */
    public void testIsRetryablePolicy() {
        // Retryable: transport / IO failures and the transient issuer statuses (429, 5xx).
        assertTrue(HttpsWorkloadIdentityIssuerClient.isRetryable(new IOException("boom")));
        assertTrue(HttpsWorkloadIdentityIssuerClient.isRetryable(new java.net.SocketTimeoutException("timeout")));
        assertTrue(HttpsWorkloadIdentityIssuerClient.isRetryable(new WorkloadIdentityIssuerException("429", 429)));
        assertTrue(HttpsWorkloadIdentityIssuerClient.isRetryable(new WorkloadIdentityIssuerException("500", 500)));
        assertTrue(HttpsWorkloadIdentityIssuerClient.isRetryable(new WorkloadIdentityIssuerException("503", 503)));
        assertTrue(HttpsWorkloadIdentityIssuerClient.isRetryable(new WorkloadIdentityIssuerException("599", 599)));

        // Not retryable: deterministic client-side errors, parse failures (sentinel status -1),
        // explicit cancellation, and 4xx other than 429.
        assertFalse(HttpsWorkloadIdentityIssuerClient.isRetryable(new WorkloadIdentityIssuerException("parse", -1)));
        assertFalse(HttpsWorkloadIdentityIssuerClient.isRetryable(new WorkloadIdentityIssuerException("200 body bad", 200)));
        assertFalse(HttpsWorkloadIdentityIssuerClient.isRetryable(new WorkloadIdentityIssuerException("400", 400)));
        assertFalse(HttpsWorkloadIdentityIssuerClient.isRetryable(new WorkloadIdentityIssuerException("403", 403)));
        assertFalse(HttpsWorkloadIdentityIssuerClient.isRetryable(new WorkloadIdentityIssuerException("404", 404)));
        assertFalse(HttpsWorkloadIdentityIssuerClient.isRetryable(new java.util.concurrent.CancellationException("cancelled")));
        assertFalse(HttpsWorkloadIdentityIssuerClient.isRetryable(new IllegalStateException("not an IOException")));
    }

    /**
     * The cross-setting check {@code RETRY_TIMEOUT >= REQUEST_TIMEOUT} must fail fast at node
     * start. Without it, a single attempt would always exceed the retry budget and the retry
     * layer would degenerate to "one attempt that also has to lose a race against the wall clock".
     */
    public void testRetryTimeoutBelowRequestTimeoutFailsAtConstruction() throws Exception {
        final Settings settings = clientSettingsWithIssuerUrl().put(
            WorkloadIdentityHttpSettings.REQUEST_TIMEOUT.getKey(),
            TimeValue.timeValueSeconds(10)
        ).put(WorkloadIdentityHttpSettings.RETRY_TIMEOUT.getKey(), TimeValue.timeValueSeconds(5)).build();
        final IllegalArgumentException ex = assertClientConstructorThrows(settings);
        assertThat(ex.getMessage(), containsString(WorkloadIdentityHttpSettings.RETRY_TIMEOUT.getKey()));
        assertThat(ex.getMessage(), containsString(WorkloadIdentityHttpSettings.REQUEST_TIMEOUT.getKey()));
        assertThat(ex.getMessage(), containsString("must be greater than or equal to setting"));
    }

    /**
     * The cross-setting check {@code RETRY_MAX_DELAY_BOUND >= RETRY_INITIAL_DELAY} must fail at
     * node start. {@link org.elasticsearch.action.support.RetryableAction} enforces the same
     * invariant, but only when {@code run()} is first invoked; mirroring the check at construction
     * surfaces operator typos before any token request is dispatched.
     */
    public void testRetryMaxDelayBoundBelowInitialDelayFailsAtConstruction() throws Exception {
        final Settings settings = clientSettingsWithIssuerUrl().put(
            WorkloadIdentityHttpSettings.RETRY_INITIAL_DELAY.getKey(),
            TimeValue.timeValueMillis(500)
        ).put(WorkloadIdentityHttpSettings.RETRY_MAX_DELAY_BOUND.getKey(), TimeValue.timeValueMillis(100)).build();
        final IllegalArgumentException ex = assertClientConstructorThrows(settings);
        assertThat(ex.getMessage(), containsString(WorkloadIdentityHttpSettings.RETRY_MAX_DELAY_BOUND.getKey()));
        assertThat(ex.getMessage(), containsString(WorkloadIdentityHttpSettings.RETRY_INITIAL_DELAY.getKey()));
        assertThat(ex.getMessage(), containsString("must be greater than or equal to setting"));
    }

    /**
     * Stands up the surrounding wiring exactly as {@link ClientHarness} does, then expects the
     * {@link HttpsWorkloadIdentityIssuerClient} constructor itself to throw an
     * {@link IllegalArgumentException}. Cannot reuse {@link ClientHarness} directly because its
     * try-with-resources contract assumes a successful client construction; here we need to
     * tear the manager and thread pool down even when the inner constructor throws.
     */
    private static IllegalArgumentException assertClientConstructorThrows(Settings settings) {
        final ThreadPool threadPool = new TestThreadPool(HttpsWorkloadIdentityIssuerClientTests.class.getSimpleName());
        try {
            final Environment environment = TestEnvironment.newEnvironment(settings);
            // ENABLED=false: this code path never reaches a successful client construction, let
            // alone a request, so file-watch polling would never be exercised even if enabled.
            final ResourceWatcherService resourceWatcher = new ResourceWatcherService(
                Settings.builder().put(ResourceWatcherService.ENABLED.getKey(), false).build(),
                threadPool
            );
            try {
                final WorkloadIdentitySslConfig sslConfig = new WorkloadIdentitySslConfig(settings, environment, resourceWatcher);
                final WorkloadIdentityHttpClientManager manager = new WorkloadIdentityHttpClientManager(settings, sslConfig, threadPool);
                try {
                    return expectThrows(
                        IllegalArgumentException.class,
                        () -> new HttpsWorkloadIdentityIssuerClient(settings, manager, threadPool)
                    );
                } finally {
                    manager.close();
                }
            } finally {
                resourceWatcher.close();
            }
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
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

            final IssueTokenRequest request = new IssueTokenRequest("aud");

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
     * End-to-end test of the SSL reload drain. Stages a writable copy of the CA material under
     * {@code createTempDir()} so the test does not mutate the shared classpath resource, stands
     * up the full production wiring (a {@link org.elasticsearch.watcher.ResourceWatcherService}
     * with auto-polling disabled, a {@link WorkloadIdentitySslConfig} watching the staged CA
     * file, and the manager subscribed to the SSL config's reload event), then walks the entire
     * three-request rotation drain — pre-rotation issuance, rotation tick, post-rotation reuse
     * of the stale-stamped connection, and a subsequent fresh handshake — asserting at each
     * step the precise log line the corresponding code path produces.
     */
    public void testTokenIssuanceSurvivesSslReload() throws Exception {
        final AtomicInteger callCount = new AtomicInteger();
        final long farFutureEpochSecond = (System.currentTimeMillis() / 1000) + 3_600;
        installCountingHandlerOver(callCount, farFutureEpochSecond);

        // Stage a writable copy of the CA so the test mutates a temp file rather than the shared
        // classpath resource. Other SSL material is still loaded from the classpath; only the CA
        // is on the FileWatcher's watchlist (it's the file we mutate below).
        final Path workDir = createTempDir();
        final Path watchedCa = workDir.resolve("ca.crt");
        Files.copy(getDataPath("ca.crt"), watchedCa, StandardCopyOption.REPLACE_EXISTING);

        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(WorkloadIdentitySslConfig.SETTING_PREFIX + "secure_key_passphrase", "client-password");
        final Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .put(WorkloadIdentityIssuerSettings.ISSUER_URL_SETTING.getKey(), issuerBaseUrl())
            .putList(WorkloadIdentitySslConfig.SETTING_PREFIX + "certificate_authorities", watchedCa.toString())
            .put(WorkloadIdentitySslConfig.SETTING_PREFIX + "certificate", getDataPath("node/node.crt").toString())
            .put(WorkloadIdentitySslConfig.SETTING_PREFIX + "key", getDataPath("node/node.key").toString())
            .setSecureSettings(secureSettings)
            .build();
        final Environment environment = TestEnvironment.newEnvironment(settings);
        final ThreadPool threadPool = new TestThreadPool(getTestName());
        // ENABLED=false so the polling thread does not race with notifyNow; the test drives
        // reload deterministically.
        final ResourceWatcherService resourceWatcher = new ResourceWatcherService(
            Settings.builder().put(ResourceWatcherService.ENABLED.getKey(), false).build(),
            threadPool
        );

        // The handshake-stamp, stale-connection-retire, manager-publish, and ssl-config reload
        // logs all live at DEBUG (low-volume in production, but useful when triaging a rotation).
        // Bump those loggers to DEBUG for the duration of this test and restore them in the
        // finally below.
        final Logger stampLogger = LogManager.getLogger(ReloadableSchemeIoSessionStrategy.class);
        final Logger reuseLogger = LogManager.getLogger(RotationAwareReuseStrategy.class);
        final Logger managerLogger = LogManager.getLogger(WorkloadIdentityHttpClientManager.class);
        final Logger sslConfigLogger = LogManager.getLogger(WorkloadIdentitySslConfig.class);
        final Level previousStampLevel = stampLogger.getLevel();
        final Level previousReuseLevel = reuseLogger.getLevel();
        final Level previousManagerLevel = managerLogger.getLevel();
        final Level previousSslConfigLevel = sslConfigLogger.getLevel();
        Loggers.setLevel(stampLogger, Level.DEBUG);
        Loggers.setLevel(reuseLogger, Level.DEBUG);
        Loggers.setLevel(managerLogger, Level.DEBUG);
        Loggers.setLevel(sslConfigLogger, Level.DEBUG);

        try {
            final WorkloadIdentitySslConfig sslConfig = new WorkloadIdentitySslConfig(settings, environment, resourceWatcher);
            try (WorkloadIdentityHttpClientManager manager = new WorkloadIdentityHttpClientManager(settings, sslConfig, threadPool)) {
                // Mirror the plugin's wiring: the initial setDelegate happens via sslConfig.start()
                // firing the manager listener, which advances the epoch from 0 to 1 — hence the
                // first TLS handshake below is stamped at epoch 1.
                sslConfig.addReloadListener(manager::reload);
                sslConfig.start();
                manager.start();
                final HttpsWorkloadIdentityIssuerClient client = new HttpsWorkloadIdentityIssuerClient(settings, manager, threadPool);

                try (
                    MockLog mockLog = MockLog.capture(
                        WorkloadIdentitySslConfig.class,
                        WorkloadIdentityHttpClientManager.class,
                        ReloadableSchemeIoSessionStrategy.class,
                        RotationAwareReuseStrategy.class
                    )
                ) {
                    // The whole drain emits five distinct log events across four loggers. Register
                    // them all up-front: each expectation owns its own latch and counts down the
                    // first time the matching event fires, regardless of order against the other
                    // expectations.
                    mockLog.addExpectation(
                        new MockLog.SeenEventExpectation(
                            "first TLS handshake stamped at epoch 1",
                            ReloadableSchemeIoSessionStrategy.class.getCanonicalName(),
                            Level.DEBUG,
                            "*stamped new workload-identity TLS connection*epoch [1]*"
                        )
                    );
                    mockLog.addExpectation(
                        new MockLog.SeenEventExpectation(
                            "ssl config reloaded",
                            WorkloadIdentitySslConfig.class.getCanonicalName(),
                            Level.DEBUG,
                            "loaded workload-identity SSL context"
                        )
                    );
                    mockLog.addExpectation(
                        new MockLog.SeenEventExpectation(
                            "manager published SSL strategy",
                            WorkloadIdentityHttpClientManager.class.getCanonicalName(),
                            Level.DEBUG,
                            "*published workload-identity SSL strategy*"
                        )
                    );
                    mockLog.addExpectation(
                        new MockLog.SeenEventExpectation(
                            "stale-epoch connection retired",
                            RotationAwareReuseStrategy.class.getCanonicalName(),
                            Level.DEBUG,
                            "*retiring workload-identity HTTP connection: stamped epoch [1] differs from current [2]*"
                        )
                    );
                    mockLog.addExpectation(
                        new MockLog.SeenEventExpectation(
                            "post-rotation handshake stamped at epoch 2",
                            ReloadableSchemeIoSessionStrategy.class.getCanonicalName(),
                            Level.DEBUG,
                            "*stamped new workload-identity TLS connection*epoch [2]*"
                        )
                    );

                    // --- Phase 1: first issuance warms up the pool. The TLS handshake fires the
                    // "stamped...epoch [1]" log (epoch is 1 not 0: the initial setDelegate during
                    // sslConfig.start() advanced it from the construction-time 0). After the
                    // response, the reuse strategy sees stamped==current==1 and the connection
                    // returns to the pool.
                    final PlainActionFuture<IssueTokenResponse> firstFuture = new PlainActionFuture<>();
                    client.issueToken(new IssueTokenRequest("aud-1"), firstFuture);
                    assertEquals("header.payload.sig", firstFuture.get(10, TimeUnit.SECONDS).token());
                    assertEquals(
                        "rotation epoch after the initial sslConfig.start() publish must be one",
                        1,
                        manager.getSslStrategy().currentEpoch()
                    );

                    final CloseableHttpAsyncClient httpClientBefore = manager.getHttpClient();
                    final SSLIOSessionStrategy delegateBefore = manager.getSslStrategy().getDelegate();

                    // --- Phase 2: rotate. Appending a trailing newline to the watched CA fires
                    // the FileWatcher → sslConfig.loadAndPublish() → manager.reload() chain,
                    // which logs "reloaded..." then "rotated..." and advances the wrapper epoch
                    // from 1 to 2. The HC client instance itself is untouched, mirroring the
                    // in-place rotation contract: in-flight requests on the previous strategy
                    // continue undisturbed.
                    Files.writeString(watchedCa, Files.readString(watchedCa) + "\n", StandardCharsets.US_ASCII);
                    resourceWatcher.notifyNow(ResourceWatcherService.Frequency.HIGH);

                    assertSame("the HC client instance must NOT change across SSL reload", httpClientBefore, manager.getHttpClient());
                    assertNotSame(
                        "the scheme strategy delegate must be swapped on SSL reload",
                        delegateBefore,
                        manager.getSslStrategy().getDelegate()
                    );
                    assertEquals("rotation must advance the rotation epoch by one", 2, manager.getSslStrategy().currentEpoch());

                    // --- Phase 3: post-rotation issuance. HC reuses the still-idle connection
                    // from Phase 1 (stamped at epoch 1) because no handshake is needed. The
                    // request completes successfully against the unchanged HC client, then the
                    // reuse strategy compares stamped=1 against current=2, logs the retire line,
                    // and closes the connection rather than returning it to the pool. Distinct
                    // audience so the request bypasses the issuer-client token cache and crosses
                    // the network.
                    final PlainActionFuture<IssueTokenResponse> secondFuture = new PlainActionFuture<>();
                    client.issueToken(new IssueTokenRequest("aud-2"), secondFuture);
                    assertEquals("header.payload.sig", secondFuture.get(10, TimeUnit.SECONDS).token());

                    // --- Phase 4: subsequent issuance. The pool is empty (the retire above
                    // closed the only connection), so HC opens a fresh one. The handshake hits
                    // the post-rotation delegate and stamps the new connection at epoch 2. This
                    // confirms the rotation actually reached the connection-establishment path,
                    // not just the wrapper's internal state.
                    final PlainActionFuture<IssueTokenResponse> thirdFuture = new PlainActionFuture<>();
                    client.issueToken(new IssueTokenRequest("aud-3"), thirdFuture);
                    assertEquals("header.payload.sig", thirdFuture.get(10, TimeUnit.SECONDS).token());
                    assertEquals("each distinct audience must cross the network", 3, callCount.get());

                    // The retire log and the post-rotation stamp log are emitted from the IO
                    // reactor thread, decoupled from the client-facing future completion. await
                    // (with the framework's standard timeout) rather than assertMatched so the
                    // test does not race their dispatch.
                    mockLog.awaitAllExpectationsMatched();
                }
            }
        } finally {
            Loggers.setLevel(stampLogger, previousStampLevel);
            Loggers.setLevel(reuseLogger, previousReuseLevel);
            Loggers.setLevel(managerLogger, previousManagerLevel);
            Loggers.setLevel(sslConfigLogger, previousSslConfigLevel);
            try {
                resourceWatcher.close();
            } finally {
                ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
            }
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
            final IssueTokenRequest request = new IssueTokenRequest("aud");

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
            final String body = "{\"token\":\"header.payload.sig\",\"expires_at\":" + expiresAtEpochSeconds + "}";
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

    /**
     * {@link #clientSettingsWithIssuerUrl()} with the retry knobs squeezed for fast tests: tiny
     * inter-attempt delays so the retry-flow tests finish in milliseconds, but the wall-clock
     * budget still comfortably exceeds the per-request timeout (production constructor invariant).
     */
    private Settings.Builder retryEnabledSettings(int maxAttempts) throws Exception {
        return clientSettingsWithIssuerUrl().put(WorkloadIdentityHttpSettings.RETRY_INITIAL_DELAY.getKey(), TimeValue.timeValueMillis(10))
            .put(WorkloadIdentityHttpSettings.RETRY_MAX_DELAY_BOUND.getKey(), TimeValue.timeValueMillis(50))
            .put(WorkloadIdentityHttpSettings.RETRY_MAX_ATTEMPTS.getKey(), maxAttempts)
            .put(WorkloadIdentityHttpSettings.RETRY_TIMEOUT.getKey(), TimeValue.timeValueSeconds(15));
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
     * Variant of {@link #awaitToken} that rethrows any checked exception as an
     * {@link AssertionError}, so the call site can be used inside a {@link Runnable} (e.g. the
     * lambda passed to {@link MockLog#assertThatLogger}) without declaring {@code throws}.
     */
    private static void awaitTokenUnchecked(ClientHarness harness, IssueTokenRequest request) {
        try {
            awaitToken(harness, request);
        } catch (Exception e) {
            throw new AssertionError("issueToken failed unexpectedly", e);
        }
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
     * Encapsulates the full client wiring for a single test: SSL config (backed by a
     * {@link ResourceWatcherService} with auto-polling disabled, since file-watch reload is not
     * exercised by these tests), HTTP client manager, and the
     * {@link HttpsWorkloadIdentityIssuerClient} under test. Closing the harness releases the
     * HTTP client and stops the background eviction task.
     */
    private static final class ClientHarness implements AutoCloseable {
        final ThreadPool threadPool;
        final ResourceWatcherService resourceWatcher;
        final WorkloadIdentitySslConfig sslConfig;
        final WorkloadIdentityHttpClientManager manager;
        final HttpsWorkloadIdentityIssuerClient client;

        ClientHarness(Settings settings) {
            this(settings, new TestThreadPool(HttpsWorkloadIdentityIssuerClientTests.class.getSimpleName()));
        }

        ClientHarness(Settings settings, ThreadPool threadPool) {
            this(settings, threadPool, HttpsWorkloadIdentityIssuerClient.MAX_CACHE_ENTRIES);
        }

        ClientHarness(Settings settings, int maxCacheEntries) {
            this(settings, new TestThreadPool(HttpsWorkloadIdentityIssuerClientTests.class.getSimpleName()), maxCacheEntries);
        }

        ClientHarness(Settings settings, ThreadPool threadPool, int maxCacheEntries) {
            this.threadPool = threadPool;
            final Environment environment = TestEnvironment.newEnvironment(settings);
            // ENABLED=false: file-watch polling is not exercised here. The watcher is still a
            // required collaborator of WorkloadIdentitySslConfig; the disabled instance is cheap
            // (no scheduler thread, no I/O).
            this.resourceWatcher = new ResourceWatcherService(
                Settings.builder().put(ResourceWatcherService.ENABLED.getKey(), false).build(),
                threadPool
            );
            this.sslConfig = new WorkloadIdentitySslConfig(settings, environment, resourceWatcher);
            this.manager = new WorkloadIdentityHttpClientManager(settings, sslConfig, threadPool);
            // Mirror WorkloadIdentityPlugin's wiring: listener before sslConfig.start() so the
            // initial publish populates the manager's SSL strategy; manager.start() comes last.
            this.sslConfig.addReloadListener(manager::reload);
            this.sslConfig.start();
            this.manager.start();
            this.client = new HttpsWorkloadIdentityIssuerClient(settings, manager, threadPool, maxCacheEntries);
        }

        @Override
        public void close() {
            try {
                manager.close();
            } finally {
                try {
                    resourceWatcher.close();
                } finally {
                    ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
                }
            }
        }
    }

    /**
     * Test {@link ThreadPool} whose 3-argument {@link ThreadPool#schedule(Runnable, TimeValue, Executor)
     * schedule} overload conditionally rejects with an {@link EsRejectedExecutionException}. Only that
     * overload is intercepted; {@code scheduleWithFixedDelay(...)} (used by {@link HttpConnectionEvictor})
     * is left alone so the connection-evictor startup in {@link ClientHarness} is unaffected. Used to
     * exercise the catch branch in {@code HttpsWorkloadIdentityIssuerClient#scheduleEviction}.
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
            // Apply via setSSLParameters rather than setNeedClientAuth: with the BouncyCastle JSSE
            // provider used in FIPS, the legacy HttpsParameters.setNeedClientAuth path requests the
            // client cert during the handshake but the resulting peer chain is not exposed on the
            // server-side SSLSession; routing the same flag through SSLParameters fixes that without
            // weakening the need-vs-want contract that testHandshakeFailsWithoutClientCert relies on.
            final SSLParameters sslParameters = getSSLContext().getDefaultSSLParameters();
            sslParameters.setNeedClientAuth(true);
            params.setSSLParameters(sslParameters);
        }
    }
}
