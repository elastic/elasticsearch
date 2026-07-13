/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.nio.conn.NoopIOSessionStrategy;
import org.apache.http.nio.conn.SchemeIOSessionStrategy;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ssl.TestsSSLService;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.elasticsearch.xpack.inference.Utils.mockClusterService;
import static org.elasticsearch.xpack.inference.external.http.HttpClientTests.createConnectionManager;
import static org.elasticsearch.xpack.inference.external.http.HttpClientTests.createHttpPost;
import static org.elasticsearch.xpack.inference.logging.ThrottlerManagerTests.mockThrottlerManager;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * Verifies that the shared inference {@link HttpClient} routes outbound requests through the proxy configured via the
 * {@code xpack.inference.proxy.*} settings, and connects directly to the target host when no proxy is configured. Requests are sent
 * through a recording forward proxy ({@link MockHttpProxyServer}) to a {@link MockWebServer} upstream, so both hops of the proxied
 * exchange can be asserted at the socket level rather than by inspecting client configuration.
 */
public class HttpClientProxyTests extends ESTestCase {
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);

    private final MockWebServer upstreamServer = new MockWebServer();
    private MockHttpProxyServer proxyServer;
    private ThreadPool threadPool;

    @Before
    public void init() throws Exception {
        upstreamServer.start();
        proxyServer = new MockHttpProxyServer();
        threadPool = createThreadPool(inferenceUtilityExecutors());
    }

    @After
    public void shutdown() throws IOException {
        terminate(threadPool);
        proxyServer.close();
        upstreamServer.close();
    }

    public void testSend_RoutesRequestThroughProxy_WhenProxyIsConfigured() throws Exception {
        int responseCode = randomIntBetween(200, 203);
        String body = randomAlphaOfLengthBetween(2, 8096);
        upstreamServer.enqueue(new MockResponse().setResponseCode(responseCode).setBody(body));

        var httpPost = createHttpPost(upstreamServer.getPort(), randomAlphaOfLength(3), randomAlphaOfLength(3));

        // Enable proxy via settings
        var settings = Settings.builder()
            .put(HttpSettings.ProxySettings.INFERENCE_PROXY_HOST_KEY, proxyServer.getHost())
            .put(HttpSettings.ProxySettings.INFERENCE_PROXY_PORT_KEY, proxyServer.getPort())
            .put(HttpSettings.ProxySettings.INFERENCE_PROXY_SCHEME_KEY, "http")
            .build();

        try (var httpClient = createHttpClient(settings)) {
            httpClient.start();

            PlainActionFuture<HttpResult> listener = new PlainActionFuture<>();
            httpClient.send(httpPost, HttpClientContext.create(), listener);
            var result = listener.actionGet(TIMEOUT);

            assertThat(result.response().getStatusLine().getStatusCode(), equalTo(responseCode));
            assertThat(new String(result.body(), StandardCharsets.UTF_8), is(body));

            // the proxy handled the request; a forward-proxied request line carries the absolute target URI
            assertThat(proxyServer.proxiedRequestLines(), hasSize(1));
            assertThat(proxyServer.proxiedRequestLines().getFirst(), containsString("http://localhost:" + upstreamServer.getPort()));

            // the upstream was reached through the proxy, not directly
            assertThat(upstreamServer.requests(), hasSize(1));
            assertThat(
                upstreamServer.requests().getFirst().getHeader(MockHttpProxyServer.VIA_HEADER),
                equalTo(MockHttpProxyServer.VIA_HEADER_VALUE)
            );
        }
    }

    public void testSend_SendsRequestDirectlyToUpstream_WhenNoProxyIsConfigured() throws Exception {
        int responseCode = randomIntBetween(200, 203);
        String body = randomAlphaOfLengthBetween(2, 8096);
        upstreamServer.enqueue(new MockResponse().setResponseCode(responseCode).setBody(body));

        var httpPost = createHttpPost(upstreamServer.getPort(), randomAlphaOfLength(3), randomAlphaOfLength(3));

        // Empty settings passed to the client -> http/proxy settings also empty
        try (var httpClient = createHttpClient(Settings.EMPTY)) {
            httpClient.start();

            PlainActionFuture<HttpResult> listener = new PlainActionFuture<>();
            httpClient.send(httpPost, HttpClientContext.create(), listener);
            var result = listener.actionGet(TIMEOUT);

            assertThat(result.response().getStatusLine().getStatusCode(), equalTo(responseCode));
            assertThat(new String(result.body(), StandardCharsets.UTF_8), is(body));

            // the proxy saw nothing and the upstream was reached directly (no proxy hop marker)
            assertThat(proxyServer.proxiedRequestLines(), empty());

            assertThat(upstreamServer.requests(), hasSize(1));
            assertThat(upstreamServer.requests().getFirst().getHeader(MockHttpProxyServer.VIA_HEADER), nullValue());
        }
    }

    /**
     * Documents a known limitation of the {@code https} proxy scheme rather than desired behavior: httpasyncclient 4.x cannot layer
     * the target's TLS session over a TLS connection to the proxy. The {@code CONNECT} handshake itself succeeds (the proxy is reached
     * over TLS and grants the tunnel), but upgrading the granted tunnel to the target's TLS session fails because
     * {@code SSLIOSessionStrategy#upgrade} rejects a session that is already TLS. Since every inference provider is an https target,
     * an {@code https} proxy scheme currently breaks all requests through this client. If this test starts failing after an Apache
     * HTTP client upgrade (e.g. to 5.x), the limitation is gone and the https scheme can be fully supported on this path.
     */
    public void testSend_CannotLayerTargetTlsOverTlsProxyConnection_WhenProxySchemeIsHttps() throws Exception {
        var sslContext = buildSelfTrustedSslContext();

        try (var tlsProxyServer = new MockHttpProxyServer(sslContext).grantingConnectTunnels()) {
            var settings = Settings.builder()
                .put(HttpSettings.ProxySettings.INFERENCE_PROXY_HOST_KEY, tlsProxyServer.getHost())
                .put(HttpSettings.ProxySettings.INFERENCE_PROXY_PORT_KEY, tlsProxyServer.getPort())
                .put(HttpSettings.ProxySettings.INFERENCE_PROXY_SCHEME_KEY, "https")
                .build();

            // an https target is required to trigger the tunnel upgrade; it is never actually reached
            var httpPost = new HttpPost("https://localhost:" + upstreamServer.getPort() + "/" + randomAlphaOfLength(3));
            var httpRequest = new HttpRequest(httpPost, "inference-id");

            var httpSettings = new HttpSettings(settings, mockClusterService(settings));
            var connectionManager = createTlsCapableConnectionManager(sslContext);
            try (var httpClient = HttpClient.create(httpSettings, threadPool, connectionManager, mockThrottlerManager())) {
                httpClient.start();

                PlainActionFuture<HttpResult> listener = new PlainActionFuture<>();
                httpClient.send(httpRequest, HttpClientContext.create(), listener);
                var exception = expectThrows(Exception.class, () -> listener.actionGet(TIMEOUT));

                assertThat(causeChainMessages(exception), containsString("already upgraded to TLS/SSL"));

                // the handshake did reach the proxy over TLS, so the failure above is the TLS-over-TLS layering, not connectivity
                assertThat(tlsProxyServer.proxiedRequestLines(), hasSize(1));
                assertThat(
                    tlsProxyServer.proxiedRequestLines().getFirst(),
                    containsString("CONNECT localhost:" + upstreamServer.getPort())
                );
                assertThat(upstreamServer.requests(), empty());
            }
        }
    }

    private HttpClient createHttpClient(Settings settings) throws Exception {
        var httpSettings = new HttpSettings(settings, mockClusterService(settings));
        return HttpClient.create(httpSettings, threadPool, createConnectionManager(), mockThrottlerManager());
    }

    /**
     * Mirrors the connection manager built by {@code HttpClientManager} in production: an {@code https} scheme strategy backed by an
     * {@link SSLContext} — here one that trusts the mock proxy's self-signed certificate so the proxy-hop TLS handshake succeeds.
     */
    private static PoolingNHttpClientConnectionManager createTlsCapableConnectionManager(SSLContext sslContext) throws Exception {
        var sessionStrategyRegistry = RegistryBuilder.<SchemeIOSessionStrategy>create()
            .register("http", NoopIOSessionStrategy.INSTANCE)
            .register("https", new SSLIOSessionStrategy(sslContext, NoopHostnameVerifier.INSTANCE))
            .build();
        return new PoolingNHttpClientConnectionManager(new DefaultConnectingIOReactor(), sessionStrategyRegistry);
    }

    /**
     * A self-signed certificate that is both the proxy's identity and the client's only trust anchor, built from the shared
     * {@code testnode} PEM fixtures on the x-pack core test classpath.
     */
    private SSLContext buildSelfTrustedSslContext() throws Exception {
        // the fixtures live inside the x-pack core test-artifacts jar, so they must be materialized as files for the PEM settings
        var tempDir = createTempDir();
        var certPath = copyResourceToFile("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt", tempDir);
        var keyPath = copyResourceToFile("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem", tempDir);
        var secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.http.ssl.secure_key_passphrase", "testnode");
        var environmentSettings = Settings.builder()
            .put("path.home", createTempDir())
            .put("xpack.http.ssl.key", keyPath)
            .put("xpack.http.ssl.certificate", certPath)
            .put("xpack.http.ssl.certificate_authorities", certPath)
            .setSecureSettings(secureSettings)
            .build();

        var sslService = new TestsSSLService(TestEnvironment.newEnvironment(environmentSettings));
        return sslService.sslContext("xpack.http.ssl");
    }

    private static Path copyResourceToFile(String resource, Path directory) throws IOException {
        try (var resourceStream = HttpClientProxyTests.class.getResourceAsStream(resource)) {
            assertNotNull("classpath resource not found: " + resource, resourceStream);
            var target = directory.resolve(resource.substring(resource.lastIndexOf('/') + 1));
            Files.copy(resourceStream, target);
            return target;
        }
    }

    private static String causeChainMessages(Throwable exception) {
        var messages = new StringBuilder();
        for (var current = exception; current != null && current.getCause() != current; current = current.getCause()) {
            messages.append(current.getMessage()).append(" | ");
        }
        return messages.toString();
    }
}
