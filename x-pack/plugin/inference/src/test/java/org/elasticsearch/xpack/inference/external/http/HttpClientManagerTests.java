/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.breaker.TestCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.SslConfiguration;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.ssl.SslSettingsLoader;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLPeerUnverifiedException;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.elasticsearch.xpack.inference.Utils.mockClusterService;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.HttpClientTests.createHttpPost;
import static org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettings.ELASTIC_INFERENCE_SERVICE_SSL_CONFIGURATION_PREFIX;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class HttpClientManagerTests extends ESTestCase {
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);

    private final MockWebServer webServer = new MockWebServer();
    private ThreadPool threadPool;
    private Path sslResourceDir;

    @Before
    public void init() throws Exception {
        webServer.start();
        threadPool = createThreadPool(inferenceUtilityExecutors());
        sslResourceDir = createTempDir();
    }

    @After
    public void shutdown() {
        terminate(threadPool);
        webServer.close();
    }

    public void testSend_MockServerReceivesRequest() throws Exception {
        int responseCode = randomIntBetween(200, 203);
        String body = randomAlphaOfLengthBetween(2, 8096);
        webServer.enqueue(new MockResponse().setResponseCode(responseCode).setBody(body));

        String paramKey = randomAlphaOfLength(3);
        String paramValue = randomAlphaOfLength(3);
        var httpPost = createHttpPost(webServer.getPort(), paramKey, paramValue);

        var manager = HttpClientManager.create(
            Settings.EMPTY,
            threadPool,
            mockClusterServiceEmpty(),
            mock(ThrottlerManager.class),
            new TestCircuitBreaker()
        );
        try (var httpClient = manager.getHttpClient()) {
            httpClient.start();

            PlainActionFuture<HttpResult> listener = new PlainActionFuture<>();
            httpClient.send(httpPost, HttpClientContext.create(), listener);

            var result = listener.actionGet(TIMEOUT);

            assertThat(result.response().getStatusLine().getStatusCode(), equalTo(responseCode));
            assertThat(new String(result.body(), StandardCharsets.UTF_8), is(body));
            assertThat(webServer.requests(), hasSize(1));
            assertThat(webServer.requests().get(0).getUri().getPath(), equalTo(httpPost.httpRequestBase().getURI().getPath()));
            assertThat(webServer.requests().get(0).getUri().getQuery(), equalTo(paramKey + "=" + paramValue));
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
        }
    }

    public void testCreateWithSslServiceSendsRequestOverHttps() throws Exception {
        try (var tlsServer = startTlsServer("server", false)) {
            var settings = eisSslSettings();
            if (randomBoolean()) {
                settings.put(ELASTIC_INFERENCE_SERVICE_SSL_CONFIGURATION_PREFIX + "verification_mode", "full");
            }
            assertHttpsRequestSucceeds(tlsServer, settings);
        }
    }

    public void testCreateWithSslServiceFailsHostnameVerification() throws Exception {
        try (var tlsServer = startTlsServer("server-no-san", false)) {
            var manager = createWithSslService(eisSslSettings());
            try (var httpClient = manager.getHttpClient()) {
                httpClient.start();

                PlainActionFuture<HttpResult> listener = new PlainActionFuture<>();
                httpClient.send(createHttpsGet(tlsServer), HttpClientContext.create(), listener);

                var exception = expectThrows(Exception.class, () -> listener.actionGet(TIMEOUT));
                var unverified = ExceptionsHelper.unwrap(exception, SSLPeerUnverifiedException.class);
                assertThat(unverified, notNullValue());
                assertThat(unverified.getMessage(), containsString("[" + tlsServer.getHostName() + "]"));
                assertThat(unverified.getMessage(), containsString("subject alternative names"));
                assertThat(unverified.getMessage(), containsString("CN=server-no-san"));
                assertThat(tlsServer.requests(), hasSize(0));
            }
        }
    }

    public void testCreateWithSslServiceSkipsHostnameVerificationWithCertificateMode() throws Exception {
        try (var tlsServer = startTlsServer("server-no-san", false)) {
            assertHttpsRequestSucceeds(
                tlsServer,
                eisSslSettings().put(ELASTIC_INFERENCE_SERVICE_SSL_CONFIGURATION_PREFIX + "verification_mode", "certificate")
            );
        }
    }

    public void testCreateWithSslServiceSendsClientCertificate() throws Exception {
        assumeFalse(
            "A bug in JDK 22 and earlier prevents the mock server from requiring certificates."
                + " See https://bugs.openjdk.org/browse/JDK-8326233",
            Runtime.version().feature() <= 22
        );
        try (var tlsServer = startTlsServer("server", true)) {
            assertHttpsRequestSucceeds(
                tlsServer,
                eisSslSettings().put(ELASTIC_INFERENCE_SERVICE_SSL_CONFIGURATION_PREFIX + "certificate", sslResource("client", ".crt"))
                    .put(ELASTIC_INFERENCE_SERVICE_SSL_CONFIGURATION_PREFIX + "key", sslResource("client", ".key"))
            );
        }
    }

    public void testStartsANewEvictor_WithNewEvictionInterval() {
        var threadPool = mock(ThreadPool.class);
        var manager = HttpClientManager.create(
            Settings.EMPTY,
            threadPool,
            mockClusterServiceEmpty(),
            mock(ThrottlerManager.class),
            new TestCircuitBreaker()
        );

        var evictionInterval = TimeValue.timeValueSeconds(1);
        manager.setEvictionInterval(evictionInterval);
        verify(threadPool).scheduleWithFixedDelay(any(Runnable.class), eq(evictionInterval), any());
    }

    public void test_DoesNotStartANewEvictor_WithNewEvictionMaxIdle() {
        var mockConnectionManager = mock(PoolingNHttpClientConnectionManager.class);

        Settings settings = Settings.builder()
            .put(HttpClientManager.CONNECTION_EVICTION_THREAD_INTERVAL_SETTING.getKey(), TimeValue.timeValueNanos(1))
            .build();
        var manager = new HttpClientManager(
            settings,
            mockConnectionManager,
            threadPool,
            mockClusterService(settings),
            mock(ThrottlerManager.class),
            new TestCircuitBreaker()
        );

        var evictionMaxIdle = TimeValue.timeValueSeconds(1);
        manager.setConnectionMaxIdle(evictionMaxIdle);

        assertFalse(manager.isEvictionThreadRunning());
    }

    private void assertHttpsRequestSucceeds(MockWebServer tlsServer, Settings.Builder sslSettings) throws Exception {
        String body = randomAlphaOfLengthBetween(2, 100);
        tlsServer.enqueue(new MockResponse().setResponseCode(200).setBody(body));

        var manager = createWithSslService(sslSettings);
        try (var httpClient = manager.getHttpClient()) {
            httpClient.start();

            PlainActionFuture<HttpResult> listener = new PlainActionFuture<>();
            httpClient.send(createHttpsGet(tlsServer), HttpClientContext.create(), listener);

            var result = listener.actionGet(TIMEOUT);
            assertThat(result.response().getStatusLine().getStatusCode(), equalTo(200));
            assertThat(new String(result.body(), StandardCharsets.UTF_8), is(body));
            assertThat(tlsServer.requests(), hasSize(1));
        }
    }

    private HttpClientManager createWithSslService(Settings.Builder sslSettings) {
        var sslService = new SSLService(newEnvironment(sslSettings.build()));
        return HttpClientManager.create(
            Settings.EMPTY,
            threadPool,
            mockClusterServiceEmpty(),
            mock(ThrottlerManager.class),
            sslService,
            TimeValue.timeValueSeconds(60),
            new TestCircuitBreaker()
        );
    }

    private Settings.Builder eisSslSettings() throws IOException {
        return Settings.builder()
            .putList(ELASTIC_INFERENCE_SERVICE_SSL_CONFIGURATION_PREFIX + "certificate_authorities", sslResource("ca", ".crt").toString());
    }

    private MockWebServer startTlsServer(String serverCert, boolean requireClientCert) throws Exception {
        var settings = Settings.builder()
            .put("ssl.certificate", sslResource(serverCert, ".crt"))
            .put("ssl.key", sslResource(serverCert, ".key"))
            .put("ssl.client_authentication", requireClientCert ? "required" : "none");
        if (requireClientCert) {
            settings.putList("ssl.certificate_authorities", sslResource("ca", ".crt").toString());
        }
        SslConfiguration sslConfiguration = SslSettingsLoader.load(settings.build(), "ssl.", newEnvironment());
        var tlsServer = new MockWebServer(sslConfiguration.createSslContext(), new MockWebServer.TlsConfig(sslConfiguration));
        // The test certificates only have SANs for 127.0.0.1 and localhost
        tlsServer.start(InetAddress.getByName("127.0.0.1"));
        return tlsServer;
    }

    /**
     * Copies the certificate files from the x-pack core test artifact, which is a jar on this classpath, to a temp dir.
     */
    private Path sslResource(String name, String extension) throws IOException {
        Path file = sslResourceDir.resolve(name + extension);
        if (Files.exists(file) == false) {
            try (
                var in = HttpClientManagerTests.class.getResourceAsStream(
                    "/org/elasticsearch/xpack/core/ssl/" + name + "/" + name + extension
                )
            ) {
                Files.copy(Objects.requireNonNull(in), file);
            }
        }
        return file;
    }

    private static HttpRequest createHttpsGet(MockWebServer tlsServer) throws Exception {
        return new HttpRequest(new HttpGet("https://" + tlsServer.getHostName() + ":" + tlsServer.getPort() + "/"), "inferenceEntityId");
    }
}
