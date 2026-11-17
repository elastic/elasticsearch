/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http;

import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.breaker.TestCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

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
 * {@code xpack.inference.http.proxy.*} settings, and connects directly to the target host when no proxy is configured. Requests are sent
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

            assertThat(result.response().getCode(), equalTo(responseCode));
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

            assertThat(result.response().getCode(), equalTo(responseCode));
            assertThat(new String(result.body(), StandardCharsets.UTF_8), is(body));

            // the proxy saw nothing and the upstream was reached directly (no proxy hop marker)
            assertThat(proxyServer.proxiedRequestLines(), empty());

            assertThat(upstreamServer.requests(), hasSize(1));
            assertThat(upstreamServer.requests().getFirst().getHeader(MockHttpProxyServer.VIA_HEADER), nullValue());
        }
    }

    private HttpClient createHttpClient(Settings settings) throws Exception {
        var httpSettings = new HttpSettings(settings, mockClusterService(settings));
        return HttpClient.create(httpSettings, threadPool, createConnectionManager(), mockThrottlerManager(), new TestCircuitBreaker());
    }
}
