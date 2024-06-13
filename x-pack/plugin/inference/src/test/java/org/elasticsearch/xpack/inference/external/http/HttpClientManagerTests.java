/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http;

import org.apache.http.HttpHeaders;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.junit.After;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.Utils.mockClusterService;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.HttpClientTests.createHttpPost;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class HttpClientManagerTests extends ESTestCase {
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);

    private final MockWebServer webServer = new MockWebServer();
    private ThreadPool threadPool;

    @Before
    public void init() throws Exception {
        webServer.start();
        threadPool = createThreadPool(inferenceUtilityPool());
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

        var manager = HttpClientManager.create(Settings.EMPTY, threadPool, mockClusterServiceEmpty(), mock(ThrottlerManager.class));
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

    public void testStartsANewEvictor_WithNewEvictionInterval() {
        var threadPool = mock(ThreadPool.class);
        var manager = HttpClientManager.create(Settings.EMPTY, threadPool, mockClusterServiceEmpty(), mock(ThrottlerManager.class));

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
            mock(ThrottlerManager.class)
        );

        var evictionMaxIdle = TimeValue.timeValueSeconds(1);
        manager.setConnectionMaxIdle(evictionMaxIdle);

        assertFalse(manager.isEvictionThreadRunning());
    }
}
