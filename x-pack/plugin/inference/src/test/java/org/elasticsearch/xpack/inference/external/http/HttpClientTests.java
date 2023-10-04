/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.nio.conn.NHttpClientConnectionManager;
import org.apache.http.nio.reactor.IOReactorException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class HttpClientTests extends ESTestCase {
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private final MockWebServer webServer = new MockWebServer();

    private ThreadPool threadPool;
    private HttpClient httpClient;

    @Before
    public void init() throws Exception {
        webServer.start();
        threadPool = new TestThreadPool(
            getTestName(),
            new ScalingExecutorBuilder(
                UTILITY_THREAD_POOL_NAME,
                1,
                4,
                TimeValue.timeValueMinutes(10),
                false,
                "xpack.inference.utility_thread_pool"
            )
        );

        httpClient = HttpClient.create(Settings.EMPTY, threadPool);
    }

    @After
    public void shutdown() throws IOException {
        httpClient.close();
        terminate(threadPool);
        webServer.close();
    }

    public void testBasicRequest() throws Exception {
        int responseCode = randomIntBetween(200, 203);
        String body = randomAlphaOfLengthBetween(2, 8096);
        webServer.enqueue(new MockResponse().setResponseCode(responseCode).setBody(body));

        String paramKey = randomAlphaOfLength(3);
        String paramValue = randomAlphaOfLength(3);
        URI uri = new URIBuilder().setScheme("http")
            .setHost("localhost")
            .setPort(webServer.getPort())
            .setPathSegments("/" + randomAlphaOfLength(5))
            .setParameter(paramKey, paramValue)
            .build();

        HttpPost httpPost = new HttpPost(uri);

        ByteArrayEntity byteEntity = new ByteArrayEntity(randomAlphaOfLength(5).getBytes(StandardCharsets.UTF_8));
        httpPost.setEntity(byteEntity);

        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType());

        PlainActionFuture<HttpResult> listener = new PlainActionFuture<>();
        httpClient.send(httpPost, listener);

        var result = listener.actionGet(TIMEOUT);

        assertThat(result.response().getStatusLine().getStatusCode(), equalTo(responseCode));
        assertThat(new String(result.body(), StandardCharsets.UTF_8), is(body));
        assertThat(webServer.requests(), hasSize(1));
        assertThat(webServer.requests().get(0).getUri().getPath(), equalTo(uri.getPath()));
        assertThat(webServer.requests().get(0).getUri().getQuery(), equalTo(paramKey + "=" + paramValue));
        assertThat(webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
    }

    private static IdleConnectionEvictor createEvictor(ThreadPool threadPool, NHttpClientConnectionManager connectionManager) {
        return new IdleConnectionEvictor(
            threadPool,
            connectionManager,
            HttpSettings.CONNECTION_EVICTION_THREAD_SLEEP_TIME_SETTING.get(Settings.EMPTY),
            HttpSettings.CONNECTION_EVICTION_MAX_IDLE_TIME_SETTING.get(Settings.EMPTY)
        );
    }

    private static PoolingNHttpClientConnectionManager createConnectionManager() throws IOReactorException {
        return new PoolingNHttpClientConnectionManager(new DefaultConnectingIOReactor());
    }
}
