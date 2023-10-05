/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http;

import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.UncategorizedExecutionException;
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

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HttpClientTests extends ESTestCase {
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private final MockWebServer webServer = new MockWebServer();
    private ThreadPool threadPool;

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

        try (var httpClient = HttpClient.create(Settings.EMPTY, threadPool)) {
            PlainActionFuture<HttpResult> listener = new PlainActionFuture<>();
            httpClient.send(httpPost, listener);

            var result = listener.actionGet(TIMEOUT);

            assertThat(result.response().getStatusLine().getStatusCode(), equalTo(responseCode));
            assertThat(new String(result.body(), StandardCharsets.UTF_8), is(body));
            assertThat(webServer.requests(), hasSize(1));
            assertThat(webServer.requests().get(0).getUri().getPath(), equalTo(httpPost.getURI().getPath()));
            assertThat(webServer.requests().get(0).getUri().getQuery(), equalTo(paramKey + "=" + paramValue));
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
        }
    }

    public void testSend_FailedCallsOnFailure() throws Exception {
        var asyncClient = mock(CloseableHttpAsyncClient.class);

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            FutureCallback<HttpResponse> listener = (FutureCallback<HttpResponse>) invocation.getArguments()[1];
            listener.failed(new ElasticsearchException("failure"));
            return mock(Future.class);
        }).when(asyncClient).execute(any(), any());

        var evictor = createEvictor(threadPool);
        var httpPost = createHttpPost(webServer.getPort(), "a", "b");

        try (var client = new HttpClient(Settings.EMPTY, asyncClient, evictor, threadPool)) {
            PlainActionFuture<HttpResult> listener = new PlainActionFuture<>();
            client.send(httpPost, listener);

            var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(thrownException.getMessage(), is("failure"));
        }
    }

    public void testSend_CancelledCallsOnFailure() throws Exception {
        var asyncClient = mock(CloseableHttpAsyncClient.class);

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            FutureCallback<HttpResponse> listener = (FutureCallback<HttpResponse>) invocation.getArguments()[1];
            listener.cancelled();
            return mock(Future.class);
        }).when(asyncClient).execute(any(), any());

        var evictor = createEvictor(threadPool);
        var httpPost = createHttpPost(webServer.getPort(), "a", "b");

        try (var client = new HttpClient(Settings.EMPTY, asyncClient, evictor, threadPool)) {
            PlainActionFuture<HttpResult> listener = new PlainActionFuture<>();
            client.send(httpPost, listener);

            var thrownException = expectThrows(CancellationException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(thrownException.getMessage(), is(format("Request [%s] was cancelled", httpPost.getRequestLine())));
        }
    }

    @SuppressWarnings("unchecked")
    public void testStart_MultipleCallsOnlyStartTheClientOnce() throws Exception {
        var asyncClient = mock(CloseableHttpAsyncClient.class);
        when(asyncClient.execute(any(), any())).thenReturn(mock(Future.class));

        var evictor = createEvictor(threadPool);
        var httpPost = createHttpPost(webServer.getPort(), "a", "b");

        try (var client = new HttpClient(Settings.EMPTY, asyncClient, evictor, threadPool)) {
            PlainActionFuture<HttpResult> listener = new PlainActionFuture<>();
            client.send(httpPost, listener);
            client.send(httpPost, listener);

            verify(asyncClient, times(1)).start();
        }
    }

    public void testSend_FailsWhenMaxBytesReadIsExceeded() throws Exception {
        int responseCode = randomIntBetween(200, 203);
        String body = randomAlphaOfLengthBetween(10, 8096);
        webServer.enqueue(new MockResponse().setResponseCode(responseCode).setBody(body));

        String paramKey = randomAlphaOfLength(3);
        String paramValue = randomAlphaOfLength(3);
        var httpPost = createHttpPost(webServer.getPort(), paramKey, paramValue);

        Settings settings = Settings.builder().put(HttpSettings.MAX_HTTP_RESPONSE_SIZE.getKey(), ByteSizeValue.ONE).build();

        try (var httpClient = HttpClient.create(settings, threadPool)) {
            PlainActionFuture<HttpResult> listener = new PlainActionFuture<>();
            httpClient.send(httpPost, listener);

            var throwException = expectThrows(UncategorizedExecutionException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(throwException.getCause().getCause().getMessage(), is("Maximum limit of [1] bytes reached"));
        }
    }

    private static HttpPost createHttpPost(int port, String paramKey, String paramValue) throws URISyntaxException {
        URI uri = new URIBuilder().setScheme("http")
            .setHost("localhost")
            .setPort(port)
            .setPathSegments("/" + randomAlphaOfLength(5))
            .setParameter(paramKey, paramValue)
            .build();

        HttpPost httpPost = new HttpPost(uri);

        ByteArrayEntity byteEntity = new ByteArrayEntity(
            randomAlphaOfLength(5).getBytes(StandardCharsets.UTF_8),
            ContentType.APPLICATION_JSON
        );
        httpPost.setEntity(byteEntity);

        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType());
        return httpPost;
    }

    private static IdleConnectionEvictor createEvictor(ThreadPool threadPool) throws IOReactorException {
        var manager = createConnectionManager();
        return new IdleConnectionEvictor(
            threadPool,
            manager,
            HttpSettings.CONNECTION_EVICTION_THREAD_SLEEP_TIME_SETTING.get(Settings.EMPTY),
            HttpSettings.CONNECTION_EVICTION_MAX_IDLE_TIME_SETTING.get(Settings.EMPTY)
        );
    }

    private static PoolingNHttpClientConnectionManager createConnectionManager() throws IOReactorException {
        return new PoolingNHttpClientConnectionManager(new DefaultConnectingIOReactor());
    }
}
