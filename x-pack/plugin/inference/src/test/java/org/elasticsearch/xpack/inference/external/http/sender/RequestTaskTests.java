/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.HttpClient;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.external.http.HttpClientTests.createConnectionManager;
import static org.elasticsearch.xpack.inference.external.http.HttpClientTests.createHttpPost;
import static org.elasticsearch.xpack.inference.external.http.HttpClientTests.createThreadPool;
import static org.elasticsearch.xpack.inference.external.http.HttpClientTests.emptyHttpSettings;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class RequestTaskTests extends ESTestCase {
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private final MockWebServer webServer = new MockWebServer();
    private ThreadPool threadPool;

    @Before
    public void init() throws Exception {
        webServer.start();
        threadPool = createThreadPool(getTestName());
    }

    @After
    public void shutdown() {
        terminate(threadPool);
        webServer.close();
    }

    public void testDoRun_SendsRequestAndReceivesResponse() throws Exception {
        int responseCode = randomIntBetween(200, 203);
        String body = randomAlphaOfLengthBetween(2, 8096);
        webServer.enqueue(new MockResponse().setResponseCode(responseCode).setBody(body));

        String paramKey = randomAlphaOfLength(3);
        String paramValue = randomAlphaOfLength(3);
        var httpPost = createHttpPost(webServer.getPort(), paramKey, paramValue);

        try (var httpClient = HttpClient.create(emptyHttpSettings(), threadPool, createConnectionManager())) {
            httpClient.start();

            PlainActionFuture<HttpResult> listener = new PlainActionFuture<>();
            var requestTask = new RequestTask(httpPost, httpClient, listener);
            requestTask.setContext(HttpClientContext.create());
            requestTask.doRun();
            var result = listener.actionGet(TIMEOUT);

            assertThat(result.response().getStatusLine().getStatusCode(), equalTo(responseCode));
            assertThat(new String(result.body(), StandardCharsets.UTF_8), is(body));
            assertThat(webServer.requests(), hasSize(1));
            assertThat(webServer.requests().get(0).getUri().getPath(), equalTo(httpPost.getURI().getPath()));
            assertThat(webServer.requests().get(0).getUri().getQuery(), equalTo(paramKey + "=" + paramValue));
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
        }
    }

    public void testDoRun_Throws_WhenContextIsNotSet() {
        PlainActionFuture<HttpResult> listener = new PlainActionFuture<>();

        var requestTask = new RequestTask(mock(HttpUriRequest.class), mock(HttpClient.class), listener);
        var thrownException = expectThrows(AssertionError.class, requestTask::doRun);
        assertThat(thrownException.getMessage(), is("the http context must be set before calling doRun"));
    }

    public void testDoRun_SendThrowsIOException() throws Exception {
        var httpClient = mock(HttpClient.class);
        doThrow(new IOException("exception")).when(httpClient).send(any(), any(), any());

        String paramKey = randomAlphaOfLength(3);
        String paramValue = randomAlphaOfLength(3);
        var httpPost = createHttpPost(webServer.getPort(), paramKey, paramValue);

        PlainActionFuture<HttpResult> listener = new PlainActionFuture<>();
        var requestTask = new RequestTask(httpPost, httpClient, listener);
        requestTask.setContext(HttpClientContext.create());
        requestTask.doRun();

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(thrownException.getMessage(), is(format("Failed to send request [%s]", httpPost.getRequestLine())));
    }
}
