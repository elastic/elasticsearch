/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.huggingface;

import org.apache.http.HttpHeaders;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderFactory;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig.DEFAULT_RESULTS_FIELD;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.external.http.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.external.http.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.retry.RetrySettingsTests.buildSettingsWithRetryFields;
import static org.elasticsearch.xpack.inference.external.request.huggingface.HuggingFaceElserRequestTests.createRequest;
import static org.elasticsearch.xpack.inference.logging.ThrottlerManagerTests.mockThrottlerManager;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class HuggingFaceClientTests extends ESTestCase {
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private final MockWebServer webServer = new MockWebServer();
    private ThreadPool threadPool;
    private HttpClientManager clientManager;

    @Before
    public void init() throws Exception {
        webServer.start();
        threadPool = createThreadPool(inferenceUtilityPool());
        clientManager = HttpClientManager.create(Settings.EMPTY, threadPool, mockClusterServiceEmpty(), mockThrottlerManager());
    }

    @After
    public void shutdown() throws IOException {
        clientManager.close();
        terminate(threadPool);
        webServer.close();
    }

    @SuppressWarnings("unchecked")
    public void testSend_SuccessfulResponse() throws IOException, URISyntaxException {
        var senderFactory = new HttpRequestSenderFactory(threadPool, clientManager, mockClusterServiceEmpty(), Settings.EMPTY);

        try (var sender = senderFactory.createSender("test_service")) {
            sender.start();

            String responseJson = """
                [
                    {
                        ".": 0.133155956864357
                    }
                ]
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            HuggingFaceClient huggingFaceClient = new HuggingFaceClient(
                sender,
                new ServiceComponents(threadPool, mockThrottlerManager(), Settings.EMPTY)
            );

            PlainActionFuture<List<? extends InferenceResults>> listener = new PlainActionFuture<>();
            huggingFaceClient.send(createRequest(getUrl(webServer), "secret", "abc"), listener);

            var result = listener.actionGet(TIMEOUT).get(0);

            assertThat(result.asMap(), is(Map.of(DEFAULT_RESULTS_FIELD, Map.of(".", 0.13315596f))));

            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().get(0).getUri().getQuery());
            assertThat(
                webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE),
                equalTo(XContentType.JSON.mediaTypeWithoutParameters())
            );
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.size(), is(1));
            assertThat(requestMap.get("inputs"), instanceOf(List.class));
            var inputList = (List<String>) requestMap.get("inputs");
            assertThat(inputList, contains("abc"));
        }
    }

    @SuppressWarnings("unchecked")
    public void testSend_FailsFromInvalidResponseFormat() throws IOException, URISyntaxException {
        var senderFactory = new HttpRequestSenderFactory(threadPool, clientManager, mockClusterServiceEmpty(), Settings.EMPTY);

        try (var sender = senderFactory.createSender("test_service")) {
            sender.start();

            String responseJson = """
                [
                  {
                    "outputs": [
                      [
                        [
                          ".",
                          ".",
                          0.133155956864357
                        ]
                      ]
                    ]
                  }
                ]
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            HuggingFaceClient huggingFaceClient = new HuggingFaceClient(
                sender,
                new ServiceComponents(
                    threadPool,
                    mockThrottlerManager(),
                    // timeout as zero for no retries
                    buildSettingsWithRetryFields(TimeValue.timeValueMillis(1), TimeValue.timeValueMinutes(1), TimeValue.timeValueSeconds(0))
                )
            );

            PlainActionFuture<List<? extends InferenceResults>> listener = new PlainActionFuture<>();
            huggingFaceClient.send(createRequest(getUrl(webServer), "secret", "abc"), listener);

            var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(
                thrownException.getMessage(),
                is("Failed to parse object: expecting token of type [VALUE_NUMBER] but found [START_ARRAY]")
            );

            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().get(0).getUri().getQuery());
            assertThat(
                webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE),
                equalTo(XContentType.JSON.mediaTypeWithoutParameters())
            );
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.size(), is(1));
            assertThat(requestMap.get("inputs"), instanceOf(List.class));
            var inputList = (List<String>) requestMap.get("inputs");
            assertThat(inputList, contains("abc"));
        }
    }

    public void testSend_ThrowsException() throws URISyntaxException, IOException {
        var sender = mock(Sender.class);
        doThrow(new ElasticsearchException("failed")).when(sender).send(any(), any());

        HuggingFaceClient huggingFaceClient = new HuggingFaceClient(
            sender,
            new ServiceComponents(threadPool, mockThrottlerManager(), Settings.EMPTY)
        );
        PlainActionFuture<List<? extends InferenceResults>> listener = new PlainActionFuture<>();

        huggingFaceClient.send(createRequest(getUrl(webServer), "secret", "abc"), listener);

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(thrownException.getMessage(), is("failed"));
    }

}
