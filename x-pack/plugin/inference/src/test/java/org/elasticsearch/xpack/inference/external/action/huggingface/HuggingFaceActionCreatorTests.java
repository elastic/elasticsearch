/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action.huggingface;

import org.apache.http.HttpHeaders;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.inference.common.TruncatorTests;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.DocumentsOnlyInput;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.results.SparseEmbeddingResultsTests;
import org.elasticsearch.xpack.inference.results.TextEmbeddingResultsTests;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.huggingface.elser.HuggingFaceElserModelTests;
import org.elasticsearch.xpack.inference.services.huggingface.embeddings.HuggingFaceEmbeddingsModelTests;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.external.http.retry.RetrySettingsTests.buildSettingsWithRetryFields;
import static org.elasticsearch.xpack.inference.logging.ThrottlerManagerTests.mockThrottlerManager;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class HuggingFaceActionCreatorTests extends ESTestCase {
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private final MockWebServer webServer = new MockWebServer();
    private ThreadPool threadPool;
    private HttpClientManager clientManager;

    @Before
    public void init() throws Exception {
        webServer.start();
        threadPool = createThreadPool(inferenceUtilityPool());
        clientManager = HttpClientManager.create(Settings.EMPTY, threadPool, mockClusterServiceEmpty(), mock(ThrottlerManager.class));
    }

    @After
    public void shutdown() throws IOException {
        clientManager.close();
        terminate(threadPool);
        webServer.close();
    }

    @SuppressWarnings("unchecked")
    public void testExecute_ReturnsSuccessfulResponse_ForElserAction() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

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

            var model = HuggingFaceElserModelTests.createModel(getUrl(webServer), "secret");
            var actionCreator = new HuggingFaceActionCreator(sender, createWithEmptySettings(threadPool));
            var action = actionCreator.create(model);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new DocumentsOnlyInput(List.of("abc")), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

            var result = listener.actionGet(TIMEOUT);

            assertThat(
                result.asMap(),
                is(
                    SparseEmbeddingResultsTests.buildExpectationSparseEmbeddings(
                        List.of(new SparseEmbeddingResultsTests.EmbeddingExpectation(Map.of(".", 0.13315596f), false))
                    )
                )
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

    @SuppressWarnings("unchecked")
    public void testSend_FailsFromInvalidResponseFormat_ForElserAction() throws IOException {
        // timeout as zero for no retries
        var settings = buildSettingsWithRetryFields(
            TimeValue.timeValueMillis(1),
            TimeValue.timeValueMinutes(1),
            TimeValue.timeValueSeconds(0)
        );
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager, settings);

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

            var model = HuggingFaceElserModelTests.createModel(getUrl(webServer), "secret");
            var actionCreator = new HuggingFaceActionCreator(
                sender,
                new ServiceComponents(threadPool, mockThrottlerManager(), settings, TruncatorTests.createTruncator())
            );
            var action = actionCreator.create(model);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new DocumentsOnlyInput(List.of("abc")), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

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

    @SuppressWarnings("unchecked")
    public void testExecute_ReturnsSuccessfulResponse_ForEmbeddingsAction() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = senderFactory.createSender("test_service")) {
            sender.start();

            String responseJson = """
                {
                    "embeddings": [
                        [
                            -0.0123,
                            0.123
                        ]
                    ]
                {
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = HuggingFaceEmbeddingsModelTests.createModel(getUrl(webServer), "secret");
            var actionCreator = new HuggingFaceActionCreator(sender, createWithEmptySettings(threadPool));
            var action = actionCreator.create(model);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new DocumentsOnlyInput(List.of("abc")), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

            var result = listener.actionGet(TIMEOUT);

            assertThat(result.asMap(), is(TextEmbeddingResultsTests.buildExpectationFloat(List.of(new float[] { -0.0123F, 0.123F }))));

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
    public void testSend_FailsFromInvalidResponseFormat_ForEmbeddingsAction() throws IOException {
        // timeout as zero for no retries
        var settings = buildSettingsWithRetryFields(
            TimeValue.timeValueMillis(1),
            TimeValue.timeValueMinutes(1),
            TimeValue.timeValueSeconds(0)
        );
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager, settings);

        try (var sender = senderFactory.createSender("test_service")) {
            sender.start();

            // this will fail because the only valid formats are {"embeddings": [[...]]} or [[...]]
            String responseJson = """
                [
                    {
                        "embeddings": [
                            [
                                -0.0123,
                                0.123
                            ]
                        ]
                    {
                ]
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = HuggingFaceEmbeddingsModelTests.createModel(getUrl(webServer), "secret");
            var actionCreator = new HuggingFaceActionCreator(
                sender,
                new ServiceComponents(threadPool, mockThrottlerManager(), settings, TruncatorTests.createTruncator())
            );
            var action = actionCreator.create(model);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new DocumentsOnlyInput(List.of("abc")), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

            var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(
                thrownException.getMessage(),
                is("Failed to parse object: expecting token of type [START_ARRAY] but found [START_OBJECT]")
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

    public void testExecute_ReturnsSuccessfulResponse_AfterTruncating() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = senderFactory.createSender("test_service")) {
            sender.start();

            String responseJsonContentTooLarge = """
                {
                    "error": "Input validation error: `inputs` must have less than 512 tokens. Given: 571",
                    "error_type": "Validation"
                }
                """;

            String responseJson = """
                {
                    "embeddings": [
                        [
                            -0.0123,
                            0.123
                        ]
                    ]
                {
                """;
            webServer.enqueue(new MockResponse().setResponseCode(413).setBody(responseJsonContentTooLarge));
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = HuggingFaceEmbeddingsModelTests.createModel(getUrl(webServer), "secret");
            var actionCreator = new HuggingFaceActionCreator(sender, createWithEmptySettings(threadPool));
            var action = actionCreator.create(model);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new DocumentsOnlyInput(List.of("abcd")), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

            var result = listener.actionGet(TIMEOUT);

            assertThat(result.asMap(), is(TextEmbeddingResultsTests.buildExpectationFloat(List.of(new float[] { -0.0123F, 0.123F }))));

            assertThat(webServer.requests(), hasSize(2));
            {
                assertNull(webServer.requests().get(0).getUri().getQuery());
                assertThat(
                    webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE),
                    equalTo(XContentType.JSON.mediaTypeWithoutParameters())
                );
                assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

                var initialRequestAsMap = entityAsMap(webServer.requests().get(0).getBody());
                var initialInputs = initialRequestAsMap.get("inputs");
                assertThat(initialInputs, is(List.of("abcd")));
            }
            {
                assertNull(webServer.requests().get(1).getUri().getQuery());
                assertThat(
                    webServer.requests().get(1).getHeader(HttpHeaders.CONTENT_TYPE),
                    equalTo(XContentType.JSON.mediaTypeWithoutParameters())
                );
                assertThat(webServer.requests().get(1).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

                var truncatedRequest = entityAsMap(webServer.requests().get(1).getBody());
                var truncatedInputs = truncatedRequest.get("inputs");
                assertThat(truncatedInputs, is(List.of("ab")));
            }
        }
    }

    public void testExecute_TruncatesInputBeforeSending() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = senderFactory.createSender("test_service")) {
            sender.start();

            String responseJson = """
                {
                    "embeddings": [
                        [
                            -0.0123,
                            0.123
                        ]
                    ]
                {
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            // truncated to 1 token = 3 characters
            var model = HuggingFaceEmbeddingsModelTests.createModel(getUrl(webServer), "secret", 1);
            var actionCreator = new HuggingFaceActionCreator(sender, createWithEmptySettings(threadPool));
            var action = actionCreator.create(model);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new DocumentsOnlyInput(List.of("123456")), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

            var result = listener.actionGet(TIMEOUT);

            assertThat(result.asMap(), is(TextEmbeddingResultsTests.buildExpectationFloat(List.of(new float[] { -0.0123F, 0.123F }))));

            assertThat(webServer.requests(), hasSize(1));

            assertNull(webServer.requests().get(0).getUri().getQuery());
            assertThat(
                webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE),
                equalTo(XContentType.JSON.mediaTypeWithoutParameters())
            );
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

            var initialRequestAsMap = entityAsMap(webServer.requests().get(0).getBody());
            var initialInputs = initialRequestAsMap.get("inputs");
            assertThat(initialInputs, is(List.of("123")));

        }
    }
}
