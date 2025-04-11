/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.action;

import org.apache.http.HttpHeaders;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResultsTests;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSparseEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.telemetry.TraceContext;
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
import static org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests.createSender;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class ElasticInferenceServiceActionCreatorTests extends ESTestCase {

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

        try (var sender = createSender(senderFactory)) {
            sender.start();

            String responseJson = """
                {
                    "data": [
                        {
                            "hello": 2.1259406,
                            "greet": 1.7073475
                        }
                    ]
                }
                """;

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = ElasticInferenceServiceSparseEmbeddingsModelTests.createModel(getUrl(webServer), "my-model-id");
            var actionCreator = new ElasticInferenceServiceActionCreator(sender, createWithEmptySettings(threadPool), createTraceContext());
            var action = actionCreator.create(model);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(
                new EmbeddingsInput(List.of("hello world"), null, InputType.UNSPECIFIED),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(TIMEOUT);

            assertThat(
                result.asMap(),
                is(
                    SparseEmbeddingResultsTests.buildExpectationSparseEmbeddings(
                        List.of(
                            new SparseEmbeddingResultsTests.EmbeddingExpectation(Map.of("hello", 2.1259406f, "greet", 1.7073475f), false)
                        )
                    )
                )
            );

            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().get(0).getUri().getQuery());
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.size(), is(2));
            assertThat(requestMap.get("input"), instanceOf(List.class));
            var inputList = (List<String>) requestMap.get("input");
            assertThat(inputList, contains("hello world"));
            assertThat(requestMap.get("model"), is("my-model-id"));
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

        try (var sender = createSender(senderFactory)) {
            sender.start();

            // This will fail because the expected output is {"data": [{...}]}
            String responseJson = """
                {
                    "data": {
                        "hello": 2.1259406
                    }
                }
                """;

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = ElasticInferenceServiceSparseEmbeddingsModelTests.createModel(getUrl(webServer), "my-model-id");
            var actionCreator = new ElasticInferenceServiceActionCreator(sender, createWithEmptySettings(threadPool), createTraceContext());
            var action = actionCreator.create(model);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(
                new EmbeddingsInput(List.of("hello world"), null, InputType.UNSPECIFIED),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(
                thrownException.getMessage(),
                is("Failed to parse object: expecting token of type [START_ARRAY] but found [START_OBJECT]")
            );

            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().get(0).getUri().getQuery());
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.size(), is(2));
            assertThat(requestMap.get("input"), instanceOf(List.class));
            var inputList = (List<String>) requestMap.get("input");
            assertThat(inputList, contains("hello world"));
            assertThat(requestMap.get("model"), is("my-model-id"));
        }
    }

    public void testExecute_ReturnsSuccessfulResponse_AfterTruncating() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = createSender(senderFactory)) {
            sender.start();

            String responseJsonContentTooLarge = """
                {
                    "error": "Input validation error: `input` must have less than 512 tokens. Given: 571",
                    "error_type": "Validation"
                }
                """;

            String responseJson = """
                {
                    "data": [
                        {
                            "hello": 2.1259406,
                            "greet": 1.7073475
                        }
                    ]
                }
                """;

            webServer.enqueue(new MockResponse().setResponseCode(413).setBody(responseJsonContentTooLarge));
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = ElasticInferenceServiceSparseEmbeddingsModelTests.createModel(getUrl(webServer), "my-model-id");
            var actionCreator = new ElasticInferenceServiceActionCreator(sender, createWithEmptySettings(threadPool), createTraceContext());
            var action = actionCreator.create(model);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(
                new EmbeddingsInput(List.of("hello world"), null, InputType.UNSPECIFIED),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(TIMEOUT);

            assertThat(
                result.asMap(),
                is(
                    SparseEmbeddingResultsTests.buildExpectationSparseEmbeddings(
                        List.of(
                            new SparseEmbeddingResultsTests.EmbeddingExpectation(Map.of("hello", 2.1259406f, "greet", 1.7073475f), true)
                        )
                    )
                )
            );

            assertThat(webServer.requests(), hasSize(2));
            {
                assertNull(webServer.requests().get(0).getUri().getQuery());
                assertThat(webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));

                var initialRequestAsMap = entityAsMap(webServer.requests().get(0).getBody());
                var initialInputs = initialRequestAsMap.get("input");
                assertThat(initialInputs, is(List.of("hello world")));
            }
            {
                assertNull(webServer.requests().get(1).getUri().getQuery());
                assertThat(webServer.requests().get(1).getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));

                var truncatedRequest = entityAsMap(webServer.requests().get(1).getBody());
                var truncatedInputs = truncatedRequest.get("input");
                assertThat(truncatedInputs, is(List.of("hello")));
            }
        }
    }

    public void testExecute_TruncatesInputBeforeSending() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = createSender(senderFactory)) {
            sender.start();

            String responseJson = """
                {
                    "data": [
                        {
                            "hello": 2.1259406,
                            "greet": 1.7073475
                        }
                    ]
                }
                """;

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            // truncated to 1 token = 3 characters
            var model = ElasticInferenceServiceSparseEmbeddingsModelTests.createModel(getUrl(webServer), "my-model-id", 1);
            var actionCreator = new ElasticInferenceServiceActionCreator(sender, createWithEmptySettings(threadPool), createTraceContext());
            var action = actionCreator.create(model);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(
                new EmbeddingsInput(List.of("hello world"), null, InputType.UNSPECIFIED),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(TIMEOUT);

            assertThat(
                result.asMap(),
                is(
                    SparseEmbeddingResultsTests.buildExpectationSparseEmbeddings(
                        List.of(
                            new SparseEmbeddingResultsTests.EmbeddingExpectation(Map.of("hello", 2.1259406f, "greet", 1.7073475f), true)
                        )
                    )
                )
            );

            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().get(0).getUri().getQuery());
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));

            var initialRequestAsMap = entityAsMap(webServer.requests().get(0).getBody());
            var initialInputs = initialRequestAsMap.get("input");
            assertThat(initialInputs, is(List.of("hel")));
        }
    }

    private TraceContext createTraceContext() {
        return new TraceContext(randomAlphaOfLength(10), randomAlphaOfLength(10));
    }

}
