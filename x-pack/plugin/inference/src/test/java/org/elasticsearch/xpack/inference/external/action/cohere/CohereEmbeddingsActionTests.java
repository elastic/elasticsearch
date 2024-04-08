/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action.cohere;

import org.apache.http.HttpHeaders;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.sender.DocumentsOnlyInput;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.request.cohere.CohereUtils;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.results.TextEmbeddingByteResultsTests;
import org.elasticsearch.xpack.inference.services.cohere.CohereTruncation;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingType;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingsTaskSettings;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.results.TextEmbeddingResultsTests.buildExpectation;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class CohereEmbeddingsActionTests extends ESTestCase {
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

    public void testExecute_ReturnsSuccessfulResponse() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = HttpRequestSenderTests.createSenderWithSingleRequestManager(senderFactory, "test_service")) {
            sender.start();

            String responseJson = """
                {
                    "id": "de37399c-5df6-47cb-bc57-e3c5680c977b",
                    "texts": [
                        "hello"
                    ],
                    "embeddings": {
                        "float": [
                            [
                                0.123,
                                -0.123
                            ]
                        ]
                    },
                    "meta": {
                        "api_version": {
                            "version": "1"
                        },
                        "billed_units": {
                            "input_tokens": 1
                        }
                    },
                    "response_type": "embeddings_by_type"
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var action = createAction(
                getUrl(webServer),
                "secret",
                new CohereEmbeddingsTaskSettings(InputType.INGEST, CohereTruncation.START),
                "model",
                CohereEmbeddingType.FLOAT,
                sender
            );

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new DocumentsOnlyInput(List.of("abc")), listener);

            var result = listener.actionGet(TIMEOUT);

            MatcherAssert.assertThat(result.asMap(), is(buildExpectation(List.of(List.of(0.123F, -0.123F)))));
            MatcherAssert.assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().get(0).getUri().getQuery());
            MatcherAssert.assertThat(
                webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE),
                equalTo(XContentType.JSON.mediaType())
            );
            MatcherAssert.assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));
            MatcherAssert.assertThat(
                webServer.requests().get(0).getHeader(CohereUtils.REQUEST_SOURCE_HEADER),
                equalTo(CohereUtils.ELASTIC_REQUEST_SOURCE)
            );

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            MatcherAssert.assertThat(
                requestMap,
                is(
                    Map.of(
                        "texts",
                        List.of("abc"),
                        "model",
                        "model",
                        "input_type",
                        "search_document",
                        "embedding_types",
                        List.of("float"),
                        "truncate",
                        "start"
                    )
                )
            );
        }
    }

    public void testExecute_ReturnsSuccessfulResponse_ForInt8ResponseType() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = HttpRequestSenderTests.createSenderWithSingleRequestManager(senderFactory, "test_service")) {
            sender.start();

            String responseJson = """
                {
                    "id": "de37399c-5df6-47cb-bc57-e3c5680c977b",
                    "texts": [
                        "hello"
                    ],
                    "embeddings": {
                        "int8": [
                            [
                                0,
                                -1
                            ]
                        ]
                    },
                    "meta": {
                        "api_version": {
                            "version": "1"
                        },
                        "billed_units": {
                            "input_tokens": 1
                        }
                    },
                    "response_type": "embeddings_by_type"
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var action = createAction(
                getUrl(webServer),
                "secret",
                new CohereEmbeddingsTaskSettings(InputType.INGEST, CohereTruncation.START),
                "model",
                CohereEmbeddingType.INT8,
                sender
            );

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new DocumentsOnlyInput(List.of("abc")), listener);

            var result = listener.actionGet(TIMEOUT);

            MatcherAssert.assertThat(
                result.asMap(),
                is(TextEmbeddingByteResultsTests.buildExpectation(List.of(List.of((byte) 0, (byte) -1))))
            );
            MatcherAssert.assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().get(0).getUri().getQuery());
            MatcherAssert.assertThat(
                webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE),
                equalTo(XContentType.JSON.mediaType())
            );
            MatcherAssert.assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));
            MatcherAssert.assertThat(
                webServer.requests().get(0).getHeader(CohereUtils.REQUEST_SOURCE_HEADER),
                equalTo(CohereUtils.ELASTIC_REQUEST_SOURCE)
            );

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            MatcherAssert.assertThat(
                requestMap,
                is(
                    Map.of(
                        "texts",
                        List.of("abc"),
                        "model",
                        "model",
                        "input_type",
                        "search_document",
                        "embedding_types",
                        List.of("int8"),
                        "truncate",
                        "start"
                    )
                )
            );
        }
    }

    public void testExecute_ThrowsURISyntaxException_ForInvalidUrl() throws IOException {
        try (var sender = mock(Sender.class)) {
            var thrownException = expectThrows(
                IllegalArgumentException.class,
                () -> createAction("^^", "secret", CohereEmbeddingsTaskSettings.EMPTY_SETTINGS, null, null, sender)
            );
            MatcherAssert.assertThat(thrownException.getMessage(), is("unable to parse url [^^]"));
        }
    }

    public void testExecute_ThrowsElasticsearchException() {
        var sender = mock(Sender.class);
        doThrow(new ElasticsearchException("failed")).when(sender).send(any(), any(), any());

        var action = createAction(getUrl(webServer), "secret", CohereEmbeddingsTaskSettings.EMPTY_SETTINGS, null, null, sender);

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(new DocumentsOnlyInput(List.of("abc")), listener);

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));

        MatcherAssert.assertThat(thrownException.getMessage(), is("failed"));
    }

    public void testExecute_ThrowsElasticsearchException_WhenSenderOnFailureIsCalled() {
        var sender = mock(Sender.class);

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<HttpResult> listener = (ActionListener<HttpResult>) invocation.getArguments()[1];
            listener.onFailure(new IllegalStateException("failed"));

            return Void.TYPE;
        }).when(sender).send(any(), any(), any());

        var action = createAction(getUrl(webServer), "secret", CohereEmbeddingsTaskSettings.EMPTY_SETTINGS, null, null, sender);

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(new DocumentsOnlyInput(List.of("abc")), listener);

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            is(format("Failed to send Cohere embeddings request to [%s]", getUrl(webServer)))
        );
    }

    public void testExecute_ThrowsElasticsearchException_WhenSenderOnFailureIsCalled_WhenUrlIsNull() {
        var sender = mock(Sender.class);

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<HttpResult> listener = (ActionListener<HttpResult>) invocation.getArguments()[1];
            listener.onFailure(new IllegalStateException("failed"));

            return Void.TYPE;
        }).when(sender).send(any(), any(), any());

        var action = createAction(null, "secret", CohereEmbeddingsTaskSettings.EMPTY_SETTINGS, null, null, sender);

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(new DocumentsOnlyInput(List.of("abc")), listener);

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));

        MatcherAssert.assertThat(thrownException.getMessage(), is("Failed to send Cohere embeddings request"));
    }

    public void testExecute_ThrowsException() {
        var sender = mock(Sender.class);
        doThrow(new IllegalArgumentException("failed")).when(sender).send(any(), any(), any());

        var action = createAction(getUrl(webServer), "secret", CohereEmbeddingsTaskSettings.EMPTY_SETTINGS, null, null, sender);

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(new DocumentsOnlyInput(List.of("abc")), listener);

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            is(format("Failed to send Cohere embeddings request to [%s]", getUrl(webServer)))
        );
    }

    public void testExecute_ThrowsExceptionWithNullUrl() {
        var sender = mock(Sender.class);
        doThrow(new IllegalArgumentException("failed")).when(sender).send(any(), any(), any());

        var action = createAction(null, "secret", CohereEmbeddingsTaskSettings.EMPTY_SETTINGS, null, null, sender);

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(new DocumentsOnlyInput(List.of("abc")), listener);

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));

        MatcherAssert.assertThat(thrownException.getMessage(), is("Failed to send Cohere embeddings request"));
    }

    private CohereEmbeddingsAction createAction(
        String url,
        String apiKey,
        CohereEmbeddingsTaskSettings taskSettings,
        @Nullable String modelName,
        @Nullable CohereEmbeddingType embeddingType,
        Sender sender
    ) {
        var model = CohereEmbeddingsModelTests.createModel(url, apiKey, taskSettings, 1024, 1024, modelName, embeddingType);

        return new CohereEmbeddingsAction(sender, model);
    }

}
