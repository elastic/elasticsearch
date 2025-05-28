/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.action;

import org.apache.http.HttpHeaders;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.inference.InputTypeTests;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.GenericRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingType;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingsTaskSettings;
import org.elasticsearch.xpack.inference.services.voyageai.request.VoyageAIEmbeddingsRequest;
import org.elasticsearch.xpack.inference.services.voyageai.request.VoyageAIUtils;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResultsTests.buildExpectationBinary;
import static org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResultsTests.buildExpectationByte;
import static org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResultsTests.buildExpectationFloat;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.services.voyageai.action.VoyageAIActionCreator.EMBEDDINGS_HANDLER;
import static org.elasticsearch.xpack.inference.services.voyageai.request.VoyageAIEmbeddingsRequestEntity.convertToString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class VoyageAIEmbeddingsActionTests extends ESTestCase {
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

        try (var sender = HttpRequestSenderTests.createSender(senderFactory)) {
            sender.start();

            String responseJson = """
                {
                    "object": "list",
                    "data": [{
                            "object": "embedding",
                            "embedding": [
                                0.123,
                                -0.123
                            ],
                            "index": 0
                    }],
                    "model": "voyage-3-large",
                    "usage": {
                        "total_tokens": 123
                    }
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var action = createAction(
                getUrl(webServer),
                "secret",
                new VoyageAIEmbeddingsTaskSettings(null, true),
                "model",
                VoyageAIEmbeddingType.FLOAT,
                sender
            );

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            var inputType = InputTypeTests.randomSearchAndIngestWithNull();
            action.execute(
                new EmbeddingsInput(List.of(new ChunkInferenceInput("abc")), inputType),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(TIMEOUT);

            MatcherAssert.assertThat(result.asMap(), is(buildExpectationFloat(List.of(new float[] { 0.123F, -0.123F }))));
            MatcherAssert.assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().getFirst().getUri().getQuery());
            MatcherAssert.assertThat(
                webServer.requests().getFirst().getHeader(HttpHeaders.CONTENT_TYPE),
                equalTo(XContentType.JSON.mediaType())
            );
            MatcherAssert.assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));
            MatcherAssert.assertThat(
                webServer.requests().getFirst().getHeader(VoyageAIUtils.REQUEST_SOURCE_HEADER),
                equalTo(VoyageAIUtils.ELASTIC_REQUEST_SOURCE)
            );

            var requestMap = entityAsMap(webServer.requests().getFirst().getBody());
            if (inputType != null && inputType != InputType.UNSPECIFIED) {
                var convertedInputType = convertToString(inputType);
                MatcherAssert.assertThat(
                    requestMap,
                    equalTo(
                        Map.of(
                            "input",
                            List.of("abc"),
                            "model",
                            "model",
                            "input_type",
                            convertedInputType,
                            "output_dtype",
                            "float",
                            "truncation",
                            true,
                            "output_dimension",
                            1024
                        )
                    )
                );
            } else {
                MatcherAssert.assertThat(
                    requestMap,
                    equalTo(
                        Map.of(
                            "input",
                            List.of("abc"),
                            "model",
                            "model",
                            "output_dtype",
                            "float",
                            "truncation",
                            true,
                            "output_dimension",
                            1024
                        )
                    )
                );
            }
        }
    }

    public void testExecute_ReturnsSuccessfulResponse_ForInt8ResponseType() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = HttpRequestSenderTests.createSender(senderFactory)) {
            sender.start();

            String responseJson = """
                {
                    "object": "list",
                    "data": [{
                            "object": "embedding",
                            "embedding": [
                                0,
                                -1
                            ],
                            "index": 0
                    }],
                    "model": "voyage-3-large",
                    "usage": {
                        "total_tokens": 123
                    }
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var action = createAction(
                getUrl(webServer),
                "secret",
                new VoyageAIEmbeddingsTaskSettings(null, true),
                "model",
                VoyageAIEmbeddingType.INT8,
                sender
            );

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            var inputType = InputTypeTests.randomSearchAndIngestWithNull();
            action.execute(
                new EmbeddingsInput(List.of(new ChunkInferenceInput("abc")), inputType),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(TIMEOUT);

            assertEquals(buildExpectationByte(List.of(new byte[] { 0, -1 })), result.asMap());
            MatcherAssert.assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().getFirst().getUri().getQuery());
            MatcherAssert.assertThat(
                webServer.requests().getFirst().getHeader(HttpHeaders.CONTENT_TYPE),
                equalTo(XContentType.JSON.mediaType())
            );
            MatcherAssert.assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));
            MatcherAssert.assertThat(
                webServer.requests().getFirst().getHeader(VoyageAIUtils.REQUEST_SOURCE_HEADER),
                equalTo(VoyageAIUtils.ELASTIC_REQUEST_SOURCE)
            );

            var requestMap = entityAsMap(webServer.requests().getFirst().getBody());
            if (inputType != null && inputType != InputType.UNSPECIFIED) {
                var convertedInputType = convertToString(inputType);
                MatcherAssert.assertThat(
                    requestMap,
                    is(
                        Map.of(
                            "input",
                            List.of("abc"),
                            "model",
                            "model",
                            "input_type",
                            convertedInputType,
                            "output_dtype",
                            "int8",
                            "truncation",
                            true,
                            "output_dimension",
                            1024
                        )
                    )
                );
            } else {
                MatcherAssert.assertThat(
                    requestMap,
                    is(
                        Map.of(
                            "input",
                            List.of("abc"),
                            "model",
                            "model",
                            "output_dtype",
                            "int8",
                            "truncation",
                            true,
                            "output_dimension",
                            1024
                        )
                    )
                );
            }
        }
    }

    public void testExecute_ReturnsSuccessfulResponse_ForBinaryResponseType() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = HttpRequestSenderTests.createSender(senderFactory)) {
            sender.start();

            String responseJson = """
                {
                    "object": "list",
                    "data": [{
                            "object": "embedding",
                            "embedding": [
                                0,
                                -1
                            ],
                            "index": 0
                    }],
                    "model": "voyage-3-large",
                    "usage": {
                        "total_tokens": 123
                    }
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var action = createAction(
                getUrl(webServer),
                "secret",
                new VoyageAIEmbeddingsTaskSettings(null, true),
                "model",
                VoyageAIEmbeddingType.BINARY,
                sender
            );

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            var inputType = InputTypeTests.randomSearchAndIngestWithNull();
            action.execute(
                new EmbeddingsInput(List.of(new ChunkInferenceInput("abc")), inputType),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(TIMEOUT);

            assertEquals(buildExpectationBinary(List.of(new byte[] { 0, -1 })), result.asMap());
            MatcherAssert.assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().getFirst().getUri().getQuery());
            MatcherAssert.assertThat(
                webServer.requests().getFirst().getHeader(HttpHeaders.CONTENT_TYPE),
                equalTo(XContentType.JSON.mediaType())
            );
            MatcherAssert.assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));
            MatcherAssert.assertThat(
                webServer.requests().getFirst().getHeader(VoyageAIUtils.REQUEST_SOURCE_HEADER),
                equalTo(VoyageAIUtils.ELASTIC_REQUEST_SOURCE)
            );

            var requestMap = entityAsMap(webServer.requests().getFirst().getBody());
            if (inputType != null && inputType != InputType.UNSPECIFIED) {
                var convertedInputType = convertToString(inputType);
                MatcherAssert.assertThat(
                    requestMap,
                    is(
                        Map.of(
                            "input",
                            List.of("abc"),
                            "model",
                            "model",
                            "input_type",
                            convertedInputType,
                            "output_dtype",
                            "binary",
                            "truncation",
                            true,
                            "output_dimension",
                            1024
                        )
                    )
                );
            } else {
                MatcherAssert.assertThat(
                    requestMap,
                    is(
                        Map.of(
                            "input",
                            List.of("abc"),
                            "model",
                            "model",
                            "output_dtype",
                            "binary",
                            "truncation",
                            true,
                            "output_dimension",
                            1024
                        )
                    )
                );
            }
        }
    }

    public void testExecute_ThrowsElasticsearchException() {
        var sender = mock(Sender.class);
        doThrow(new ElasticsearchException("failed")).when(sender).send(any(), any(), any(), any());

        var action = createAction(getUrl(webServer), "secret", VoyageAIEmbeddingsTaskSettings.EMPTY_SETTINGS, "model", null, sender);

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(
            new EmbeddingsInput(List.of(new ChunkInferenceInput("abc")), InputTypeTests.randomSearchAndIngestWithNull()),
            InferenceAction.Request.DEFAULT_TIMEOUT,
            listener
        );

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));

        MatcherAssert.assertThat(thrownException.getMessage(), is("failed"));
    }

    public void testExecute_ThrowsElasticsearchException_WhenSenderOnFailureIsCalled() {
        var sender = mock(Sender.class);

        doAnswer(invocation -> {
            ActionListener<HttpResult> listener = invocation.getArgument(3);
            listener.onFailure(new IllegalStateException("failed"));

            return Void.TYPE;
        }).when(sender).send(any(), any(), any(), any());

        var action = createAction(getUrl(webServer), "secret", VoyageAIEmbeddingsTaskSettings.EMPTY_SETTINGS, "model", null, sender);

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(
            new EmbeddingsInput(List.of(new ChunkInferenceInput("abc")), InputTypeTests.randomSearchAndIngestWithNull()),
            InferenceAction.Request.DEFAULT_TIMEOUT,
            listener
        );

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));

        MatcherAssert.assertThat(thrownException.getMessage(), is("Failed to send VoyageAI embeddings request. Cause: failed"));
    }

    public void testExecute_ThrowsException() {
        var sender = mock(Sender.class);
        doThrow(new IllegalArgumentException("failed")).when(sender).send(any(), any(), any(), any());

        var action = createAction(getUrl(webServer), "secret", VoyageAIEmbeddingsTaskSettings.EMPTY_SETTINGS, "model", null, sender);

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(
            new EmbeddingsInput(List.of(new ChunkInferenceInput("abc")), InputTypeTests.randomSearchAndIngestWithNull()),
            InferenceAction.Request.DEFAULT_TIMEOUT,
            listener
        );

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));

        MatcherAssert.assertThat(thrownException.getMessage(), is("Failed to send VoyageAI embeddings request. Cause: failed"));
    }

    private ExecutableAction createAction(
        String url,
        String apiKey,
        VoyageAIEmbeddingsTaskSettings taskSettings,
        @Nullable String modelName,
        @Nullable VoyageAIEmbeddingType embeddingType,
        Sender sender
    ) {
        var model = VoyageAIEmbeddingsModelTests.createModel(url, apiKey, taskSettings, 1024, 1024, modelName, embeddingType);
        var manager = new GenericRequestManager<>(
            threadPool,
            model,
            EMBEDDINGS_HANDLER,
            (embeddingsInput) -> new VoyageAIEmbeddingsRequest(embeddingsInput.getStringInputs(), embeddingsInput.getInputType(), model),
            EmbeddingsInput.class
        );

        var failedToSendRequestErrorMessage = constructFailedToSendRequestMessage("VoyageAI embeddings");
        return new SenderExecutableAction(sender, manager, failedToSendRequestErrorMessage);
    }

}
