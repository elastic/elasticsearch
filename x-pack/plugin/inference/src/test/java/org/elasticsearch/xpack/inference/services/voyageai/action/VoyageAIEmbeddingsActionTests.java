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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
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
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResultsTests.buildExpectationBinary;
import static org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResultsTests.buildExpectationByte;
import static org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResultsTests.buildExpectationFloat;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.services.voyageai.action.VoyageAIActionCreator.EMBEDDINGS_HANDLER;
import static org.elasticsearch.xpack.inference.services.voyageai.request.VoyageAIEmbeddingsRequestEntity.INPUT_FIELD;
import static org.elasticsearch.xpack.inference.services.voyageai.request.VoyageAIEmbeddingsRequestEntity.INPUT_TYPE_FIELD;
import static org.elasticsearch.xpack.inference.services.voyageai.request.VoyageAIEmbeddingsRequestEntity.MODEL_FIELD;
import static org.elasticsearch.xpack.inference.services.voyageai.request.VoyageAIEmbeddingsRequestEntity.OUTPUT_DIMENSION_FIELD;
import static org.elasticsearch.xpack.inference.services.voyageai.request.VoyageAIEmbeddingsRequestEntity.OUTPUT_DTYPE_FIELD;
import static org.elasticsearch.xpack.inference.services.voyageai.request.VoyageAIEmbeddingsRequestEntity.TRUNCATION_FIELD;
import static org.elasticsearch.xpack.inference.services.voyageai.request.VoyageAIEmbeddingsRequestEntity.convertInputTypeToString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class VoyageAIEmbeddingsActionTests extends ESTestCase {

    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private static final String TEST_API_KEY = "secret";
    private static final String TEST_MODEL_ID = "some-model-id";
    private static final String TEST_INPUT = "some input";
    private static final int TEST_DIMENSIONS = 1024;
    private static final int TEST_MAX_INPUT_TOKENS = 2048;
    private static final String TEST_FAILURE_MESSAGE = "failed";
    private static final String TEST_FAILED_REQUEST_MESSAGE = Strings.format(
        "Failed to send VoyageAI embeddings request. Cause: %s",
        TEST_FAILURE_MESSAGE
    );

    private final MockWebServer webServer = new MockWebServer();
    private ThreadPool threadPool;
    private HttpClientManager clientManager;

    @Before
    public void init() throws Exception {
        webServer.start();
        threadPool = createThreadPool(inferenceUtilityExecutors());
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

            var embeddingType = VoyageAIEmbeddingType.FLOAT;
            var action = createAction(getUrl(webServer), new VoyageAIEmbeddingsTaskSettings(null, true), embeddingType, sender);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            var inputType = InputTypeTests.randomSearchAndIngestWithNull();
            action.execute(new EmbeddingsInput(List.of(TEST_INPUT), inputType), null, listener);

            var result = listener.actionGet(TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationFloat(List.of(new float[] { 0.123F, -0.123F }))));
            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().getFirst().getUri().getQuery());
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.CONTENT_TYPE), is(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.AUTHORIZATION), is("Bearer " + TEST_API_KEY));
            assertThat(
                webServer.requests().getFirst().getHeader(VoyageAIUtils.REQUEST_SOURCE_HEADER),
                is(VoyageAIUtils.ELASTIC_REQUEST_SOURCE)
            );

            var requestMap = entityAsMap(webServer.requests().getFirst().getBody());
            if (inputType != null && inputType != InputType.UNSPECIFIED) {
                var convertedInputType = convertInputTypeToString(inputType);
                assertThat(
                    requestMap,
                    is(
                        Map.of(
                            INPUT_FIELD,
                            List.of(TEST_INPUT),
                            MODEL_FIELD,
                            TEST_MODEL_ID,
                            INPUT_TYPE_FIELD,
                            convertedInputType,
                            OUTPUT_DTYPE_FIELD,
                            embeddingType.toRequestString(),
                            TRUNCATION_FIELD,
                            true,
                            OUTPUT_DIMENSION_FIELD,
                            TEST_DIMENSIONS
                        )
                    )
                );
            } else {
                assertThat(
                    requestMap,
                    is(
                        Map.of(
                            INPUT_FIELD,
                            List.of(TEST_INPUT),
                            MODEL_FIELD,
                            TEST_MODEL_ID,
                            OUTPUT_DTYPE_FIELD,
                            embeddingType.toRequestString(),
                            TRUNCATION_FIELD,
                            true,
                            OUTPUT_DIMENSION_FIELD,
                            TEST_DIMENSIONS
                        )
                    )
                );
            }
        }
    }

    public void testExecute_ReturnsSuccessfulResponse_ForInt8ResponseType() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = HttpRequestSenderTests.createSender(senderFactory)) {
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

            var embeddingType = VoyageAIEmbeddingType.INT8;
            var action = createAction(getUrl(webServer), new VoyageAIEmbeddingsTaskSettings(null, true), embeddingType, sender);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            var inputType = InputTypeTests.randomSearchAndIngestWithNull();
            action.execute(new EmbeddingsInput(List.of(TEST_INPUT), inputType), null, listener);

            var result = listener.actionGet(TIMEOUT);

            assertEquals(buildExpectationByte(List.of(new byte[] { 0, -1 })), result.asMap());
            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().getFirst().getUri().getQuery());
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.CONTENT_TYPE), is(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.AUTHORIZATION), is("Bearer " + TEST_API_KEY));
            assertThat(
                webServer.requests().getFirst().getHeader(VoyageAIUtils.REQUEST_SOURCE_HEADER),
                is(VoyageAIUtils.ELASTIC_REQUEST_SOURCE)
            );

            var requestMap = entityAsMap(webServer.requests().getFirst().getBody());
            if (inputType != null && inputType != InputType.UNSPECIFIED) {
                var convertedInputType = convertInputTypeToString(inputType);
                assertThat(
                    requestMap,
                    is(
                        Map.of(
                            INPUT_FIELD,
                            List.of(TEST_INPUT),
                            MODEL_FIELD,
                            TEST_MODEL_ID,
                            INPUT_TYPE_FIELD,
                            convertedInputType,
                            OUTPUT_DTYPE_FIELD,
                            embeddingType.toRequestString(),
                            TRUNCATION_FIELD,
                            true,
                            OUTPUT_DIMENSION_FIELD,
                            TEST_DIMENSIONS
                        )
                    )
                );
            } else {
                assertThat(
                    requestMap,
                    is(
                        Map.of(
                            INPUT_FIELD,
                            List.of(TEST_INPUT),
                            MODEL_FIELD,
                            TEST_MODEL_ID,
                            OUTPUT_DTYPE_FIELD,
                            embeddingType.toRequestString(),
                            TRUNCATION_FIELD,
                            true,
                            OUTPUT_DIMENSION_FIELD,
                            TEST_DIMENSIONS
                        )
                    )
                );
            }
        }
    }

    public void testExecute_ReturnsSuccessfulResponse_ForBinaryResponseType() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = HttpRequestSenderTests.createSender(senderFactory)) {
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

            var embeddingType = VoyageAIEmbeddingType.BINARY;
            var action = createAction(getUrl(webServer), new VoyageAIEmbeddingsTaskSettings(null, true), embeddingType, sender);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            var inputType = InputTypeTests.randomSearchAndIngestWithNull();
            action.execute(new EmbeddingsInput(List.of(TEST_INPUT), inputType), null, listener);

            var result = listener.actionGet(TIMEOUT);

            assertEquals(buildExpectationBinary(List.of(new byte[] { 0, -1 })), result.asMap());
            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().getFirst().getUri().getQuery());
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.CONTENT_TYPE), is(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.AUTHORIZATION), is("Bearer " + TEST_API_KEY));
            assertThat(
                webServer.requests().getFirst().getHeader(VoyageAIUtils.REQUEST_SOURCE_HEADER),
                is(VoyageAIUtils.ELASTIC_REQUEST_SOURCE)
            );

            var requestMap = entityAsMap(webServer.requests().getFirst().getBody());
            if (inputType != null && inputType != InputType.UNSPECIFIED) {
                var convertedInputType = convertInputTypeToString(inputType);
                assertThat(
                    requestMap,
                    is(
                        Map.of(
                            INPUT_FIELD,
                            List.of(TEST_INPUT),
                            MODEL_FIELD,
                            TEST_MODEL_ID,
                            INPUT_TYPE_FIELD,
                            convertedInputType,
                            OUTPUT_DTYPE_FIELD,
                            embeddingType.toRequestString(),
                            TRUNCATION_FIELD,
                            true,
                            OUTPUT_DIMENSION_FIELD,
                            TEST_DIMENSIONS
                        )
                    )
                );
            } else {
                assertThat(
                    requestMap,
                    is(
                        Map.of(
                            INPUT_FIELD,
                            List.of(TEST_INPUT),
                            MODEL_FIELD,
                            TEST_MODEL_ID,
                            OUTPUT_DTYPE_FIELD,
                            embeddingType.toRequestString(),
                            TRUNCATION_FIELD,
                            true,
                            OUTPUT_DIMENSION_FIELD,
                            TEST_DIMENSIONS
                        )
                    )
                );
            }
        }
    }

    public void testExecute_ThrowsElasticsearchException() {
        var sender = mock(Sender.class);
        doThrow(new ElasticsearchException(TEST_FAILURE_MESSAGE)).when(sender).send(any(), any(), any(), any());

        var action = createAction(getUrl(webServer), VoyageAIEmbeddingsTaskSettings.EMPTY_SETTINGS, VoyageAIEmbeddingType.FLOAT, sender);

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(new EmbeddingsInput(List.of(TEST_INPUT), InputTypeTests.randomSearchAndIngestWithNull()), null, listener);

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(thrownException.getMessage(), is(TEST_FAILURE_MESSAGE));
    }

    public void testExecute_ThrowsElasticsearchException_WhenSenderOnFailureIsCalled() {
        var sender = mock(Sender.class);

        doAnswer(invocation -> {
            ActionListener<HttpResult> listener = invocation.getArgument(3);
            listener.onFailure(new IllegalStateException(TEST_FAILURE_MESSAGE));

            return Void.TYPE;
        }).when(sender).send(any(), any(), any(), any());

        var action = createAction(getUrl(webServer), VoyageAIEmbeddingsTaskSettings.EMPTY_SETTINGS, VoyageAIEmbeddingType.FLOAT, sender);

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(new EmbeddingsInput(List.of(TEST_INPUT), InputTypeTests.randomSearchAndIngestWithNull()), null, listener);

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(thrownException.getMessage(), is(TEST_FAILED_REQUEST_MESSAGE));
    }

    public void testExecute_ThrowsException() {
        var sender = mock(Sender.class);
        doThrow(new IllegalArgumentException(TEST_FAILURE_MESSAGE)).when(sender).send(any(), any(), any(), any());

        var action = createAction(getUrl(webServer), VoyageAIEmbeddingsTaskSettings.EMPTY_SETTINGS, VoyageAIEmbeddingType.FLOAT, sender);

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(new EmbeddingsInput(List.of(TEST_INPUT), InputTypeTests.randomSearchAndIngestWithNull()), null, listener);

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(thrownException.getMessage(), is(TEST_FAILED_REQUEST_MESSAGE));
    }

    private ExecutableAction createAction(
        String url,
        VoyageAIEmbeddingsTaskSettings taskSettings,
        VoyageAIEmbeddingType embeddingType,
        Sender sender
    ) {
        var model = VoyageAIEmbeddingsModelTests.createModel(
            url,
            TEST_API_KEY,
            taskSettings,
            TEST_MAX_INPUT_TOKENS,
            TEST_DIMENSIONS,
            TEST_MODEL_ID,
            embeddingType
        );
        var manager = new GenericRequestManager<>(
            threadPool,
            model,
            EMBEDDINGS_HANDLER,
            (embeddingsInput) -> new VoyageAIEmbeddingsRequest(embeddingsInput.getTextInputs(), embeddingsInput.getInputType(), model),
            EmbeddingsInput.class
        );

        var failedToSendRequestErrorMessage = constructFailedToSendRequestMessage("VoyageAI embeddings");
        return new SenderExecutableAction(sender, manager, failedToSendRequestErrorMessage);
    }

}
