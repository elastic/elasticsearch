/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia;

import org.apache.http.HttpHeaders;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.RerankingInferenceService;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.inference.completion.ContentString;
import org.elasticsearch.inference.completion.Message;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceEmbedding;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.UnifiedChatCompletionException;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.services.InferenceEventsAssertion;
import org.elasticsearch.xpack.inference.services.InferenceServiceTestCase;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.nvidia.completion.NvidiaChatCompletionModelTests;
import org.elasticsearch.xpack.inference.services.nvidia.embeddings.NvidiaEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.nvidia.embeddings.NvidiaEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.nvidia.embeddings.NvidiaEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.nvidia.embeddings.NvidiaEmbeddingsTaskSettings;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.ExceptionsHelper.unwrapCause;
import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.inference.TaskType.CHAT_COMPLETION;
import static org.elasticsearch.inference.TaskType.TEXT_EMBEDDING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsTests.createRandomChunkingSettings;
import static org.elasticsearch.xpack.inference.Utils.getRequestConfigMap;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.nvidia.NvidiaServiceParameterizedTestConfiguration.API_KEY_VALUE;
import static org.elasticsearch.xpack.inference.services.nvidia.NvidiaServiceParameterizedTestConfiguration.DIMENSIONS_VALUE;
import static org.elasticsearch.xpack.inference.services.nvidia.NvidiaServiceParameterizedTestConfiguration.MODEL_VALUE;
import static org.elasticsearch.xpack.inference.services.nvidia.NvidiaServiceParameterizedTestConfiguration.URL_VALUE;
import static org.elasticsearch.xpack.inference.services.nvidia.completion.NvidiaChatCompletionModelTests.createChatCompletionModel;
import static org.elasticsearch.xpack.inference.services.nvidia.request.NvidiaRequestFields.INPUT_FIELD_NAME;
import static org.elasticsearch.xpack.inference.services.nvidia.request.NvidiaRequestFields.INPUT_TYPE_FIELD_NAME;
import static org.elasticsearch.xpack.inference.services.nvidia.request.NvidiaRequestFields.MODEL_FIELD_NAME;
import static org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettingsTests.getSecretSettingsMap;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.nullValue;

public class NvidiaServiceTests extends InferenceServiceTestCase {

    private static final String INPUT_TYPE_NVIDIA_DEFAULT_VALUE = "query";
    private static final String CONTENT_VALUE = "hello";
    private static final String SECOND_PART_OF_INPUT_VALUE = "def";
    private static final String FIRST_PART_OF_INPUT_VALUE = "abc";
    private static final String ROLE_VALUE = "user";
    private static final String INFERENCE_ID_VALUE = "id";

    public void testParseRequestConfig_NoModelId_ThrowsException() throws IOException {
        try (var service = createInferenceService()) {
            ActionListener<Model> modelVerificationListener = ActionListener.wrap(
                model -> fail("Expected exception, but got model: " + model),
                exception -> {
                    assertThat(exception, instanceOf(ValidationException.class));
                    assertThat(
                        exception.getMessage(),
                        is("Validation Failed: 1: [service_settings] does not contain the required setting [model_id];")
                    );
                }
            );

            service.parseRequestConfig(
                INFERENCE_ID_VALUE,
                CHAT_COMPLETION,
                getRequestConfigMap(
                    new HashMap<>(Map.of(ServiceFields.URL, URL_VALUE)),
                    new HashMap<>(),
                    getSecretSettingsMap(API_KEY_VALUE)
                ),
                modelVerificationListener
            );
        }
    }

    public void testUnifiedCompletionInfer() throws Exception {
        // The escapes are because the streaming response must be on a single line
        String responseJson = """
            data: {\
                "id": "chatcmpl-8425dd3d-78f3-4143-93cb-dd576ab8ae26",\
                "object": "chat.completion.chunk",\
                "created": 1750158492,\
                "model": "microsoft/phi-3-mini-128k-instruct",\
                "choices": [{\
                        "index": 0,\
                        "delta": {\
                            "content": "Deep"\
                        },\
                        "finish_reason": null,\
                        "logprobs": null\
                    }\
                ]\
            }

            """;
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var service = new NvidiaService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            var model = createChatCompletionModel(getUrl(webServer), API_KEY_VALUE, MODEL_VALUE);
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.unifiedCompletionInfer(
                model,
                UnifiedCompletionRequest.of(List.of(new Message(new ContentString(CONTENT_VALUE), ROLE_VALUE, null, null))),
                null,
                listener
            );

            var result = listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);
            InferenceEventsAssertion.assertThat(result).hasFinishedStream().hasNoErrors().hasEvent(XContentHelper.stripWhitespace("""
                {
                    "id": "chatcmpl-8425dd3d-78f3-4143-93cb-dd576ab8ae26",
                    "choices": [{
                            "delta": {
                                "content": "Deep"
                            },
                            "index": 0
                        }
                    ],
                    "model": "microsoft/phi-3-mini-128k-instruct",
                    "object": "chat.completion.chunk"
                }
                """));
        }
    }

    public void testUnifiedCompletionNonStreamingNotFoundError() throws Exception {
        String response = """
            404 page not found
            """;
        webServer.enqueue(new MockResponse().setResponseCode(404).setBody(response));

        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var service = new NvidiaService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            var model = createChatCompletionModel(getUrl(webServer), API_KEY_VALUE, MODEL_VALUE);
            var latch = new CountDownLatch(1);
            service.unifiedCompletionInfer(
                model,
                UnifiedCompletionRequest.of(List.of(new Message(new ContentString(CONTENT_VALUE), ROLE_VALUE, null, null))),
                null,
                ActionListener.runAfter(ActionTestUtils.assertNoSuccessListener(e -> {
                    try (var builder = XContentFactory.jsonBuilder()) {
                        var t = unwrapCause(e);
                        assertThat(t, isA(UnifiedChatCompletionException.class));
                        ((UnifiedChatCompletionException) t).toXContentChunked(EMPTY_PARAMS).forEachRemaining(xContent -> {
                            try {
                                xContent.toXContent(builder, EMPTY_PARAMS);
                            } catch (IOException ex) {
                                throw new RuntimeException(ex);
                            }
                        });
                        var json = XContentHelper.convertToJson(BytesReference.bytes(builder), false, builder.contentType());
                        assertThat(json, is(String.format(Locale.ROOT, XContentHelper.stripWhitespace("""
                            {
                              "error" : {
                                "code" : "not_found",
                                "message" : "Resource not found at [%s] for request from inference entity id [inferenceEntityId] status \
                            [404]. Error message: [404 page not found\\n]",
                                "type" : "nvidia_error"
                              }
                            }"""), getUrl(webServer))));
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    }
                }), latch::countDown)
            );
            assertThat(latch.await(30, TimeUnit.SECONDS), is(true));
        }
    }

    public void testMidStreamUnifiedCompletionError() throws Exception {
        String responseJson = """
            data: {"error": {"message": "midstream error"}}

            """;
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));
        testStreamError(XContentHelper.stripWhitespace("""
            {
                  "error": {
                      "message": "Received an error response for request from inference entity id [inferenceEntityId].\
             Error message: [{\\"error\\": {\\"message\\": \\"midstream error\\"}}]",
                      "type": "nvidia_error"
                  }
              }
            """));
    }

    public void testInfer_StreamRequest() throws Exception {
        String responseJson = """
            data: {\
                "id": "chatcmpl-2c57e3888b1a4e80a0c708889546288e",\
                "object": "chat.completion.chunk",\
                "created": 1760082951,\
                "model": "microsoft/phi-3-mini-128k-instruct",\
                "choices": [{\
                        "index": 0,\
                        "delta": {\
                            "content": "Deep"\
                        },\
                        "logprobs": null,\
                        "finish_reason": null\
                    }\
                ]\
            }

            """;
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

        streamCompletion().hasNoErrors().hasEvent("""
            {"completion":[{"delta":"Deep"}]}""");
    }

    private void testStreamError(String expectedResponse) throws Exception {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var service = new NvidiaService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            var model = createChatCompletionModel(getUrl(webServer), API_KEY_VALUE, MODEL_VALUE);
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.unifiedCompletionInfer(
                model,
                UnifiedCompletionRequest.of(List.of(new Message(new ContentString(CONTENT_VALUE), ROLE_VALUE, null, null))),
                null,
                listener
            );

            var result = listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);

            InferenceEventsAssertion.assertThat(result).hasFinishedStream().hasNoEvents().hasErrorMatching(e -> {
                e = unwrapCause(e);
                assertThat(e, isA(UnifiedChatCompletionException.class));
                try (var builder = XContentFactory.jsonBuilder()) {
                    ((UnifiedChatCompletionException) e).toXContentChunked(EMPTY_PARAMS).forEachRemaining(xContent -> {
                        try {
                            xContent.toXContent(builder, EMPTY_PARAMS);
                        } catch (IOException ex) {
                            throw new RuntimeException(ex);
                        }
                    });
                    var json = XContentHelper.convertToJson(BytesReference.bytes(builder), false, builder.contentType());

                    assertThat(json, is(expectedResponse));
                }
            });
        }
    }

    public void testInfer_StreamRequest_ErrorResponse() {
        String responseJson = """
            404 page not found""";
        webServer.enqueue(new MockResponse().setResponseCode(404).setBody(responseJson));

        var e = assertThrows(ElasticsearchStatusException.class, this::streamCompletion);
        assertThat(e.status(), is(RestStatus.NOT_FOUND));
        assertThat(e.getMessage(), is(Strings.format("""
            Resource not found at [%s] for request from inference entity id [inferenceEntityId] status [404]. \
            Error message: [404 page not found]""", getUrl(webServer))));
    }

    public void testInfer_StreamRequestRetry() throws Exception {
        webServer.enqueue(new MockResponse().setResponseCode(503).setBody("""
            failed to decode json body: json: bool unexpected end of JSON input"""));
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody("""
            data: {\
                "id": "chatcmpl-2c57e3888b1a4e80a0c708889546288e",\
                "object": "chat.completion.chunk",\
                "created": 1760082951,\
                "model": "microsoft/phi-3-mini-128k-instruct",\
                "choices": [{\
                        "index": 0,\
                        "delta": {\
                            "content": "Deep"\
                        },\
                        "logprobs": null,\
                        "finish_reason": null\
                    }\
                ]\
            }

            """));

        streamCompletion().hasNoErrors().hasEvent("""
            {"completion":[{"delta":"Deep"}]}""");
    }

    public void testChunkedInfer_ChunkingSettingsNotSet() throws IOException {
        var model = NvidiaEmbeddingsModelTests.createEmbeddingsModel(
            getUrl(webServer),
            API_KEY_VALUE,
            MODEL_VALUE,
            1234,
            DIMENSIONS_VALUE,
            null,
            null,
            null
        );

        testChunkedInfer(model);
    }

    public void testChunkedInfer_ChunkingSettingsSet() throws IOException {
        var model = NvidiaEmbeddingsModelTests.createEmbeddingsModel(
            getUrl(webServer),
            API_KEY_VALUE,
            MODEL_VALUE,
            1234,
            DIMENSIONS_VALUE,
            null,
            null,
            createRandomChunkingSettings()
        );

        testChunkedInfer(model);
    }

    public void testChunkedInfer(NvidiaEmbeddingsModel model) throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new NvidiaService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {

            String responseJson = """
                {
                    "object": "list",
                    "data": [
                        {
                            "index": 0,
                            "object": "embedding",
                            "embedding": [
                                0.0089111328125,
                                -0.007049560546875
                            ]
                        },
                        {
                            "index": 1,
                            "object": "embedding",
                            "embedding": [
                                -0.008544921875,
                                -0.0230712890625
                            ]
                        }
                    ],
                    "model": "baai/bge-m3",
                    "usage": {
                        "num_images": 0,
                        "prompt_tokens": 7,
                        "total_tokens": 7
                    }
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            PlainActionFuture<List<ChunkedInference>> listener = new PlainActionFuture<>();
            service.chunkedInfer(
                model,
                null,
                List.of(new ChunkInferenceInput(FIRST_PART_OF_INPUT_VALUE), new ChunkInferenceInput(SECOND_PART_OF_INPUT_VALUE)),
                new HashMap<>(),
                null,
                null,
                listener
            );

            var results = listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);

            assertThat(results, hasSize(2));
            {
                assertThat(results.getFirst(), instanceOf(ChunkedInferenceEmbedding.class));
                var floatResult = (ChunkedInferenceEmbedding) results.getFirst();
                assertThat(floatResult.chunks(), hasSize(1));
                assertThat(floatResult.chunks().getFirst().embedding(), instanceOf(DenseEmbeddingFloatResults.Embedding.class));
                assertThat(
                    Arrays.equals(
                        new float[] { 0.0089111328125f, -0.007049560546875f },
                        ((DenseEmbeddingFloatResults.Embedding) floatResult.chunks().getFirst().embedding()).values()
                    ),
                    is(true)
                );
            }
            {
                assertThat(results.get(1), instanceOf(ChunkedInferenceEmbedding.class));
                var floatResult = (ChunkedInferenceEmbedding) results.get(1);
                assertThat(floatResult.chunks(), hasSize(1));
                assertThat(floatResult.chunks().getFirst().embedding(), instanceOf(DenseEmbeddingFloatResults.Embedding.class));
                assertThat(
                    Arrays.equals(
                        new float[] { -0.008544921875f, -0.0230712890625f },
                        ((DenseEmbeddingFloatResults.Embedding) floatResult.chunks().getFirst().embedding()).values()
                    ),
                    is(true)
                );
            }

            assertThat(webServer.requests(), hasSize(1));
            assertThat(webServer.requests().getFirst().getUri().getQuery(), is(nullValue()));
            assertThat(
                webServer.requests().getFirst().getHeader(HttpHeaders.CONTENT_TYPE),
                is(XContentType.JSON.mediaTypeWithoutParameters())
            );
            assertThat(
                webServer.requests().getFirst().getHeader(HttpHeaders.AUTHORIZATION),
                is(Strings.format("Bearer %s", API_KEY_VALUE))
            );

            var requestMap = entityAsMap(webServer.requests().getFirst().getBody());
            assertThat(requestMap.size(), is(3));
            assertThat(requestMap.get(INPUT_FIELD_NAME), is(List.of(FIRST_PART_OF_INPUT_VALUE, SECOND_PART_OF_INPUT_VALUE)));
            assertThat(requestMap.get(MODEL_FIELD_NAME), is(MODEL_VALUE));
            assertThat(requestMap.get(INPUT_TYPE_FIELD_NAME), is(INPUT_TYPE_NVIDIA_DEFAULT_VALUE));
        }
    }

    public void testGetConfiguration() throws Exception {
        try (var service = createInferenceService()) {
            String content = XContentHelper.stripWhitespace("""
                {
                       "service": "nvidia",
                       "name": "NVIDIA",
                       "task_types": ["text_embedding", "rerank", "completion", "chat_completion"],
                       "configurations": {
                           "api_key": {
                               "description": "API Key for the provider you're connecting to.",
                               "label": "API Key",
                               "required": true,
                               "sensitive": true,
                               "updatable": true,
                               "type": "str",
                               "supported_task_types": ["text_embedding", "rerank", "completion", "chat_completion"]
                           },
                           "model_id": {
                               "description": "The name of the model to use for the inference task. Refer to the NVIDIA models \
                documentation for the list of available models.",
                               "label": "Model ID",
                               "required": true,
                               "sensitive": false,
                               "updatable": false,
                               "type": "str",
                               "supported_task_types": ["text_embedding", "rerank", "completion", "chat_completion"]
                           },
                           "rate_limit.requests_per_minute": {
                               "description": "Minimize the number of rate limit errors.",
                               "label": "Rate Limit",
                               "required": false,
                               "sensitive": false,
                               "updatable": false,
                               "type": "int",
                               "supported_task_types": ["text_embedding", "rerank", "completion", "chat_completion"]
                           },
                           "url": {
                               "description": "The URL endpoint to use for the requests.",
                               "label": "URL",
                               "required": false,
                               "sensitive": false,
                               "updatable": false,
                               "type": "str",
                               "supported_task_types": ["text_embedding", "rerank", "completion", "chat_completion"]
                           }
                       }
                   }
                """);
            InferenceServiceConfiguration configuration = InferenceServiceConfiguration.fromXContentBytes(
                new BytesArray(content),
                XContentType.JSON
            );
            boolean humanReadable = true;
            BytesReference originalBytes = toShuffledXContent(configuration, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
            InferenceServiceConfiguration serviceConfiguration = service.getConfiguration();
            assertToXContentEquivalent(
                originalBytes,
                toXContent(serviceConfiguration, XContentType.JSON, humanReadable),
                XContentType.JSON
            );
        }
    }

    private InferenceEventsAssertion streamCompletion() throws Exception {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var service = new NvidiaService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            var model = NvidiaChatCompletionModelTests.createCompletionModel(getUrl(webServer), API_KEY_VALUE, MODEL_VALUE);
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                model,
                null,
                null,
                null,
                List.of(FIRST_PART_OF_INPUT_VALUE),
                true,
                new HashMap<>(),
                InputType.INGEST,
                null,
                listener
            );

            return InferenceEventsAssertion.assertThat(listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT)).hasFinishedStream();
        }
    }

    @Override
    public InferenceService createInferenceService() {
        return new NvidiaService(
            HttpRequestSenderTests.createSenderFactory(threadPool, clientManager),
            createWithEmptySettings(threadPool),
            mockClusterServiceEmpty()
        );
    }

    @Override
    public Model createEmbeddingModel(SimilarityMeasure similarity) {
        var settings = new NvidiaEmbeddingsServiceSettings(
            randomAlphaOfLength(8),
            randomAlphaOfLength(8),
            randomInt(),
            similarity,
            null,
            null
        );
        return new NvidiaEmbeddingsModel(
            randomAlphaOfLength(8),
            TEXT_EMBEDDING,
            NvidiaService.NAME,
            settings,
            NvidiaEmbeddingsTaskSettings.EMPTY_SETTINGS,
            null,
            null
        );
    }

    @Override
    protected void assertRerankerWindowSize(RerankingInferenceService rerankingInferenceService) {
        assertThat(rerankingInferenceService.rerankerWindowSize("any model"), is(300));
    }
}
