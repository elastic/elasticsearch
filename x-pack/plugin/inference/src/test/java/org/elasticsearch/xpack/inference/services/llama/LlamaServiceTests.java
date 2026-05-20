/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.llama;

import org.apache.http.HttpHeaders;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.EmptySecretSettings;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.inference.completion.ContentString;
import org.elasticsearch.inference.completion.Message;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceEmbedding;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.UnifiedChatCompletionException;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.services.InferenceEventsAssertion;
import org.elasticsearch.xpack.inference.services.InferenceServiceTestCase;
import org.elasticsearch.xpack.inference.services.llama.completion.LlamaChatCompletionModelTests;
import org.elasticsearch.xpack.inference.services.llama.embeddings.LlamaEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.llama.embeddings.LlamaEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.llama.embeddings.LlamaEmbeddingsServiceSettings;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.ExceptionsHelper.unwrapCause;
import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.llama.completion.LlamaChatCompletionModelTests.createChatCompletionModel;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.isA;

public class LlamaServiceTests extends InferenceServiceTestCase {
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);

    public void testUnifiedCompletionInfer() throws Exception {
        // The escapes are because the streaming response must be on a single line
        String responseJson = """
            data: {\
                "id": "chatcmpl-8425dd3d-78f3-4143-93cb-dd576ab8ae26",\
                "choices": [{\
                        "delta": {\
                            "content": "Deep",\
                            "function_call": null,\
                            "refusal": null,\
                            "role": "assistant",\
                            "tool_calls": null\
                        },\
                        "finish_reason": null,\
                        "index": 0,\
                        "logprobs": null\
                    }\
                ],\
                "created": 1750158492,\
                "model": "llama3.2:3b",\
                "object": "chat.completion.chunk",\
                "service_tier": null,\
                "system_fingerprint": "fp_ollama",\
                "usage": null\
            }

            """;
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var service = new LlamaService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            var model = createChatCompletionModel("model", getUrl(webServer), "secret");
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.unifiedCompletionInfer(
                model,
                UnifiedCompletionRequest.of(List.of(new Message(new ContentString("hello"), "user", null, null))),
                null,
                listener
            );

            var result = listener.actionGet(TIMEOUT);
            InferenceEventsAssertion.assertThat(result).hasFinishedStream().hasNoErrors().hasEvent(XContentHelper.stripWhitespace("""
                {
                    "id": "chatcmpl-8425dd3d-78f3-4143-93cb-dd576ab8ae26",
                    "choices": [{
                            "delta": {
                                "content": "Deep",
                                "role": "assistant"
                            },
                            "index": 0
                        }
                    ],
                    "model": "llama3.2:3b",
                    "object": "chat.completion.chunk"
                }
                """));
        }
    }

    public void testUnifiedCompletionNonStreamingNotFoundError() throws Exception {
        String responseJson = """
            {
                "detail": "Not Found"
            }
            """;
        webServer.enqueue(new MockResponse().setResponseCode(404).setBody(responseJson));

        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var service = new LlamaService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            var model = createChatCompletionModel("model", getUrl(webServer), "secret");
            var latch = new CountDownLatch(1);
            service.unifiedCompletionInfer(
                model,
                UnifiedCompletionRequest.of(List.of(new Message(new ContentString("hello"), "user", null, null))),
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
                                "message" : "Resource not found at [%s] for request from inference entity id [id] status \
                            [404]. Error message: [{\\n    \\"detail\\": \\"Not Found\\"\\n}\\n]",
                                "type" : "llama_error"
                              }
                            }"""), getUrl(webServer))));
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    }
                }), latch::countDown)
            );
            assertTrue(latch.await(30, TimeUnit.SECONDS));
        }
    }

    public void testMidStreamUnifiedCompletionError() throws Exception {
        String responseJson = """
            data: {"error": {"message": "400: Invalid value: Model 'llama3.12:3b' not found"}}

            """;
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));
        testStreamError(XContentHelper.stripWhitespace("""
            {
                  "error": {
                      "message": "Received an error response for request from inference entity id [id].\
             Error message: [{\\"error\\": {\\"message\\": \\"400: Invalid value: Model 'llama3.12:3b' not found\\"}}]",
                      "type": "llama_error"
                  }
              }
            """));
    }

    public void testInfer_StreamRequest() throws Exception {
        String responseJson = """
            data: {\
                "id": "chatcmpl-8425dd3d-78f3-4143-93cb-dd576ab8ae26",\
                "choices": [{\
                        "delta": {\
                            "content": "Deep",\
                            "function_call": null,\
                            "refusal": null,\
                            "role": "assistant",\
                            "tool_calls": null\
                        },\
                        "finish_reason": null,\
                        "index": 0,\
                        "logprobs": null\
                    }\
                ],\
                "created": 1750158492,\
                "model": "llama3.2:3b",\
                "object": "chat.completion.chunk",\
                "service_tier": null,\
                "system_fingerprint": "fp_ollama",\
                "usage": null\
            }

            """;
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

        streamCompletion().hasNoErrors().hasEvent("""
            {"completion":[{"delta":"Deep"}]}""");
    }

    private void testStreamError(String expectedResponse) throws Exception {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var service = new LlamaService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            var model = createChatCompletionModel("model", getUrl(webServer), "secret");
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.unifiedCompletionInfer(
                model,
                UnifiedCompletionRequest.of(List.of(new Message(new ContentString("hello"), "user", null, null))),
                null,
                listener
            );

            var result = listener.actionGet(TIMEOUT);

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
            {
                "detail": "Not Found"
            }""";
        webServer.enqueue(new MockResponse().setResponseCode(404).setBody(responseJson));

        var e = assertThrows(ElasticsearchStatusException.class, this::streamCompletion);
        assertThat(e.status(), equalTo(RestStatus.NOT_FOUND));
        assertThat(e.getMessage(), equalTo(String.format(Locale.ROOT, """
            Resource not found at [%s] for request from inference entity id [id] status [404]. Error message: [{
                "detail": "Not Found"
            }]""", getUrl(webServer))));
    }

    public void testInfer_StreamRequestRetry() throws Exception {
        webServer.enqueue(new MockResponse().setResponseCode(503).setBody("""
            {
              "error": {
                "message": "server busy"
              }
            }"""));
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody("""
            data: {\
                "id": "chatcmpl-8425dd3d-78f3-4143-93cb-dd576ab8ae26",\
                "choices": [{\
                        "delta": {\
                            "content": "Deep",\
                            "function_call": null,\
                            "refusal": null,\
                            "role": "assistant",\
                            "tool_calls": null\
                        },\
                        "finish_reason": null,\
                        "index": 0,\
                        "logprobs": null\
                    }\
                ],\
                "created": 1750158492,\
                "model": "llama3.2:3b",\
                "object": "chat.completion.chunk",\
                "service_tier": null,\
                "system_fingerprint": "fp_ollama",\
                "usage": null\
            }

            """));

        streamCompletion().hasNoErrors().hasEvent("""
            {"completion":[{"delta":"Deep"}]}""");
    }

    public void testChunkedInfer_ChunkingSettingsNotSet() throws IOException {
        var model = LlamaEmbeddingsModelTests.createEmbeddingsModel("id", "url", "api_key");
        model.setURI(getUrl(webServer));

        testChunkedInfer(model);
    }

    public void testChunkedInfer_ChunkingSettingsSet() throws IOException {
        var model = LlamaEmbeddingsModelTests.createEmbeddingsModelWithChunkingSettings("id", "url", "api_key");
        model.setURI(getUrl(webServer));

        testChunkedInfer(model);
    }

    public void testChunkedInfer_noInputs() throws IOException {
        var model = LlamaEmbeddingsModelTests.createEmbeddingsModelWithChunkingSettings("id", "url", "api_key");
        model.setURI(getUrl(webServer));

        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new LlamaService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            PlainActionFuture<List<ChunkedInference>> listener = new PlainActionFuture<>();
            service.chunkedInfer(model, null, List.of(), new HashMap<>(), InputType.INTERNAL_INGEST, null, listener);

            var results = listener.actionGet(TIMEOUT);

            assertThat(results, empty());
            assertThat(webServer.requests(), empty());
        }
    }

    public void testChunkedInfer(LlamaEmbeddingsModel model) throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new LlamaService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {

            String responseJson = """
                {
                    "embeddings": [
                        [
                            0.010060793,
                            -0.0017529363
                        ],
                        [
                            0.110060793,
                            -0.1017529363
                        ]
                    ]
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            PlainActionFuture<List<ChunkedInference>> listener = new PlainActionFuture<>();
            service.chunkedInfer(
                model,
                null,
                List.of(new ChunkInferenceInput("abc"), new ChunkInferenceInput("def")),
                new HashMap<>(),
                InputType.INTERNAL_INGEST,
                null,
                listener
            );

            var results = listener.actionGet(TIMEOUT);

            assertThat(results, hasSize(2));
            {
                assertThat(results.get(0), CoreMatchers.instanceOf(ChunkedInferenceEmbedding.class));
                var floatResult = (ChunkedInferenceEmbedding) results.get(0);
                assertThat(floatResult.chunks(), hasSize(1));
                assertThat(floatResult.chunks().get(0).embedding(), instanceOf(DenseEmbeddingFloatResults.Embedding.class));
                assertTrue(
                    Arrays.equals(
                        new float[] { 0.010060793f, -0.0017529363f },
                        ((DenseEmbeddingFloatResults.Embedding) floatResult.chunks().get(0).embedding()).values()
                    )
                );
            }
            {
                assertThat(results.get(1), CoreMatchers.instanceOf(ChunkedInferenceEmbedding.class));
                var floatResult = (ChunkedInferenceEmbedding) results.get(1);
                assertThat(floatResult.chunks(), hasSize(1));
                assertThat(floatResult.chunks().get(0).embedding(), instanceOf(DenseEmbeddingFloatResults.Embedding.class));
                assertTrue(
                    Arrays.equals(
                        new float[] { 0.110060793f, -0.1017529363f },
                        ((DenseEmbeddingFloatResults.Embedding) floatResult.chunks().get(0).embedding()).values()
                    )
                );
            }

            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().get(0).getUri().getQuery());
            assertThat(
                webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE),
                equalTo(XContentType.JSON.mediaTypeWithoutParameters())
            );
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer api_key"));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.size(), Matchers.is(2));
            assertThat(requestMap.get("contents"), Matchers.is(List.of("abc", "def")));
            assertThat(requestMap.get("model_id"), Matchers.is("id"));
        }
    }

    public void testGetConfiguration() throws Exception {
        try (var service = createInferenceService()) {
            String content = XContentHelper.stripWhitespace("""
                {
                       "service": "llama",
                       "name": "Llama",
                       "task_types": ["text_embedding", "completion", "chat_completion"],
                       "configurations": {
                           "api_key": {
                               "description": "API Key for the provider you're connecting to.",
                               "label": "API Key",
                               "required": true,
                               "sensitive": true,
                               "updatable": true,
                               "type": "str",
                               "supported_task_types": ["text_embedding", "completion", "chat_completion"]
                           },
                           "model_id": {
                               "description": "Refer to the Llama models documentation for the list of available models.",
                               "label": "Model",
                               "required": true,
                               "sensitive": false,
                               "updatable": false,
                               "type": "str",
                               "supported_task_types": ["text_embedding", "completion", "chat_completion"]
                           },
                           "rate_limit.requests_per_minute": {
                               "description": "Minimize the number of rate limit errors.",
                               "label": "Rate Limit",
                               "required": false,
                               "sensitive": false,
                               "updatable": false,
                               "type": "int",
                               "supported_task_types": ["text_embedding", "completion", "chat_completion"]
                           },
                           "url": {
                               "description": "The URL endpoint to use for the requests.",
                               "label": "URL",
                               "required": true,
                               "sensitive": false,
                               "updatable": false,
                               "type": "str",
                               "supported_task_types": ["text_embedding", "completion", "chat_completion"]
                           }
                       }
                   }
                """);
            InferenceServiceConfiguration configuration = InferenceServiceConfiguration.fromXContentBytes(
                new BytesArray(content),
                XContentType.JSON
            );
            boolean humanReadable = true;
            BytesReference originalBytes = toShuffledXContent(configuration, XContentType.JSON, EMPTY_PARAMS, humanReadable);
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
        try (var service = new LlamaService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            var model = LlamaChatCompletionModelTests.createCompletionModel("model", getUrl(webServer), "secret");
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(model, null, null, null, List.of("abc"), true, new HashMap<>(), InputType.INGEST, null, listener);

            return InferenceEventsAssertion.assertThat(listener.actionGet(TIMEOUT)).hasFinishedStream();
        }
    }

    @Override
    public InferenceService createInferenceService() {
        return new LlamaService(
            HttpRequestSenderTests.createSenderFactory(threadPool, clientManager),
            createWithEmptySettings(threadPool),
            mockClusterServiceEmpty()
        );
    }

    public Model createEmbeddingModel(SimilarityMeasure similarity) {
        var settings = new LlamaEmbeddingsServiceSettings(randomAlphaOfLength(8), randomAlphaOfLength(8), null, similarity, null, null);
        return new LlamaEmbeddingsModel(
            randomAlphaOfLength(8),
            TaskType.TEXT_EMBEDDING,
            LlamaService.NAME,
            settings,
            null,
            EmptySecretSettings.INSTANCE
        );
    }
}
