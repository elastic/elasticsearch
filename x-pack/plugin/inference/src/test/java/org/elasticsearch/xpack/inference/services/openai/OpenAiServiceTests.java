/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * this file was contributed to by a generative AI
 */

package org.elasticsearch.xpack.inference.services.openai;

import org.apache.http.HttpHeaders;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.EmbeddingRequest;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.InferenceStringGroup;
import org.elasticsearch.inference.InferenceStringTests;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.inference.completion.ContentString;
import org.elasticsearch.inference.completion.Message;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsTests;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceEmbedding;
import org.elasticsearch.xpack.core.inference.results.EmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.GenericDenseEmbeddingFloatResultsTests;
import org.elasticsearch.xpack.core.inference.results.UnifiedChatCompletionException;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.services.InferenceEventsAssertion;
import org.elasticsearch.xpack.inference.services.InferenceServiceTestCase;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionModelTests;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsModelTests;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.ExceptionsHelper.unwrapCause;
import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResultsTests.buildExpectationFloat;
import static org.elasticsearch.xpack.inference.Utils.getInvalidModel;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.services.SenderServiceTests.createMockSender;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.openai.OpenAiUtils.ORGANIZATION_HEADER;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class OpenAiServiceTests extends InferenceServiceTestCase {

    public void testInfer_ThrowsErrorWhenInputTypeIsSpecified() throws IOException {
        var sender = createMockSender();

        var factory = mock(HttpRequestSender.Factory.class);
        when(factory.createSender()).thenReturn(sender);

        var model = OpenAiEmbeddingsModelTests.createModel(getUrl(webServer), "org", "secret", "model", "user", TaskType.TEXT_EMBEDDING);

        try (var service = new OpenAiService(factory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(model, null, null, null, List.of(""), false, new HashMap<>(), InputType.INGEST, null, listener);

            var thrownException = expectThrows(ValidationException.class, () -> listener.actionGet(TEST_REQUEST_TIMEOUT));

            assertThat(
                thrownException.getMessage(),
                is("Validation Failed: 1: Invalid input_type [ingest]. The input_type option is not supported by this service;")
            );

            verify(factory, times(1)).createSender();
        }

        verify(sender, times(1)).close();
        verifyNoMoreInteractions(factory);
        verifyNoMoreInteractions(sender);
    }

    public void testInfer_ThrowsErrorWhenTaskTypeIsNotValid() throws IOException {
        var sender = createMockSender();

        var factory = mock(HttpRequestSender.Factory.class);
        when(factory.createSender()).thenReturn(sender);

        var mockModel = getInvalidModel("model_id", "service_name", TaskType.SPARSE_EMBEDDING);

        try (var service = new OpenAiService(factory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(mockModel, null, null, null, List.of(""), false, new HashMap<>(), InputType.INTERNAL_INGEST, null, listener);

            var thrownException = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TEST_REQUEST_TIMEOUT));
            assertThat(
                thrownException.getMessage(),
                is(
                    "Inference entity [model_id] does not support task type [sparse_embedding] "
                        + "for inference, the task type must be one of [text_embedding, completion]."
                )
            );

            verify(factory, times(1)).createSender();
        }

        verify(sender, times(1)).close();
        verifyNoMoreInteractions(factory);
        verifyNoMoreInteractions(sender);
    }

    public void testInfer_ThrowsErrorWhenTaskTypeIsNotValid_ChatCompletion() throws IOException {
        var sender = createMockSender();

        var factory = mock(HttpRequestSender.Factory.class);
        when(factory.createSender()).thenReturn(sender);

        var mockModel = getInvalidModel("model_id", "service_name", TaskType.CHAT_COMPLETION);

        try (var service = new OpenAiService(factory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(mockModel, null, null, null, List.of(""), false, new HashMap<>(), InputType.INGEST, null, listener);

            var thrownException = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TEST_REQUEST_TIMEOUT));
            assertThat(
                thrownException.getMessage(),
                is(
                    "Inference entity [model_id] does not support task type [chat_completion] "
                        + "for inference, the task type must be one of [text_embedding, completion]. "
                        + "The task type for the inference entity is chat_completion, "
                        + "please use the _inference/chat_completion/model_id/_stream URL."
                )
            );

            verify(factory, times(1)).createSender();
        }

        verify(sender, times(1)).close();
        verifyNoMoreInteractions(factory);
        verifyNoMoreInteractions(sender);
    }

    public void testInfer_SendsRequest() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new OpenAiService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {

            String responseJson = """
                {
                  "object": "list",
                  "data": [
                      {
                          "object": "embedding",
                          "index": 0,
                          "embedding": [
                              0.0123,
                              -0.0123
                          ]
                      }
                  ],
                  "model": "text-embedding-ada-002-v2",
                  "usage": {
                      "prompt_tokens": 8,
                      "total_tokens": 8
                  }
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = OpenAiEmbeddingsModelTests.createModel(
                getUrl(webServer),
                "org",
                "secret",
                "model",
                "user",
                TaskType.TEXT_EMBEDDING
            );
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(model, null, null, null, List.of("abc"), false, new HashMap<>(), InputType.INTERNAL_INGEST, null, listener);

            var result = listener.actionGet(TEST_REQUEST_TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationFloat(List.of(new float[] { 0.0123F, -0.0123F }))));
            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().getFirst().getUri().getQuery());
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));
            assertThat(webServer.requests().getFirst().getHeader(ORGANIZATION_HEADER), equalTo("org"));

            var requestMap = entityAsMap(webServer.requests().getFirst().getBody());
            assertThat(requestMap.size(), is(3));
            assertThat(requestMap.get("input"), is(List.of("abc")));
            assertThat(requestMap.get("model"), is("model"));
            assertThat(requestMap.get("user"), is("user"));
        }
    }

    public void testInfer_ReturnsErrorWhenCallingInfer_WithChatCompletion() throws IOException {
        var sender = mock(HttpRequestSender.class);

        var s = mock(HttpRequestSender.Factory.class);
        when(s.createSender()).thenReturn(sender);

        try (var service = new OpenAiService(s, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {

            var endpointId = "endpoint_id";
            var model = OpenAiChatCompletionModelTests.createModelWithTaskType(
                endpointId,
                getUrl(webServer),
                "org",
                "secret",
                "model",
                "user",
                TaskType.CHAT_COMPLETION
            );

            var listener = new PlainActionFuture<InferenceServiceResults>();
            service.infer(model, null, null, null, List.of("abc"), false, new HashMap<>(), InputType.INTERNAL_INGEST, null, listener);

            var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TEST_REQUEST_TIMEOUT));
            assertThat(
                exception.getMessage(),
                containsString(
                    Strings.format(
                        "The task type for the inference entity is chat_completion, "
                            + "please use the _inference/chat_completion/%s/_stream URL",
                        endpointId
                    )
                )
            );
        }
    }

    public void testUnifiedCompletionInfer() throws Exception {
        String responseJson = Strings.format("""
            data: %s

            """, XContentHelper.stripWhitespace("""
            {
                "id": "12345",
                "object": "chat.completion.chunk",
                "created": 123456789,
                "model": "gpt-4o-mini",
                "system_fingerprint": "123456789",
                "choices": [{
                        "index": 0,
                        "delta": {
                            "content": "hello, world"
                        },
                        "logprobs": null,
                        "finish_reason": "stop"
                    }
                ],
                "usage": {
                    "prompt_tokens": 16,
                    "completion_tokens": 28,
                    "total_tokens": 44,
                    "prompt_tokens_details": {
                        "cached_tokens": 0,
                        "audio_tokens": 0
                    },
                    "completion_tokens_details": {
                        "reasoning_tokens": 0,
                        "audio_tokens": 0,
                        "accepted_prediction_tokens": 0,
                        "rejected_prediction_tokens": 0
                    }
                }
            }
            """));
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var service = new OpenAiService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            var model = OpenAiChatCompletionModelTests.createChatCompletionModel(getUrl(webServer), "org", "secret", "model", "user");
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.unifiedCompletionInfer(
                model,
                UnifiedCompletionRequest.of(List.of(new Message(new ContentString("hello"), "user", null, null))),
                null,
                listener
            );

            var result = listener.actionGet(TEST_REQUEST_TIMEOUT);
            InferenceEventsAssertion.assertThat(result).hasFinishedStream().hasNoErrors().hasEvent(XContentHelper.stripWhitespace("""
                {
                    "id": "12345",
                    "choices": [{
                            "delta": {
                                "content": "hello, world"
                            },
                            "finish_reason": "stop",
                            "index": 0
                        }
                    ],
                    "model": "gpt-4o-mini",
                    "object": "chat.completion.chunk",
                    "usage": {
                        "completion_tokens": 28,
                        "prompt_tokens": 16,
                        "total_tokens": 44,
                        "prompt_tokens_details": {
                            "cached_tokens": 0
                        },
                        "completion_tokens_details": {
                            "reasoning_tokens": 0
                        }
                    }
                }
                """));
        }
    }

    public void testUnifiedCompletionError() throws Exception {
        String responseJson = """
            {
                "error": {
                    "message": "The model `gpt-4awero` does not exist or you do not have access to it.",
                    "type": "invalid_request_error",
                    "param": null,
                    "code": "model_not_found"
                }
            }""";
        webServer.enqueue(new MockResponse().setResponseCode(404).setBody(responseJson));

        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var service = new OpenAiService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            var model = OpenAiChatCompletionModelTests.createChatCompletionModel(getUrl(webServer), "org", "secret", "model", "user");
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

                        assertThat(json, is(String.format(Locale.ROOT, """
                            {\
                            "error":{\
                            "code":"model_not_found",\
                            "message":"Resource not found at [%s] for request from inference entity id [id] status \
                            [404]. Error message: [The model `gpt-4awero` does not exist or you do not have access to it.]",\
                            "type":"invalid_request_error"\
                            }}""", getUrl(webServer))));
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
            event: error
            data: { "error": { "message": "Timed out waiting for more data", "type": "timeout" } }

            """;
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));
        testStreamError("""
            {\
            "error":{\
            "message":"Received an error response for request from inference entity id [id]. Error message: \
            [Timed out waiting for more data]",\
            "type":"timeout"\
            }}""");
    }

    private void testStreamError(String expectedResponse) throws Exception {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var service = new OpenAiService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            var model = OpenAiChatCompletionModelTests.createChatCompletionModel(getUrl(webServer), "org", "secret", "model", "user");
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.unifiedCompletionInfer(
                model,
                UnifiedCompletionRequest.of(List.of(new Message(new ContentString("hello"), "user", null, null))),
                null,
                listener
            );

            var result = listener.actionGet(TEST_REQUEST_TIMEOUT);

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

    public void testUnifiedCompletionMalformedError() throws Exception {
        String responseJson = """
            data: { invalid json }

            """;
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));
        testStreamError("""
            {\
            "error":{\
            "code":"bad_request",\
            "message":"[1:3] Unexpected character ('i' (code 105)): was expecting double-quote to start field name\\n\
             at [Source: (String)\\"{ invalid json }\\"; line: 1, column: 3]",\
            "type":"x_content_parse_exception"\
            }}""");
    }

    public void testInfer_StreamRequest() throws Exception {
        String responseJson = """
            data: {\
                "id":"12345",\
                "object":"chat.completion.chunk",\
                "created":123456789,\
                "model":"gpt-4o-mini",\
                "system_fingerprint": "123456789",\
                "choices":[\
                    {\
                        "index":0,\
                        "delta":{\
                            "content":"hello, world"\
                        },\
                        "logprobs":null,\
                        "finish_reason":null\
                    }\
                ]\
            }

            """;
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

        streamCompletion().hasNoErrors().hasEvent("""
            {"completion":[{"delta":"hello, world"}]}""");
    }

    private InferenceEventsAssertion streamCompletion() throws Exception {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var service = new OpenAiService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            var model = OpenAiChatCompletionModelTests.createCompletionModel(getUrl(webServer), "org", "secret", "model", "user");
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(model, null, null, null, List.of("abc"), true, new HashMap<>(), InputType.INGEST, null, listener);

            return InferenceEventsAssertion.assertThat(listener.actionGet(TEST_REQUEST_TIMEOUT)).hasFinishedStream();
        }
    }

    public void testInfer_StreamRequest_ErrorResponse() throws Exception {
        String responseJson = """
            {
              "error": {
                "message": "You didn't provide an API key...",
                "type": "invalid_request_error",
                "param": null,
                "code": null
              }
            }""";
        webServer.enqueue(new MockResponse().setResponseCode(401).setBody(responseJson));

        var e = assertThrows(ElasticsearchStatusException.class, this::streamCompletion);
        assertThat(e.status(), equalTo(RestStatus.UNAUTHORIZED));
        assertThat(
            e.getMessage(),
            equalTo(
                "Received an authentication error status code for request from inference entity id [id] status [401]. "
                    + "Error message: [You didn't provide an API key...]"
            )
        );
    }

    public void testInfer_StreamRequestRetry() throws Exception {
        webServer.enqueue(new MockResponse().setResponseCode(503).setBody("""
            {
              "error": {
                "message": "server busy",
                "type": "server_busy"
              }
            }"""));
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody("""
            data: {\
                "id":"12345",\
                "object":"chat.completion.chunk",\
                "created":123456789,\
                "model":"gpt-4o-mini",\
                "system_fingerprint": "123456789",\
                "choices":[\
                    {\
                        "index":0,\
                        "delta":{\
                            "content":"hello, world"\
                        },\
                        "logprobs":null,\
                        "finish_reason":null\
                    }\
                ]\
            }

            """));

        streamCompletion().hasNoErrors().hasEvent("""
            {"completion":[{"delta":"hello, world"}]}""");
    }

    public void testInfer_UnauthorisedResponse() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new OpenAiService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {

            String responseJson = """
                {
                    "error": {
                        "message": "Incorrect API key provided:",
                        "type": "invalid_request_error",
                        "param": null,
                        "code": "invalid_api_key"
                    }
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(401).setBody(responseJson));

            var model = OpenAiEmbeddingsModelTests.createModel(
                getUrl(webServer),
                "org",
                "secret",
                "model",
                "user",
                TaskType.TEXT_EMBEDDING
            );
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(model, null, null, null, List.of("abc"), false, new HashMap<>(), InputType.INTERNAL_INGEST, null, listener);

            var error = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TEST_REQUEST_TIMEOUT));
            assertThat(error.getMessage(), containsString("Received an authentication error status code for request"));
            assertThat(error.getMessage(), containsString("Error message: [Incorrect API key provided:]"));
            assertThat(webServer.requests(), hasSize(1));
        }
    }

    public void testMoveModelFromTaskToServiceSettings() {
        var taskSettings = new HashMap<String, Object>();
        taskSettings.put(ServiceFields.MODEL_ID, "model");
        var serviceSettings = new HashMap<String, Object>();
        OpenAiService.moveModelFromTaskToServiceSettings(taskSettings, serviceSettings);
        assertThat(taskSettings.keySet(), empty());
        assertEquals("model", serviceSettings.get(ServiceFields.MODEL_ID));
    }

    public void testMoveModelFromTaskToServiceSettings_OldID() {
        var taskSettings = new HashMap<String, Object>();
        taskSettings.put("model", "model");
        var serviceSettings = new HashMap<String, Object>();
        OpenAiService.moveModelFromTaskToServiceSettings(taskSettings, serviceSettings);
        assertThat(taskSettings.keySet(), empty());
        assertEquals("model", serviceSettings.get(ServiceFields.MODEL_ID));
    }

    public void testMoveModelFromTaskToServiceSettings_AlreadyMoved() {
        var taskSettings = new HashMap<String, Object>();
        var serviceSettings = new HashMap<String, Object>();
        taskSettings.put(ServiceFields.MODEL_ID, "model");
        OpenAiService.moveModelFromTaskToServiceSettings(taskSettings, serviceSettings);
        assertThat(taskSettings.keySet(), empty());
        assertEquals("model", serviceSettings.get(ServiceFields.MODEL_ID));
    }

    public void testChunkedInfer_ChunkingSettingsSet() throws IOException {
        var model = OpenAiEmbeddingsModelTests.createModel(
            getUrl(webServer),
            "org",
            "secret",
            "model",
            "user",
            ChunkingSettingsTests.createRandomChunkingSettings(),
            TaskType.TEXT_EMBEDDING
        );

        testChunkedInfer(model);
    }

    public void testChunkedInfer_ChunkingSettingsNotSet() throws IOException {
        var model = OpenAiEmbeddingsModelTests.createModel(
            getUrl(webServer),
            "org",
            "secret",
            "model",
            "user",
            (ChunkingSettings) null,
            TaskType.TEXT_EMBEDDING
        );

        testChunkedInfer(model);
    }

    public void testChunkedInfer_noInputs() throws IOException {
        var model = OpenAiEmbeddingsModelTests.createModel(
            getUrl(webServer),
            "org",
            "secret",
            "model",
            "user",
            (ChunkingSettings) null,
            TaskType.TEXT_EMBEDDING
        );
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new OpenAiService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            PlainActionFuture<List<ChunkedInference>> listener = new PlainActionFuture<>();
            service.chunkedInfer(model, null, List.of(), new HashMap<>(), InputType.INTERNAL_INGEST, null, listener);

            var results = listener.actionGet(TEST_REQUEST_TIMEOUT);
            assertThat(results, empty());
            assertThat(webServer.requests(), empty());
        }
    }

    private void testChunkedInfer(OpenAiEmbeddingsModel model) throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new OpenAiService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {

            // response with 2 embeddings
            String responseJson = """
                {
                  "object": "list",
                  "data": [
                      {
                          "object": "embedding",
                          "index": 0,
                          "embedding": [
                              0.123,
                              -0.123
                          ]
                      },
                      {
                          "object": "embedding",
                          "index": 1,
                          "embedding": [
                              0.223,
                              -0.223
                          ]
                      }
                  ],
                  "model": "text-embedding-ada-002-v2",
                  "usage": {
                      "prompt_tokens": 8,
                      "total_tokens": 8
                  }
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            PlainActionFuture<List<ChunkedInference>> listener = new PlainActionFuture<>();
            service.chunkedInfer(
                model,
                null,
                List.of(new ChunkInferenceInput("a"), new ChunkInferenceInput("bb")),
                new HashMap<>(),
                InputType.INTERNAL_INGEST,
                null,
                listener
            );

            var results = listener.actionGet(TEST_REQUEST_TIMEOUT);
            assertThat(results, hasSize(2));
            {
                assertThat(results.getFirst(), CoreMatchers.instanceOf(ChunkedInferenceEmbedding.class));
                var floatResult = (ChunkedInferenceEmbedding) results.getFirst();
                assertThat(floatResult.chunks(), hasSize(1));
                assertEquals(new ChunkedInference.TextOffset(0, 1), floatResult.chunks().getFirst().offset());
                assertThat(floatResult.chunks().getFirst().embedding(), Matchers.instanceOf(EmbeddingFloatResults.Embedding.class));
                assertThat(
                    ((EmbeddingFloatResults.Embedding) floatResult.chunks().getFirst().embedding()).values(),
                    is(new float[] { 0.123f, -0.123f })
                );
            }
            {
                assertThat(results.get(1), CoreMatchers.instanceOf(ChunkedInferenceEmbedding.class));
                var floatResult = (ChunkedInferenceEmbedding) results.get(1);
                assertThat(floatResult.chunks(), hasSize(1));
                assertEquals(new ChunkedInference.TextOffset(0, 2), floatResult.chunks().getFirst().offset());
                assertThat(floatResult.chunks().getFirst().embedding(), Matchers.instanceOf(EmbeddingFloatResults.Embedding.class));
                assertThat(
                    ((EmbeddingFloatResults.Embedding) floatResult.chunks().getFirst().embedding()).values(),
                    is(new float[] { 0.223f, -0.223f })
                );
            }

            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().getFirst().getUri().getQuery());
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));
            assertThat(webServer.requests().getFirst().getHeader(ORGANIZATION_HEADER), equalTo("org"));

            var requestMap = entityAsMap(webServer.requests().getFirst().getBody());
            assertThat(requestMap.size(), is(3));
            assertThat(requestMap.get("input"), is(List.of("a", "bb")));
            assertThat(requestMap.get("model"), is("model"));
            assertThat(requestMap.get("user"), is("user"));
        }
    }

    public void testEmbeddingInfer() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new OpenAiService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {

            String responseJson = """
                {
                  "object": "list",
                  "data": [
                      {
                          "object": "embedding",
                          "index": 0,
                          "embedding": [
                              0.0123,
                              -0.0123
                          ]
                      },
                      {
                          "object": "embedding",
                          "index": 0,
                          "embedding": [
                              1.0123,
                              -1.0123
                          ]
                      }
                  ],
                  "model": "text-embedding-ada-002-v2",
                  "usage": {
                      "prompt_tokens": 8,
                      "total_tokens": 8
                  }
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = OpenAiEmbeddingsModelTests.createModel(getUrl(webServer), "org", "secret", "model", "user", TaskType.EMBEDDING);
            var listener = new TestPlainActionFuture<InferenceServiceResults>();
            var inputString1 = "abc";
            var inputString2 = "def";
            service.embeddingInfer(
                model,
                new EmbeddingRequest(
                    List.of(new InferenceStringGroup(inputString1), new InferenceStringGroup(inputString2)),
                    InputType.UNSPECIFIED,
                    Map.of()
                ),
                null,
                listener
            );

            var result = listener.actionGet(TEST_REQUEST_TIMEOUT);

            assertThat(
                result.asMap(),
                is(
                    GenericDenseEmbeddingFloatResultsTests.buildExpectationFloat(
                        List.of(new float[] { 0.0123F, -0.0123F }, new float[] { 1.0123F, -1.0123F })
                    )
                )
            );

            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().getFirst().getUri().getQuery());
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));
            assertThat(webServer.requests().getFirst().getHeader(ORGANIZATION_HEADER), equalTo("org"));

            var requestMap = entityAsMap(webServer.requests().getFirst().getBody());
            assertThat(requestMap.size(), is(3));
            assertThat(requestMap.get("input"), is(List.of(inputString1, inputString2)));
            assertThat(requestMap.get("model"), is("model"));
            assertThat(requestMap.get("user"), is("user"));
        }
    }

    public void testEmbeddingInfer_FailsWithNonTextInputs() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new OpenAiService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {

            var model = OpenAiEmbeddingsModelTests.createModel(getUrl(webServer), "org", "secret", "model", "user", TaskType.EMBEDDING);
            var listener = new TestPlainActionFuture<InferenceServiceResults>();
            service.embeddingInfer(
                model,
                new EmbeddingRequest(
                    List.of(
                        new InferenceStringGroup("abc"),
                        new InferenceStringGroup(new InferenceString(DataType.IMAGE, InferenceStringTests.TEST_DATA_URI))
                    ),
                    InputType.UNSPECIFIED,
                    Map.of()
                ),
                null,
                listener
            );

            var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TEST_REQUEST_TIMEOUT));
            assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
            assertThat(exception.getMessage(), is("The openai service does not support embedding with non-text inputs"));
        }
    }

    public void testEmbeddingInfer_FailsWithMultipleItemsForOneContentObject() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new OpenAiService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {

            var model = OpenAiEmbeddingsModelTests.createModel(getUrl(webServer), "org", "secret", "model", "user", TaskType.EMBEDDING);
            var listener = new TestPlainActionFuture<InferenceServiceResults>();
            service.embeddingInfer(
                model,
                new EmbeddingRequest(
                    List.of(
                        new InferenceStringGroup(
                            List.of(new InferenceString(DataType.TEXT, "abc"), new InferenceString(DataType.TEXT, "def"))
                        )
                    ),
                    InputType.UNSPECIFIED,
                    Map.of()
                ),
                null,
                listener
            );

            var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TEST_REQUEST_TIMEOUT));
            assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
            assertThat(
                exception.getMessage(),
                is(
                    "Field [content] must contain a single item for [openai] service. "
                        + "[content] object with multiple items found at $.input.content[0]"
                )
            );
        }
    }

    public void testEmbeddingInfer_FailsWithNonOpenAiModel() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new OpenAiService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {

            var mockModel = getInvalidModel("model_id", "service_name");
            var listener = new TestPlainActionFuture<InferenceServiceResults>();
            service.embeddingInfer(
                mockModel,
                new EmbeddingRequest(List.of(new InferenceStringGroup("abc")), InputType.UNSPECIFIED, Map.of()),
                null,
                listener
            );

            var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TEST_REQUEST_TIMEOUT));
            assertThat(exception.status(), is(RestStatus.INTERNAL_SERVER_ERROR));
            assertThat(
                exception.getMessage(),
                is("The internal model was invalid, please delete the service [service_name] with id [model_id] and add it again.")
            );
        }
    }

    @SuppressWarnings("checkstyle:LineLength")
    public void testGetConfiguration() throws Exception {
        try (var service = createInferenceService()) {
            String content = XContentHelper.stripWhitespace(
                """
                    {
                            "service": "openai",
                            "name": "OpenAI",
                            "task_types": ["text_embedding", "completion", "chat_completion", "embedding"],
                            "configurations": {
                                "api_key": {
                                    "description": "The OpenAI API authentication key. For more details about generating OpenAI API keys, refer to the https://platform.openai.com/account/api-keys.",
                                    "label": "API Key",
                                    "required": true,
                                    "sensitive": true,
                                    "updatable": true,
                                    "type": "str",
                                    "supported_task_types": ["text_embedding", "completion", "chat_completion", "embedding"]
                                },
                                "url": {
                                    "description": "The absolute URL of the external service to send requests to.",
                                    "label": "URL",
                                    "required": false,
                                    "sensitive": false,
                                    "updatable": false,
                                    "type": "str",
                                    "supported_task_types": ["text_embedding", "completion", "chat_completion", "embedding"]
                                },
                                "dimensions": {
                                    "description": "The number of dimensions the resulting embeddings should have. For more information refer to https://platform.openai.com/docs/api-reference/embeddings/create#embeddings-create-dimensions.",
                                    "label": "Dimensions",
                                    "required": false,
                                    "sensitive": false,
                                    "updatable": false,
                                    "type": "int",
                                    "supported_task_types": ["text_embedding", "embedding"]
                                },
                                "organization_id": {
                                    "description": "The unique identifier of your organization.",
                                    "label": "Organization ID",
                                    "required": false,
                                    "sensitive": false,
                                    "updatable": false,
                                    "type": "str",
                                    "supported_task_types": ["text_embedding", "completion", "chat_completion", "embedding"]
                                },
                                "rate_limit.requests_per_minute": {
                                    "description": "Default number of requests allowed per minute. For text_embedding and embedding it is 3000. For completion and chat_completion it is 500.",
                                    "label": "Rate Limit",
                                    "required": false,
                                    "sensitive": false,
                                    "updatable": false,
                                    "type": "int",
                                    "supported_task_types": ["text_embedding", "completion", "chat_completion", "embedding"]
                                },
                                "model_id": {
                                    "description": "The name of the model to use for the inference task.",
                                    "label": "Model ID",
                                    "required": true,
                                    "sensitive": false,
                                    "updatable": false,
                                    "type": "str",
                                    "supported_task_types": ["text_embedding", "completion", "chat_completion", "embedding"]
                                },
                                "headers": {
                                    "description": "Custom headers to include in the requests to OpenAI.",
                                    "label": "Custom Headers",
                                    "required": false,
                                    "sensitive": false,
                                    "updatable": true,
                                    "type": "map",
                                    "supported_task_types": ["text_embedding", "completion", "chat_completion", "embedding"]
                                }
                            }
                        }
                    """
            );
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

    @Override
    public InferenceService createInferenceService() {
        return new OpenAiService(
            HttpRequestSenderTests.createSenderFactory(threadPool, clientManager),
            createWithEmptySettings(threadPool),
            mockClusterServiceEmpty()
        );
    }

    @Override
    public Model createEmbeddingModel(SimilarityMeasure similarity) {
        return OpenAiEmbeddingsModelTests.createModel(
            randomAlphaOfLength(8),
            null,
            randomAlphaOfLength(8),
            randomAlphaOfLength(8),
            null,
            similarity,
            null,
            null,
            false,
            TaskType.TEXT_EMBEDDING
        );
    }
}
