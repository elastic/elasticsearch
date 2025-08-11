/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral;

import org.apache.http.HttpHeaders;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceEmbedding;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.UnifiedChatCompletionException;
import org.elasticsearch.xpack.inference.ModelConfigurationsTests;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.InferenceEventsAssertion;
import org.elasticsearch.xpack.inference.services.mistral.completion.MistralChatCompletionModel;
import org.elasticsearch.xpack.inference.services.mistral.completion.MistralChatCompletionModelTests;
import org.elasticsearch.xpack.inference.services.mistral.embeddings.MistralEmbeddingModelTests;
import org.elasticsearch.xpack.inference.services.mistral.embeddings.MistralEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.mistral.embeddings.MistralEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
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
import static org.elasticsearch.xpack.inference.Utils.getInvalidModel;
import static org.elasticsearch.xpack.inference.Utils.getPersistedConfigMap;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.chunking.ChunkingSettingsTests.createRandomChunkingSettings;
import static org.elasticsearch.xpack.inference.chunking.ChunkingSettingsTests.createRandomChunkingSettingsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.mistral.MistralConstants.API_KEY_FIELD;
import static org.elasticsearch.xpack.inference.services.mistral.completion.MistralChatCompletionServiceSettingsTests.getServiceSettingsMap;
import static org.elasticsearch.xpack.inference.services.mistral.embeddings.MistralEmbeddingsServiceSettingsTests.createRequestSettingsMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class MistralServiceTests extends ESTestCase {
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

    public void testParseRequestConfig_CreatesAMistralEmbeddingsModel() throws IOException {
        try (var service = createService()) {
            ActionListener<Model> modelVerificationListener = ActionListener.wrap(model -> {
                assertThat(model, instanceOf(MistralEmbeddingsModel.class));

                var embeddingsModel = (MistralEmbeddingsModel) model;
                var serviceSettings = (MistralEmbeddingsServiceSettings) model.getServiceSettings();
                assertThat(serviceSettings.modelId(), is("mistral-embed"));
                assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
            }, exception -> fail("Unexpected exception: " + exception));

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(getEmbeddingsServiceSettingsMap(null, null), getTaskSettingsMap(), getSecretSettingsMap("secret")),
                modelVerificationListener
            );
        }
    }

    public void testParseRequestConfig_CreatesAMistralEmbeddingsModelWhenChunkingSettingsProvided() throws IOException {
        try (var service = createService()) {
            ActionListener<Model> modelVerificationListener = ActionListener.wrap(model -> {
                assertThat(model, instanceOf(MistralEmbeddingsModel.class));

                var embeddingsModel = (MistralEmbeddingsModel) model;
                var serviceSettings = (MistralEmbeddingsServiceSettings) model.getServiceSettings();
                assertThat(serviceSettings.modelId(), is("mistral-embed"));
                assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
                assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
            }, exception -> fail("Unexpected exception: " + exception));

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    getEmbeddingsServiceSettingsMap(null, null),
                    getTaskSettingsMap(),
                    createRandomChunkingSettingsMap(),
                    getSecretSettingsMap("secret")
                ),
                modelVerificationListener
            );
        }
    }

    public void testParseRequestConfig_CreatesAMistralEmbeddingsModelWhenChunkingSettingsNotProvided() throws IOException {
        try (var service = createService()) {
            ActionListener<Model> modelVerificationListener = ActionListener.wrap(model -> {
                assertThat(model, instanceOf(MistralEmbeddingsModel.class));

                var embeddingsModel = (MistralEmbeddingsModel) model;
                var serviceSettings = (MistralEmbeddingsServiceSettings) model.getServiceSettings();
                assertThat(serviceSettings.modelId(), is("mistral-embed"));
                assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
                assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
            }, exception -> fail("Unexpected exception: " + exception));

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(getEmbeddingsServiceSettingsMap(null, null), getTaskSettingsMap(), getSecretSettingsMap("secret")),
                modelVerificationListener
            );
        }
    }

    public void testParseRequestConfig_CreatesChatCompletionsModel() throws IOException {
        var model = "model";
        var secret = "secret";

        try (var service = createService()) {
            ActionListener<Model> modelVerificationListener = ActionListener.wrap(m -> {
                assertThat(m, instanceOf(MistralChatCompletionModel.class));

                var completionsModel = (MistralChatCompletionModel) m;

                assertThat(completionsModel.getServiceSettings().modelId(), is(model));
                assertThat(completionsModel.getSecretSettings().apiKey().toString(), is(secret));

            }, exception -> fail("Unexpected exception: " + exception));

            service.parseRequestConfig(
                "id",
                TaskType.COMPLETION,
                getRequestConfigMap(getServiceSettingsMap(model), getSecretSettingsMap(secret)),
                modelVerificationListener
            );
        }
    }

    public void testParseRequestConfig_ThrowsException_WithoutModelId() throws IOException {
        var secret = "secret";

        try (var service = createService()) {
            ActionListener<Model> modelVerificationListener = ActionListener.wrap(m -> {
                assertThat(m, instanceOf(MistralChatCompletionModel.class));

                var completionsModel = (MistralChatCompletionModel) m;

                assertNull(completionsModel.getServiceSettings().modelId());
                assertThat(completionsModel.getSecretSettings().apiKey().toString(), is(secret));

            }, exception -> {
                assertThat(exception, instanceOf(ValidationException.class));
                assertThat(
                    exception.getMessage(),
                    is("Validation Failed: 1: [service_settings] does not contain the required setting [model];")
                );
            });

            service.parseRequestConfig(
                "id",
                TaskType.COMPLETION,
                getRequestConfigMap(Collections.emptyMap(), getSecretSettingsMap(secret)),
                modelVerificationListener
            );
        }
    }

    public void testInfer_ThrowsErrorWhenTaskTypeIsNotValid_ChatCompletion() throws IOException {
        var sender = mock(Sender.class);

        var factory = mock(HttpRequestSender.Factory.class);
        when(factory.createSender()).thenReturn(sender);

        var mockModel = getInvalidModel("model_id", "service_name", TaskType.CHAT_COMPLETION);

        try (var service = new MistralService(factory, createWithEmptySettings(threadPool))) {
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                mockModel,
                null,
                null,
                null,
                List.of(""),
                false,
                new HashMap<>(),
                InputType.INGEST,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var thrownException = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(
                thrownException.getMessage(),
                is("The internal model was invalid, please delete the service [service_name] with id [model_id] and add it again.")
            );

            verify(factory, times(1)).createSender();
            verify(sender, times(1)).start();
        }

        verify(sender, times(1)).close();
        verifyNoMoreInteractions(factory);
        verifyNoMoreInteractions(sender);
    }

    public void testUnifiedCompletionInfer() throws Exception {
        // The escapes are because the streaming response must be on a single line
        String responseJson = """
            data: {\
                 "id": "37d683fc0b3949b880529fb20973aca7",\
                 "object": "chat.completion.chunk",\
                 "created": 1749032579,\
                 "model": "mistral-small-latest",\
                 "choices": [\
                     {\
                         "index": 0,\
                         "delta": {\
                             "content": "Cho"\
                         },\
                         "finish_reason": "length",\
                         "logprobs": null\
                     }\
                 ],\
                 "usage": {\
                     "prompt_tokens": 10,\
                     "total_tokens": 11,\
                     "completion_tokens": 1\
                 }\
             }

            """;
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var service = new MistralService(senderFactory, createWithEmptySettings(threadPool))) {
            var model = MistralChatCompletionModelTests.createChatCompletionModel(getUrl(webServer), "secret", "model");
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.unifiedCompletionInfer(
                model,
                UnifiedCompletionRequest.of(
                    List.of(new UnifiedCompletionRequest.Message(new UnifiedCompletionRequest.ContentString("hello"), "user", null, null))
                ),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(TIMEOUT);
            InferenceEventsAssertion.assertThat(result).hasFinishedStream().hasNoErrors().hasEvent(XContentHelper.stripWhitespace("""
                {
                    "id": "37d683fc0b3949b880529fb20973aca7",
                    "choices": [{
                            "delta": {
                                "content": "Cho"
                            },
                            "finish_reason": "length",
                            "index": 0
                        }
                    ],
                    "model": "mistral-small-latest",
                    "object": "chat.completion.chunk",
                    "usage": {
                        "completion_tokens": 1,
                        "prompt_tokens": 10,
                        "total_tokens": 11
                    }
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
        try (var service = new MistralService(senderFactory, createWithEmptySettings(threadPool))) {
            var model = MistralChatCompletionModelTests.createChatCompletionModel(getUrl(webServer), "secret", "model");
            var latch = new CountDownLatch(1);
            service.unifiedCompletionInfer(
                model,
                UnifiedCompletionRequest.of(
                    List.of(new UnifiedCompletionRequest.Message(new UnifiedCompletionRequest.ContentString("hello"), "user", null, null))
                ),
                InferenceAction.Request.DEFAULT_TIMEOUT,
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
                                "type" : "mistral_error"
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
        try (var service = new MistralService(senderFactory, createWithEmptySettings(threadPool))) {
            var model = MistralChatCompletionModelTests.createCompletionModel(getUrl(webServer), "secret", "model");
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                model,
                null,
                null,
                null,
                List.of("abc"),
                true,
                new HashMap<>(),
                InputType.INGEST,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            return InferenceEventsAssertion.assertThat(listener.actionGet(TIMEOUT)).hasFinishedStream();
        }
    }

    public void testInfer_StreamRequest_ErrorResponse() {
        String responseJson = """
            {
                "message": "Unauthorized",
                "request_id": "ad95a2165083f20b490f8f78a14bb104"
            }""";
        webServer.enqueue(new MockResponse().setResponseCode(401).setBody(responseJson));

        var e = assertThrows(ElasticsearchStatusException.class, this::streamCompletion);
        assertThat(e.status(), equalTo(RestStatus.UNAUTHORIZED));
        assertThat(e.getMessage(), equalTo("""
            Received an authentication error status code for request from inference entity id [id] status [401]. Error message: [{
                "message": "Unauthorized",
                "request_id": "ad95a2165083f20b490f8f78a14bb104"
            }]"""));
    }

    public void testSupportsStreaming() throws IOException {
        try (var service = new MistralService(mock(), createWithEmptySettings(mock()))) {
            assertThat(service.supportedStreamingTasks(), is(EnumSet.of(TaskType.COMPLETION, TaskType.CHAT_COMPLETION)));
            assertFalse(service.canStream(TaskType.ANY));
        }
    }

    public void testParseRequestConfig_ThrowsUnsupportedModelType() throws IOException {
        try (var service = createService()) {
            ActionListener<Model> modelVerificationListener = ActionListener.wrap(
                model -> fail("Expected exception, but got model: " + model),
                exception -> {
                    assertThat(exception, instanceOf(ElasticsearchStatusException.class));
                    assertThat(exception.getMessage(), is("The [mistral] service does not support task type [sparse_embedding]"));
                }
            );

            service.parseRequestConfig(
                "id",
                TaskType.SPARSE_EMBEDDING,
                getRequestConfigMap(getEmbeddingsServiceSettingsMap(null, null), getTaskSettingsMap(), getSecretSettingsMap("secret")),
                modelVerificationListener
            );
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInConfig() throws IOException {
        testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInConfig(
            getRequestConfigMap(getEmbeddingsServiceSettingsMap(null, null), getTaskSettingsMap(), getSecretSettingsMap("secret")),
            TaskType.TEXT_EMBEDDING
        );
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInConfig_Completion() throws IOException {
        testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInConfig(
            getRequestConfigMap(getServiceSettingsMap("mistral-completion"), getSecretSettingsMap("secret")),
            TaskType.COMPLETION
        );
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInConfig_ChatCompletion() throws IOException {
        testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInConfig(
            getRequestConfigMap(getServiceSettingsMap("mistral-chat-completion"), getSecretSettingsMap("secret")),
            TaskType.CHAT_COMPLETION
        );
    }

    private void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInConfig(Map<String, Object> secret, TaskType chatCompletion)
        throws IOException {
        try (var service = createService()) {
            secret.put("extra_key", "value");

            ActionListener<Model> modelVerificationListener = ActionListener.wrap(
                model -> fail("Expected exception, but got model: " + model),
                exception -> {
                    assertThat(exception, instanceOf(ElasticsearchStatusException.class));
                    assertThat(
                        exception.getMessage(),
                        is("Configuration contains settings [{extra_key=value}] unknown to the [mistral] service")
                    );
                }
            );

            service.parseRequestConfig("id", chatCompletion, secret, modelVerificationListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInEmbeddingTaskSettingsMap() throws IOException {
        testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInTaskSettingsMap(
            getEmbeddingsServiceSettingsMap(null, null),
            TaskType.TEXT_EMBEDDING
        );
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInCompletionTaskSettingsMap() throws IOException {
        testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInTaskSettingsMap(
            getServiceSettingsMap("mistral-completion"),
            TaskType.COMPLETION
        );
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInChatCompletionTaskSettingsMap() throws IOException {
        testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInTaskSettingsMap(
            getServiceSettingsMap("mistral-chat-completion"),
            TaskType.CHAT_COMPLETION
        );
    }

    private void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInTaskSettingsMap(
        Map<String, Object> serviceSettingsMap,
        TaskType chatCompletion
    ) throws IOException {
        try (var service = createService()) {
            var taskSettings = new HashMap<String, Object>();
            taskSettings.put("extra_key", "value");

            var config = getRequestConfigMap(serviceSettingsMap, taskSettings, getSecretSettingsMap("secret"));

            ActionListener<Model> modelVerificationListener = ActionListener.wrap(
                model -> fail("Expected exception, but got model: " + model),
                exception -> {
                    assertThat(exception, instanceOf(ElasticsearchStatusException.class));
                    assertThat(
                        exception.getMessage(),
                        is("Configuration contains settings [{extra_key=value}] unknown to the [mistral] service")
                    );
                }
            );

            service.parseRequestConfig("id", chatCompletion, config, modelVerificationListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInEmbeddingSecretSettingsMap() throws IOException {
        try (var service = createService()) {
            var secretSettings = getSecretSettingsMap("secret");
            secretSettings.put("extra_key", "value");

            var config = getRequestConfigMap(getEmbeddingsServiceSettingsMap(null, null), getTaskSettingsMap(), secretSettings);

            ActionListener<Model> modelVerificationListener = ActionListener.wrap(
                model -> fail("Expected exception, but got model: " + model),
                exception -> {
                    assertThat(exception, instanceOf(ElasticsearchStatusException.class));
                    assertThat(
                        exception.getMessage(),
                        is("Configuration contains settings [{extra_key=value}] unknown to the [mistral] service")
                    );
                }
            );

            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, modelVerificationListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInCompletionSecretSettingsMap() throws IOException {
        testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInSecretSettingsMap("mistral-completion", TaskType.COMPLETION);
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInChatCompletionSecretSettingsMap() throws IOException {
        testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInSecretSettingsMap("mistral-chat-completion", TaskType.CHAT_COMPLETION);
    }

    private void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInSecretSettingsMap(String modelId, TaskType chatCompletion)
        throws IOException {
        try (var service = createService()) {
            var secretSettings = getSecretSettingsMap("secret");
            secretSettings.put("extra_key", "value");

            var config = getRequestConfigMap(getServiceSettingsMap(modelId), secretSettings);

            ActionListener<Model> modelVerificationListener = ActionListener.wrap(
                model -> fail("Expected exception, but got model: " + model),
                exception -> {
                    assertThat(exception, instanceOf(ElasticsearchStatusException.class));
                    assertThat(
                        exception.getMessage(),
                        is("Configuration contains settings [{extra_key=value}] unknown to the [mistral] service")
                    );
                }
            );

            service.parseRequestConfig("id", chatCompletion, config, modelVerificationListener);
        }
    }

    public void testParsePersistedConfig_CreatesAMistralEmbeddingsModel() throws IOException {
        try (var service = createService()) {
            var config = getPersistedConfigMap(
                getEmbeddingsServiceSettingsMap(1024, 512),
                getTaskSettingsMap(),
                getSecretSettingsMap("secret")
            );

            var model = service.parsePersistedConfigWithSecrets("id", TaskType.TEXT_EMBEDDING, config.config(), config.secrets());

            assertThat(model, instanceOf(MistralEmbeddingsModel.class));

            var embeddingsModel = (MistralEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().modelId(), is("mistral-embed"));
            assertThat(embeddingsModel.getServiceSettings().dimensions(), is(1024));
            assertThat(embeddingsModel.getServiceSettings().maxInputTokens(), is(512));
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfig_CreatesAMistralCompletionModel() throws IOException {
        testParsePersistedConfig_CreatesAMistralModel("mistral-completion", TaskType.COMPLETION);
    }

    public void testParsePersistedConfig_CreatesAMistralChatCompletionModel() throws IOException {
        testParsePersistedConfig_CreatesAMistralModel("mistral-chat-completion", TaskType.CHAT_COMPLETION);
    }

    private void testParsePersistedConfig_CreatesAMistralModel(String modelId, TaskType chatCompletion) throws IOException {
        try (var service = createService()) {
            var config = getPersistedConfigMap(getServiceSettingsMap(modelId), getTaskSettingsMap(), getSecretSettingsMap("secret"));

            var model = service.parsePersistedConfigWithSecrets("id", chatCompletion, config.config(), config.secrets());

            assertThat(model, instanceOf(MistralChatCompletionModel.class));

            var embeddingsModel = (MistralChatCompletionModel) model;
            assertThat(embeddingsModel.getServiceSettings().modelId(), is(modelId));
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfig_CreatesAMistralEmbeddingsModelWhenChunkingSettingsProvided() throws IOException {
        try (var service = createService()) {
            var config = getPersistedConfigMap(
                getEmbeddingsServiceSettingsMap(1024, 512),
                getTaskSettingsMap(),
                createRandomChunkingSettingsMap(),
                getSecretSettingsMap("secret")
            );

            var model = service.parsePersistedConfigWithSecrets("id", TaskType.TEXT_EMBEDDING, config.config(), config.secrets());

            assertThat(model, instanceOf(MistralEmbeddingsModel.class));

            var embeddingsModel = (MistralEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().modelId(), is("mistral-embed"));
            assertThat(embeddingsModel.getServiceSettings().dimensions(), is(1024));
            assertThat(embeddingsModel.getServiceSettings().maxInputTokens(), is(512));
            assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfig_CreatesAMistralEmbeddingsModelWhenChunkingSettingsNotProvided() throws IOException {
        try (var service = createService()) {
            var config = getPersistedConfigMap(
                getEmbeddingsServiceSettingsMap(1024, 512),
                getTaskSettingsMap(),
                getSecretSettingsMap("secret")
            );

            var model = service.parsePersistedConfigWithSecrets("id", TaskType.TEXT_EMBEDDING, config.config(), config.secrets());

            assertThat(model, instanceOf(MistralEmbeddingsModel.class));

            var embeddingsModel = (MistralEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().modelId(), is("mistral-embed"));
            assertThat(embeddingsModel.getServiceSettings().dimensions(), is(1024));
            assertThat(embeddingsModel.getServiceSettings().maxInputTokens(), is(512));
            assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfig_ThrowsUnsupportedModelType() throws IOException {
        try (var service = createService()) {
            ActionListener<Model> modelVerificationListener = ActionListener.wrap(
                model -> fail("Expected exception, but got model: " + model),
                exception -> {
                    assertThat(exception, instanceOf(ElasticsearchStatusException.class));
                    assertThat(exception.getMessage(), is("The [mistral] service does not support task type [sparse_embedding]"));
                }
            );

            service.parseRequestConfig(
                "id",
                TaskType.SPARSE_EMBEDDING,
                getRequestConfigMap(getEmbeddingsServiceSettingsMap(null, null), getTaskSettingsMap(), getSecretSettingsMap("secret")),
                modelVerificationListener
            );
        }
    }

    public void testParsePersistedConfigWithSecrets_ThrowsErrorTryingToParseInvalidModel() throws IOException {
        try (var service = createService()) {
            var config = getPersistedConfigMap(
                getEmbeddingsServiceSettingsMap(null, null),
                getTaskSettingsMap(),
                getSecretSettingsMap("secret")
            );

            var thrownException = expectThrows(
                ElasticsearchStatusException.class,
                () -> service.parsePersistedConfigWithSecrets("id", TaskType.SPARSE_EMBEDDING, config.config(), config.secrets())
            );

            assertThat(
                thrownException.getMessage(),
                is("Failed to parse stored model [id] for [mistral] service, please delete and add the service again")
            );
        }
    }

    public void testParsePersistedConfig_DoesNotThrowWhenAnExtraKeyExistsInConfigEmbeddings() throws IOException {
        testParsePersistedConfig_DoesNotThrowWhenAnExtraKeyExistsInConfig(
            getEmbeddingsServiceSettingsMap(1024, 512),
            TaskType.TEXT_EMBEDDING,
            instanceOf(MistralEmbeddingsModel.class)
        );
    }

    public void testParsePersistedConfig_DoesNotThrowWhenAnExtraKeyExistsInConfigCompletion() throws IOException {
        testParsePersistedConfig_DoesNotThrowWhenAnExtraKeyExistsInConfig(
            getServiceSettingsMap("mistral-completion"),
            TaskType.COMPLETION,
            instanceOf(MistralChatCompletionModel.class)
        );
    }

    public void testParsePersistedConfig_DoesNotThrowWhenAnExtraKeyExistsInConfigChatCompletion() throws IOException {
        testParsePersistedConfig_DoesNotThrowWhenAnExtraKeyExistsInConfig(
            getServiceSettingsMap("mistral-chat-completion"),
            TaskType.CHAT_COMPLETION,
            instanceOf(MistralChatCompletionModel.class)
        );
    }

    private void testParsePersistedConfig_DoesNotThrowWhenAnExtraKeyExistsInConfig(
        Map<String, Object> serviceSettingsMap,
        TaskType chatCompletion,
        Matcher<MistralModel> matcher
    ) throws IOException {
        try (var service = createService()) {
            var taskSettings = getTaskSettingsMap();
            var secretSettings = getSecretSettingsMap("secret");
            var config = getPersistedConfigMap(serviceSettingsMap, taskSettings, secretSettings);
            config.config().put("extra_key", "value");

            var model = service.parsePersistedConfigWithSecrets("id", chatCompletion, config.config(), config.secrets());

            assertThat(model, matcher);
        }
    }

    public void testParsePersistedConfig_DoesNotThrowWhenExtraKeyExistsInEmbeddingServiceSettingsMap() throws IOException {
        testParsePersistedConfig_DoesNotThrowWhenExtraKeyExistsInServiceSettingsMap(
            getEmbeddingsServiceSettingsMap(1024, 512),
            TaskType.TEXT_EMBEDDING,
            instanceOf(MistralEmbeddingsModel.class)
        );
    }

    public void testParsePersistedConfig_DoesNotThrowWhenExtraKeyExistsInCompletionServiceSettingsMap() throws IOException {
        testParsePersistedConfig_DoesNotThrowWhenExtraKeyExistsInServiceSettingsMap(
            getServiceSettingsMap("mistral-completion"),
            TaskType.COMPLETION,
            instanceOf(MistralChatCompletionModel.class)
        );
    }

    public void testParsePersistedConfig_DoesNotThrowWhenExtraKeyExistsInChatCompletionServiceSettingsMap() throws IOException {
        testParsePersistedConfig_DoesNotThrowWhenExtraKeyExistsInServiceSettingsMap(
            getServiceSettingsMap("mistral-chat-completion"),
            TaskType.CHAT_COMPLETION,
            instanceOf(MistralChatCompletionModel.class)
        );
    }

    private void testParsePersistedConfig_DoesNotThrowWhenExtraKeyExistsInServiceSettingsMap(
        Map<String, Object> serviceSettingsMap,
        TaskType chatCompletion,
        Matcher<MistralModel> matcher
    ) throws IOException {
        try (var service = createService()) {
            serviceSettingsMap.put("extra_key", "value");

            var taskSettings = getTaskSettingsMap();
            var secretSettings = getSecretSettingsMap("secret");
            var config = getPersistedConfigMap(serviceSettingsMap, taskSettings, secretSettings);

            var model = service.parsePersistedConfigWithSecrets("id", chatCompletion, config.config(), config.secrets());

            assertThat(model, matcher);
        }
    }

    public void testParsePersistedConfig_DoesNotThrowWhenAnExtraKeyExistsInEmbeddingTaskSettingsMap() throws IOException {
        try (var service = createService()) {
            var serviceSettings = getEmbeddingsServiceSettingsMap(1024, 512);
            var taskSettings = new HashMap<String, Object>();
            taskSettings.put("extra_key", "value");

            var secretSettings = getSecretSettingsMap("secret");
            var config = getPersistedConfigMap(serviceSettings, taskSettings, secretSettings);

            var model = service.parsePersistedConfigWithSecrets("id", TaskType.TEXT_EMBEDDING, config.config(), config.secrets());

            assertThat(model, instanceOf(MistralEmbeddingsModel.class));
        }
    }

    public void testParsePersistedConfig_DoesNotThrowWhenAnExtraKeyExistsInEmbeddingSecretSettingsMap() throws IOException {
        testParsePersistedConfig_DoesNotThrowWhenAnExtraKeyExistsSecretSettingsMap(
            getEmbeddingsServiceSettingsMap(1024, 512),
            TaskType.TEXT_EMBEDDING,
            instanceOf(MistralEmbeddingsModel.class)
        );
    }

    public void testParsePersistedConfig_DoesNotThrowWhenAnExtraKeyExistsInCompletionSecretSettingsMap() throws IOException {
        testParsePersistedConfig_DoesNotThrowWhenAnExtraKeyExistsSecretSettingsMap(
            getServiceSettingsMap("mistral-completion"),
            TaskType.COMPLETION,
            instanceOf(MistralChatCompletionModel.class)
        );
    }

    public void testParsePersistedConfig_DoesNotThrowWhenAnExtraKeyExistsInChatCompletionSecretSettingsMap() throws IOException {
        testParsePersistedConfig_DoesNotThrowWhenAnExtraKeyExistsSecretSettingsMap(
            getServiceSettingsMap("mistral-chat-completion"),
            TaskType.CHAT_COMPLETION,
            instanceOf(MistralChatCompletionModel.class)
        );
    }

    private void testParsePersistedConfig_DoesNotThrowWhenAnExtraKeyExistsSecretSettingsMap(
        Map<String, Object> serviceSettingsMap,
        TaskType chatCompletion,
        Matcher<MistralModel> matcher
    ) throws IOException {
        try (var service = createService()) {
            var taskSettings = getTaskSettingsMap();
            var secretSettings = getSecretSettingsMap("secret");
            secretSettings.put("extra_key", "value");

            var config = getPersistedConfigMap(serviceSettingsMap, taskSettings, secretSettings);

            var model = service.parsePersistedConfigWithSecrets("id", chatCompletion, config.config(), config.secrets());

            assertThat(model, matcher);
        }
    }

    public void testParsePersistedConfig_WithoutSecretsCreatesEmbeddingsModel() throws IOException {
        try (var service = createService()) {
            var config = getPersistedConfigMap(getEmbeddingsServiceSettingsMap(1024, 512), getTaskSettingsMap(), Map.of());

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, config.config());

            assertThat(model, instanceOf(MistralEmbeddingsModel.class));

            var embeddingsModel = (MistralEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().modelId(), is("mistral-embed"));
            assertThat(embeddingsModel.getServiceSettings().dimensions(), is(1024));
            assertThat(embeddingsModel.getServiceSettings().maxInputTokens(), is(512));
        }
    }

    public void testParsePersistedConfig_WithoutSecretsCreatesAnEmbeddingsModelWhenChunkingSettingsProvided() throws IOException {
        try (var service = createService()) {
            var config = getPersistedConfigMap(
                getEmbeddingsServiceSettingsMap(1024, 512),
                getTaskSettingsMap(),
                createRandomChunkingSettingsMap(),
                Map.of()
            );

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, config.config());

            assertThat(model, instanceOf(MistralEmbeddingsModel.class));

            var embeddingsModel = (MistralEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().modelId(), is("mistral-embed"));
            assertThat(embeddingsModel.getServiceSettings().dimensions(), is(1024));
            assertThat(embeddingsModel.getServiceSettings().maxInputTokens(), is(512));
            assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
        }
    }

    public void testParsePersistedConfig_WithoutSecretsCreatesAnEmbeddingsModelWhenChunkingSettingsNotProvided() throws IOException {
        try (var service = createService()) {
            var config = getPersistedConfigMap(getEmbeddingsServiceSettingsMap(1024, 512), getTaskSettingsMap(), Map.of());

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, config.config());

            assertThat(model, instanceOf(MistralEmbeddingsModel.class));

            var embeddingsModel = (MistralEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().modelId(), is("mistral-embed"));
            assertThat(embeddingsModel.getServiceSettings().dimensions(), is(1024));
            assertThat(embeddingsModel.getServiceSettings().maxInputTokens(), is(512));
            assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
        }
    }

    public void testUpdateModelWithEmbeddingDetails_InvalidModelProvided() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var service = new MistralService(senderFactory, createWithEmptySettings(threadPool))) {
            var model = new Model(ModelConfigurationsTests.createRandomInstance());

            assertThrows(
                ElasticsearchStatusException.class,
                () -> { service.updateModelWithEmbeddingDetails(model, randomNonNegativeInt()); }
            );
        }
    }

    public void testUpdateModelWithEmbeddingDetails_NullSimilarityInOriginalModel() throws IOException {
        testUpdateModelWithEmbeddingDetails_Successful(null);
    }

    public void testUpdateModelWithEmbeddingDetails_NonNullSimilarityInOriginalModel() throws IOException {
        testUpdateModelWithEmbeddingDetails_Successful(randomFrom(SimilarityMeasure.values()));
    }

    private void testUpdateModelWithEmbeddingDetails_Successful(SimilarityMeasure similarityMeasure) throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var service = new MistralService(senderFactory, createWithEmptySettings(threadPool))) {
            var embeddingSize = randomNonNegativeInt();
            var model = MistralEmbeddingModelTests.createModel(
                randomAlphaOfLength(10),
                randomAlphaOfLength(10),
                randomAlphaOfLength(10),
                randomNonNegativeInt(),
                randomNonNegativeInt(),
                similarityMeasure,
                RateLimitSettingsTests.createRandom()
            );

            Model updatedModel = service.updateModelWithEmbeddingDetails(model, embeddingSize);

            SimilarityMeasure expectedSimilarityMeasure = similarityMeasure == null ? SimilarityMeasure.DOT_PRODUCT : similarityMeasure;
            assertEquals(expectedSimilarityMeasure, updatedModel.getServiceSettings().similarity());
            assertEquals(embeddingSize, updatedModel.getServiceSettings().dimensions().intValue());
        }
    }

    public void testInfer_ThrowsErrorWhenModelIsNotMistralEmbeddingsModel() throws IOException {
        var sender = mock(Sender.class);

        var factory = mock(HttpRequestSender.Factory.class);
        when(factory.createSender()).thenReturn(sender);

        var mockModel = getInvalidModel("model_id", "service_name");

        try (var service = new MistralService(factory, createWithEmptySettings(threadPool))) {
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                mockModel,
                null,
                null,
                null,
                List.of(""),
                false,
                new HashMap<>(),
                InputType.INTERNAL_INGEST,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var thrownException = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(
                thrownException.getMessage(),
                is("The internal model was invalid, please delete the service [service_name] with id [model_id] and add it again.")
            );

            verify(factory, times(1)).createSender();
            verify(sender, times(1)).start();
        }

        verify(sender, times(1)).close();
        verifyNoMoreInteractions(factory);
        verifyNoMoreInteractions(sender);
    }

    public void testInfer_ThrowsErrorWhenInputTypeIsSpecified() throws IOException {
        var sender = mock(Sender.class);

        var factory = mock(HttpRequestSender.Factory.class);
        when(factory.createSender()).thenReturn(sender);

        var model = MistralEmbeddingModelTests.createModel("id", "mistral-embed", "apikey", null, null, null, null);

        try (var service = new MistralService(factory, createWithEmptySettings(threadPool))) {
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();

            var thrownException = expectThrows(
                ValidationException.class,
                () -> service.infer(
                    model,
                    null,
                    null,
                    null,
                    List.of(""),
                    false,
                    new HashMap<>(),
                    InputType.INGEST,
                    InferenceAction.Request.DEFAULT_TIMEOUT,
                    listener
                )
            );
            assertThat(
                thrownException.getMessage(),
                is("Validation Failed: 1: Invalid input_type [ingest]. The input_type option is not supported by this service;")
            );

            verify(factory, times(1)).createSender();
            verify(sender, times(1)).start();
        }

        verify(sender, times(1)).close();
        verifyNoMoreInteractions(factory);
        verifyNoMoreInteractions(sender);
    }

    public void testChunkedInfer_ChunkingSettingsNotSet() throws IOException {
        var model = MistralEmbeddingModelTests.createModel("id", "mistral-embed", null, "apikey", null, null, null, null);
        model.setURI(getUrl(webServer));

        testChunkedInfer(model);
    }

    public void testChunkedInfer_ChunkingSettingsSet() throws IOException {
        var model = MistralEmbeddingModelTests.createModel(
            "id",
            "mistral-embed",
            createRandomChunkingSettings(),
            "apikey",
            null,
            null,
            null,
            null
        );
        model.setURI(getUrl(webServer));

        testChunkedInfer(model);
    }

    public void testChunkedInfer(MistralEmbeddingsModel model) throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new MistralService(senderFactory, createWithEmptySettings(threadPool))) {

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
                List.of(new ChunkInferenceInput("abc"), new ChunkInferenceInput("def")),
                new HashMap<>(),
                InputType.INTERNAL_INGEST,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var results = listener.actionGet(TIMEOUT);

            assertThat(results, hasSize(2));
            {
                assertThat(results.get(0), CoreMatchers.instanceOf(ChunkedInferenceEmbedding.class));
                var floatResult = (ChunkedInferenceEmbedding) results.get(0);
                assertThat(floatResult.chunks(), hasSize(1));
                assertThat(floatResult.chunks().get(0).embedding(), Matchers.instanceOf(TextEmbeddingFloatResults.Embedding.class));
                assertTrue(
                    Arrays.equals(
                        new float[] { 0.123f, -0.123f },
                        ((TextEmbeddingFloatResults.Embedding) floatResult.chunks().get(0).embedding()).values()
                    )
                );
            }
            {
                assertThat(results.get(1), CoreMatchers.instanceOf(ChunkedInferenceEmbedding.class));
                var floatResult = (ChunkedInferenceEmbedding) results.get(1);
                assertThat(floatResult.chunks(), hasSize(1));
                assertThat(floatResult.chunks().get(0).embedding(), Matchers.instanceOf(TextEmbeddingFloatResults.Embedding.class));
                assertTrue(
                    Arrays.equals(
                        new float[] { 0.223f, -0.223f },
                        ((TextEmbeddingFloatResults.Embedding) floatResult.chunks().get(0).embedding()).values()
                    )
                );
            }

            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().get(0).getUri().getQuery());
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer apikey"));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.size(), Matchers.is(3));
            assertThat(requestMap.get("input"), Matchers.is(List.of("abc", "def")));
            assertThat(requestMap.get("encoding_format"), Matchers.is("float"));
            assertThat(requestMap.get("model"), Matchers.is("mistral-embed"));
        }
    }

    public void testInfer_UnauthorisedResponse() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new MistralService(senderFactory, createWithEmptySettings(threadPool))) {

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

            var model = MistralEmbeddingModelTests.createModel("id", "mistral-embed", "apikey", null, null, null, null);
            model.setURI(getUrl(webServer));

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                model,
                null,
                null,
                null,
                List.of("abc"),
                false,
                new HashMap<>(),
                InputType.INTERNAL_INGEST,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var error = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(error.getMessage(), containsString("Received an authentication error status code for request"));
            assertThat(error.getMessage(), containsString("Error message: [Incorrect API key provided:]"));
            assertThat(webServer.requests(), hasSize(1));
        }
    }

    public void testGetConfiguration() throws Exception {
        try (var service = createService()) {
            String content = XContentHelper.stripWhitespace("""
                {
                       "service": "mistral",
                       "name": "Mistral",
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
                           "model": {
                               "description": "Refer to the Mistral models documentation for the list of available text embedding models.",
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
                           "max_input_tokens": {
                               "description": "Allows you to specify the maximum number of tokens per input.",
                               "label": "Maximum Input Tokens",
                               "required": false,
                               "sensitive": false,
                               "updatable": false,
                               "type": "int",
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
            BytesReference originalBytes = toShuffledXContent(configuration, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
            InferenceServiceConfiguration serviceConfiguration = service.getConfiguration();
            assertToXContentEquivalent(
                originalBytes,
                toXContent(serviceConfiguration, XContentType.JSON, humanReadable),
                XContentType.JSON
            );
        }
    }

    // ----------------------------------------------------------------

    private MistralService createService() {
        return new MistralService(mock(HttpRequestSender.Factory.class), createWithEmptySettings(threadPool));
    }

    private Map<String, Object> getRequestConfigMap(
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        Map<String, Object> chunkingSettings,
        Map<String, Object> secretSettings
    ) {
        var requestConfigMap = getRequestConfigMap(serviceSettings, taskSettings, secretSettings);
        requestConfigMap.put(ModelConfigurations.CHUNKING_SETTINGS, chunkingSettings);

        return requestConfigMap;
    }

    private Map<String, Object> getRequestConfigMap(
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        Map<String, Object> secretSettings
    ) {
        var builtServiceSettings = new HashMap<>();
        builtServiceSettings.putAll(serviceSettings);
        builtServiceSettings.putAll(secretSettings);

        return new HashMap<>(
            Map.of(ModelConfigurations.SERVICE_SETTINGS, builtServiceSettings, ModelConfigurations.TASK_SETTINGS, taskSettings)
        );
    }

    private Map<String, Object> getRequestConfigMap(Map<String, Object> serviceSettings, Map<String, Object> secretSettings) {
        var builtServiceSettings = new HashMap<>();
        builtServiceSettings.putAll(serviceSettings);
        builtServiceSettings.putAll(secretSettings);

        return new HashMap<>(Map.of(ModelConfigurations.SERVICE_SETTINGS, builtServiceSettings));
    }

    private static Map<String, Object> getEmbeddingsServiceSettingsMap(@Nullable Integer dimensions, @Nullable Integer maxTokens) {
        return createRequestSettingsMap("mistral-embed", dimensions, maxTokens, null);
    }

    private static Map<String, Object> getTaskSettingsMap() {
        // no task settings for Mistral embeddings
        return Map.of();
    }

    private static Map<String, Object> getSecretSettingsMap(String apiKey) {
        return new HashMap<>(Map.of(API_KEY_FIELD, apiKey));
    }

}
