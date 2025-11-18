/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openshiftai;

import org.apache.http.HttpHeaders;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.RerankingInferenceService;
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
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsBuilder;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceEmbedding;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.UnifiedChatCompletionException;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.AbstractInferenceServiceTests;
import org.elasticsearch.xpack.inference.services.InferenceEventsAssertion;
import org.elasticsearch.xpack.inference.services.SenderService;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.openshiftai.completion.OpenShiftAiChatCompletionModel;
import org.elasticsearch.xpack.inference.services.openshiftai.completion.OpenShiftAiChatCompletionModelTests;
import org.elasticsearch.xpack.inference.services.openshiftai.embeddings.OpenShiftAiEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.openshiftai.embeddings.OpenShiftAiEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.openshiftai.embeddings.OpenShiftAiEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.ExceptionsHelper.unwrapCause;
import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.inference.TaskType.CHAT_COMPLETION;
import static org.elasticsearch.inference.TaskType.COMPLETION;
import static org.elasticsearch.inference.TaskType.RERANK;
import static org.elasticsearch.inference.TaskType.TEXT_EMBEDDING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsTests.createRandomChunkingSettings;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.llama.embeddings.LlamaEmbeddingsServiceSettingsTests.buildServiceSettingsMap;
import static org.elasticsearch.xpack.inference.services.openshiftai.completion.OpenShiftAiChatCompletionModelTests.createChatCompletionModel;
import static org.elasticsearch.xpack.inference.services.openshiftai.completion.OpenShiftAiChatCompletionServiceSettingsTests.getServiceSettingsMap;
import static org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettingsTests.getSecretSettingsMap;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class OpenShiftAiServiceTests extends AbstractInferenceServiceTests {
    private static final String API_KEY_FIELD_NAME = "api_key";
    private static final String INPUT_FIELD_NAME = "input";
    private static final String MODEL_FIELD_NAME = "model";
    private static final String URL_VALUE = "http://www.abc.com";
    private static final String MODEL_VALUE = "some_model";
    private static final String ROLE_VALUE = "user";
    private static final String API_KEY_VALUE = "test_api_key";
    private static final String INFERENCE_ID_VALUE = "id";
    private static final int DIMENSIONS_VALUE = 1536;
    private static final int MAX_INPUT_TOKENS_VALUE = 512;
    private static final String FIRST_PART_OF_INPUT_VALUE = "abc";
    private static final String SECOND_PART_OF_INPUT_VALUE = "def";

    private final MockWebServer webServer = new MockWebServer();
    private ThreadPool threadPool;
    private HttpClientManager clientManager;

    public OpenShiftAiServiceTests() {
        super(createTestConfiguration());
    }

    public static TestConfiguration createTestConfiguration() {
        return new TestConfiguration.Builder(
            new CommonConfig(
                TaskType.TEXT_EMBEDDING,
                TaskType.SPARSE_EMBEDDING,
                EnumSet.of(TEXT_EMBEDDING, COMPLETION, CHAT_COMPLETION, RERANK)
            ) {

                @Override
                protected SenderService createService(ThreadPool threadPool, HttpClientManager clientManager) {
                    return OpenShiftAiServiceTests.createService(threadPool, clientManager);
                }

                @Override
                protected Map<String, Object> createServiceSettingsMap(TaskType taskType) {
                    return OpenShiftAiServiceTests.createServiceSettingsMap(taskType);
                }

                @Override
                protected Map<String, Object> createTaskSettingsMap() {
                    return new HashMap<>();
                }

                @Override
                protected Map<String, Object> createSecretSettingsMap() {
                    return OpenShiftAiServiceTests.createSecretSettingsMap();
                }

                @Override
                protected void assertModel(Model model, TaskType taskType, boolean modelIncludesSecrets) {
                    OpenShiftAiServiceTests.assertModel(model, taskType, modelIncludesSecrets);
                }

                @Override
                protected EnumSet<TaskType> supportedStreamingTasks() {
                    return EnumSet.of(TaskType.CHAT_COMPLETION, TaskType.COMPLETION);
                }
            }
        ).enableUpdateModelTests(new UpdateModelConfiguration() {
            @Override
            protected OpenShiftAiEmbeddingsModel createEmbeddingModel(SimilarityMeasure similarityMeasure) {
                return createInternalEmbeddingModel(similarityMeasure);
            }
        }).build();
    }

    private static void assertModel(Model model, TaskType taskType, boolean modelIncludesSecrets) {
        switch (taskType) {
            case TEXT_EMBEDDING -> assertTextEmbeddingModel(model, modelIncludesSecrets);
            case COMPLETION -> assertCompletionModel(model, modelIncludesSecrets);
            case CHAT_COMPLETION -> assertChatCompletionModel(model, modelIncludesSecrets);
            default -> fail(Strings.format("unexpected task type [%s]", taskType));
        }
    }

    private static void assertTextEmbeddingModel(Model model, boolean modelIncludesSecrets) {
        var openShiftAiModel = assertCommonModelFields(model, modelIncludesSecrets);

        assertThat(openShiftAiModel.getTaskType(), is(TaskType.TEXT_EMBEDDING));
        assertThat(model, instanceOf(OpenShiftAiEmbeddingsModel.class));
        var embeddingsModel = (OpenShiftAiEmbeddingsModel) model;
        assertThat(embeddingsModel.getServiceSettings().dimensions(), is(DIMENSIONS_VALUE));
        assertThat(embeddingsModel.getServiceSettings().similarity(), is(SimilarityMeasure.COSINE));
        assertThat(embeddingsModel.getServiceSettings().maxInputTokens(), is(MAX_INPUT_TOKENS_VALUE));
    }

    private static OpenShiftAiModel assertCommonModelFields(Model model, boolean modelIncludesSecrets) {
        assertThat(model, instanceOf(OpenShiftAiModel.class));

        var openShiftAiModel = (OpenShiftAiModel) model;
        assertThat(openShiftAiModel.getServiceSettings().modelId(), is(MODEL_VALUE));
        assertThat(openShiftAiModel.getServiceSettings().uri.toString(), is(URL_VALUE));
        assertThat(openShiftAiModel.getTaskSettings(), is(EmptyTaskSettings.INSTANCE));

        if (modelIncludesSecrets) {
            assertThat(openShiftAiModel.getSecretSettings().apiKey(), is(new SecureString(API_KEY_VALUE.toCharArray())));
        }

        return openShiftAiModel;
    }

    private static void assertCompletionModel(Model model, boolean modelIncludesSecrets) {
        var openShiftAiModel = assertCommonModelFields(model, modelIncludesSecrets);
        assertThat(openShiftAiModel.getTaskType(), is(TaskType.COMPLETION));
    }

    private static void assertChatCompletionModel(Model model, boolean modelIncludesSecrets) {
        var openShiftAiModel = assertCommonModelFields(model, modelIncludesSecrets);
        assertThat(openShiftAiModel.getTaskType(), is(TaskType.CHAT_COMPLETION));
    }

    public static SenderService createService(ThreadPool threadPool, HttpClientManager clientManager) {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        return new OpenShiftAiService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty());
    }

    private static Map<String, Object> createServiceSettingsMap(TaskType taskType) {
        Map<String, Object> settingsMap = new HashMap<>(Map.of(ServiceFields.URL, URL_VALUE, ServiceFields.MODEL_ID, MODEL_VALUE));

        if (taskType == TaskType.TEXT_EMBEDDING) {
            settingsMap.putAll(
                Map.of(
                    ServiceFields.SIMILARITY,
                    SimilarityMeasure.COSINE.toString(),
                    ServiceFields.DIMENSIONS,
                    DIMENSIONS_VALUE,
                    ServiceFields.MAX_INPUT_TOKENS,
                    MAX_INPUT_TOKENS_VALUE
                )
            );
        }

        return settingsMap;
    }

    private static Map<String, Object> createSecretSettingsMap() {
        return new HashMap<>(Map.of(API_KEY_FIELD_NAME, API_KEY_VALUE));
    }

    private static OpenShiftAiEmbeddingsModel createInternalEmbeddingModel(@Nullable SimilarityMeasure similarityMeasure) {
        return new OpenShiftAiEmbeddingsModel(
            INFERENCE_ID_VALUE,
            TaskType.TEXT_EMBEDDING,
            OpenShiftAiService.NAME,
            new OpenShiftAiEmbeddingsServiceSettings(
                MODEL_VALUE,
                URL_VALUE,
                DIMENSIONS_VALUE,
                similarityMeasure,
                MAX_INPUT_TOKENS_VALUE,
                new RateLimitSettings(10_000),
                true
            ),
            createRandomChunkingSettings(),
            new DefaultSecretSettings(new SecureString(API_KEY_VALUE.toCharArray()))
        );
    }

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

    public void testParseRequestConfig_CreatesAnEmbeddingsModelWhenChunkingSettingsProvided() throws IOException {
        var chunkingSettings = createRandomChunkingSettings();
        try (var service = createService()) {
            ActionListener<Model> modelVerificationActionListener = ActionListener.wrap(model -> {
                assertThat(model, instanceOf(OpenShiftAiEmbeddingsModel.class));

                var embeddingsModel = (OpenShiftAiEmbeddingsModel) model;
                assertThat(embeddingsModel.getServiceSettings().uri().toString(), is(URL_VALUE));
                assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
                assertThat(embeddingsModel.getConfigurations().getChunkingSettings().asMap(), is(chunkingSettings.asMap()));
                assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is(API_KEY_VALUE));
            }, e -> fail("parse request should not fail " + e.getMessage()));

            service.parseRequestConfig(
                INFERENCE_ID_VALUE,
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    getServiceSettingsMap(MODEL_VALUE, URL_VALUE),
                    chunkingSettings.asMap(),
                    getSecretSettingsMap(API_KEY_VALUE)
                ),
                modelVerificationActionListener
            );
        }
    }

    public void testParseRequestConfig_CreatesAnEmbeddingsModelWhenChunkingSettingsNotProvided() throws IOException {
        try (var service = createService()) {
            ActionListener<Model> modelVerificationActionListener = ActionListener.wrap(model -> {
                assertThat(model, instanceOf(OpenShiftAiEmbeddingsModel.class));

                var embeddingsModel = (OpenShiftAiEmbeddingsModel) model;
                assertThat(embeddingsModel.getServiceSettings().uri().toString(), is(URL_VALUE));
                assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), is(ChunkingSettingsBuilder.DEFAULT_SETTINGS));
                assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is(API_KEY_VALUE));
            }, e -> fail("parse request should not fail " + e.getMessage()));

            service.parseRequestConfig(
                INFERENCE_ID_VALUE,
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(getServiceSettingsMap(MODEL_VALUE, URL_VALUE), getSecretSettingsMap(API_KEY_VALUE)),
                modelVerificationActionListener
            );
        }
    }

    public void testParseRequestConfig_WithoutModelId_Success() throws IOException {
        try (var service = createService()) {
            ActionListener<Model> modelVerificationListener = ActionListener.wrap(m -> {
                assertThat(m, instanceOf(OpenShiftAiChatCompletionModel.class));

                var chatCompletionModel = (OpenShiftAiChatCompletionModel) m;

                assertThat(chatCompletionModel.getServiceSettings().uri().toString(), is(URL_VALUE));
                assertThat(chatCompletionModel.getServiceSettings().modelId(), is(nullValue()));
                assertThat(chatCompletionModel.getSecretSettings().apiKey().toString(), is(API_KEY_VALUE));

            }, e -> fail("parse request should not fail " + e.getMessage()));

            service.parseRequestConfig(
                INFERENCE_ID_VALUE,
                TaskType.CHAT_COMPLETION,
                getRequestConfigMap(getServiceSettingsMap(null, URL_VALUE), getSecretSettingsMap(API_KEY_VALUE)),
                modelVerificationListener
            );
        }
    }

    public void testParseRequestConfig_WithoutUrl_ThrowsException() throws IOException {
        try (var service = createService()) {
            ActionListener<Model> modelVerificationListener = ActionListener.wrap(
                m -> fail("Expected exception, but got model: " + m),
                exception -> {
                    assertThat(exception, instanceOf(ValidationException.class));
                    assertThat(
                        exception.getMessage(),
                        is("Validation Failed: 1: [service_settings] does not contain the required setting [url];")
                    );
                }
            );

            service.parseRequestConfig(
                INFERENCE_ID_VALUE,
                TaskType.CHAT_COMPLETION,
                getRequestConfigMap(getServiceSettingsMap(MODEL_VALUE, null), getSecretSettingsMap(API_KEY_VALUE)),
                modelVerificationListener
            );
        }
    }

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
        try (var service = new OpenShiftAiService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            var model = createChatCompletionModel(getUrl(webServer), API_KEY_VALUE, MODEL_VALUE);
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.unifiedCompletionInfer(
                model,
                UnifiedCompletionRequest.of(
                    List.of(
                        new UnifiedCompletionRequest.Message(new UnifiedCompletionRequest.ContentString("hello"), ROLE_VALUE, null, null)
                    )
                ),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);
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
        try (var service = new OpenShiftAiService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            var model = OpenShiftAiChatCompletionModelTests.createChatCompletionModel(getUrl(webServer), API_KEY_VALUE, MODEL_VALUE);
            var latch = new CountDownLatch(1);
            service.unifiedCompletionInfer(
                model,
                UnifiedCompletionRequest.of(
                    List.of(
                        new UnifiedCompletionRequest.Message(new UnifiedCompletionRequest.ContentString("hello"), ROLE_VALUE, null, null)
                    )
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
                        assertThat(json, is(Strings.format(XContentHelper.stripWhitespace("""
                            {
                              "error" : {
                                "code" : "not_found",
                                "message" : "Resource not found at [%s] for request from inference entity id [inferenceEntityId] status \
                            [404]. Error message: [{\\n    \\"detail\\": \\"Not Found\\"\\n}\\n]",
                                "type" : "openshift_ai_error"
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
            data: {"error": {"message": "400: Invalid value: Model 'llama3.12:3b' not found"}}

            """;
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));
        testStreamError(XContentHelper.stripWhitespace("""
            {
                  "error": {
                      "message": "Received an error response for request from inference entity id [inferenceEntityId].\
             Error message: [{\\"error\\": {\\"message\\": \\"400: Invalid value: Model 'llama3.12:3b' not found\\"}}]",
                      "type": "openshift_ai_error"
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
                "model": "llama-31-8b-instruct",\
                "choices": [{\
                        "index": 0,\
                        "delta": {\
                            "role": "assistant",\
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
        try (var service = new OpenShiftAiService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            var model = OpenShiftAiChatCompletionModelTests.createChatCompletionModel(getUrl(webServer), API_KEY_VALUE, MODEL_VALUE);
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.unifiedCompletionInfer(
                model,
                UnifiedCompletionRequest.of(
                    List.of(
                        new UnifiedCompletionRequest.Message(new UnifiedCompletionRequest.ContentString("hello"), ROLE_VALUE, null, null)
                    )
                ),
                InferenceAction.Request.DEFAULT_TIMEOUT,
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
            {
                "detail": "Not Found"
            }""";
        webServer.enqueue(new MockResponse().setResponseCode(404).setBody(responseJson));

        var e = assertThrows(ElasticsearchStatusException.class, this::streamCompletion);
        assertThat(e.status(), equalTo(RestStatus.NOT_FOUND));
        assertThat(e.getMessage(), equalTo(Strings.format("""
            Resource not found at [%s] for request from inference entity id [inferenceEntityId] status [404]. Error message: [{
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
                "id": "chatcmpl-2c57e3888b1a4e80a0c708889546288e",\
                "object": "chat.completion.chunk",\
                "created": 1760082951,\
                "model": "llama-31-8b-instruct",\
                "choices": [{\
                        "index": 0,\
                        "delta": {\
                            "role": "assistant",\
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

    public void testSupportsStreaming() throws IOException {
        try (var service = new OpenShiftAiService(mock(), createWithEmptySettings(mock()), mockClusterServiceEmpty())) {
            assertThat(service.supportedStreamingTasks(), is(EnumSet.of(TaskType.COMPLETION, TaskType.CHAT_COMPLETION)));
            assertThat(service.canStream(TaskType.ANY), is(false));
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInEmbeddingSecretSettingsMap() throws IOException {
        try (var service = createService()) {
            var secretSettings = getSecretSettingsMap(API_KEY_VALUE);
            secretSettings.put("extra_key", "value");

            var config = getRequestConfigMap(getEmbeddingsServiceSettingsMap(), secretSettings);

            ActionListener<Model> modelVerificationListener = ActionListener.wrap(
                model -> fail("Expected exception, but got model: " + model),
                exception -> {
                    assertThat(exception, instanceOf(ElasticsearchStatusException.class));
                    assertThat(
                        exception.getMessage(),
                        is("Configuration contains settings [{extra_key=value}] unknown to the [openshift_ai] service")
                    );
                }
            );

            service.parseRequestConfig(INFERENCE_ID_VALUE, TaskType.TEXT_EMBEDDING, config, modelVerificationListener);
        }
    }

    public void testChunkedInfer_ChunkingSettingsNotSet() throws IOException {
        var model = OpenShiftAiEmbeddingsModelTests.createModel(
            getUrl(webServer),
            API_KEY_VALUE,
            MODEL_VALUE,
            1234,
            false,
            DIMENSIONS_VALUE,
            null
        );

        testChunkedInfer(model);
    }

    public void testChunkedInfer_ChunkingSettingsSet() throws IOException {
        var model = OpenShiftAiEmbeddingsModelTests.createModel(
            getUrl(webServer),
            API_KEY_VALUE,
            MODEL_VALUE,
            1234,
            false,
            DIMENSIONS_VALUE,
            createRandomChunkingSettings()
        );

        testChunkedInfer(model);
    }

    public void testChunkedInfer(OpenShiftAiEmbeddingsModel model) throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new OpenShiftAiService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {

            String responseJson = """
                {
                    "id": "embd-45e6d99b97a645c0af96653598069cd9",
                    "object": "list",
                    "created": 1760085467,
                    "model": "gritlm-7b",
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
                    "usage": {
                        "prompt_tokens": 7,
                        "total_tokens": 7,
                        "completion_tokens": 0,
                        "prompt_tokens_details": null
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
                InputType.INTERNAL_INGEST,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var results = listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);

            assertThat(results, hasSize(2));
            {
                assertThat(results.getFirst(), Matchers.instanceOf(ChunkedInferenceEmbedding.class));
                var floatResult = (ChunkedInferenceEmbedding) results.getFirst();
                assertThat(floatResult.chunks(), hasSize(1));
                assertThat(floatResult.chunks().getFirst().embedding(), Matchers.instanceOf(DenseEmbeddingFloatResults.Embedding.class));
                assertThat(
                    Arrays.equals(
                        new float[] { 0.0089111328125f, -0.007049560546875f },
                        ((DenseEmbeddingFloatResults.Embedding) floatResult.chunks().getFirst().embedding()).values()
                    ),
                    is(true)
                );
            }
            {
                assertThat(results.get(1), Matchers.instanceOf(ChunkedInferenceEmbedding.class));
                var floatResult = (ChunkedInferenceEmbedding) results.get(1);
                assertThat(floatResult.chunks(), hasSize(1));
                assertThat(floatResult.chunks().getFirst().embedding(), Matchers.instanceOf(DenseEmbeddingFloatResults.Embedding.class));
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
                equalTo(XContentType.JSON.mediaTypeWithoutParameters())
            );
            assertThat(
                webServer.requests().getFirst().getHeader(HttpHeaders.AUTHORIZATION),
                is(Strings.format("Bearer %s", API_KEY_VALUE))
            );

            var requestMap = entityAsMap(webServer.requests().getFirst().getBody());
            assertThat(requestMap, aMapWithSize(2));
            assertThat(requestMap.get(INPUT_FIELD_NAME), is(List.of(FIRST_PART_OF_INPUT_VALUE, SECOND_PART_OF_INPUT_VALUE)));
            assertThat(requestMap.get(MODEL_FIELD_NAME), is(MODEL_VALUE));
        }
    }

    public void testGetConfiguration() throws Exception {
        try (var service = createService()) {
            String content = XContentHelper.stripWhitespace("""
                {
                       "service": "openshift_ai",
                       "name": "OpenShift AI",
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
                               "description": "The name of the model to use for the inference task.",
                               "label": "Model ID",
                               "required": false,
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
                               "required": true,
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
        try (var service = new OpenShiftAiService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            var model = OpenShiftAiChatCompletionModelTests.createCompletionModel(getUrl(webServer), API_KEY_VALUE, MODEL_VALUE);
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
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            return InferenceEventsAssertion.assertThat(listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT)).hasFinishedStream();
        }
    }

    private OpenShiftAiService createService() {
        return new OpenShiftAiService(
            mock(HttpRequestSender.Factory.class),
            createWithEmptySettings(threadPool),
            mockClusterServiceEmpty()
        );
    }

    private Map<String, Object> getRequestConfigMap(
        Map<String, Object> serviceSettings,
        Map<String, Object> chunkingSettings,
        Map<String, Object> secretSettings
    ) {
        var requestConfigMap = getRequestConfigMap(serviceSettings, secretSettings);
        requestConfigMap.put(ModelConfigurations.CHUNKING_SETTINGS, chunkingSettings);

        return requestConfigMap;
    }

    private Map<String, Object> getRequestConfigMap(Map<String, Object> serviceSettings, Map<String, Object> secretSettings) {
        var builtServiceSettings = new HashMap<>();
        builtServiceSettings.putAll(serviceSettings);
        builtServiceSettings.putAll(secretSettings);

        return new HashMap<>(Map.of(ModelConfigurations.SERVICE_SETTINGS, builtServiceSettings));
    }

    private static Map<String, Object> getEmbeddingsServiceSettingsMap() {
        return buildServiceSettingsMap(INFERENCE_ID_VALUE, URL_VALUE, SimilarityMeasure.COSINE.toString(), null, null, null);
    }

    @Override
    public InferenceService createInferenceService() {
        return createService();
    }

    @Override
    protected void assertRerankerWindowSize(RerankingInferenceService rerankingInferenceService) {
        assertThat(rerankingInferenceService.rerankerWindowSize(MODEL_VALUE), is(2800));
    }

}
