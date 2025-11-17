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
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsTests;
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
import org.elasticsearch.xpack.inference.services.nvidia.completion.NvidiaChatCompletionModel;
import org.elasticsearch.xpack.inference.services.nvidia.completion.NvidiaChatCompletionModelTests;
import org.elasticsearch.xpack.inference.services.nvidia.embeddings.NvidiaEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.nvidia.embeddings.NvidiaEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.nvidia.embeddings.NvidiaEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.nvidia.embeddings.NvidiaEmbeddingsTaskSettings;
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
import java.util.Locale;
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
import static org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsTests.createRandomChunkingSettingsMap;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.nvidia.completion.NvidiaChatCompletionModelTests.createChatCompletionModel;
import static org.elasticsearch.xpack.inference.services.nvidia.embeddings.NvidiaEmbeddingsServiceSettingsTests.buildServiceSettingsMap;
import static org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettingsTests.getSecretSettingsMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.mockito.Mockito.mock;

public class NvidiaServiceTests extends AbstractInferenceServiceTests {
    private final MockWebServer webServer = new MockWebServer();
    private ThreadPool threadPool;
    private HttpClientManager clientManager;

    public NvidiaServiceTests() {
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
                    return NvidiaServiceTests.createService(threadPool, clientManager);
                }

                @Override
                protected Map<String, Object> createServiceSettingsMap(TaskType taskType) {
                    return NvidiaServiceTests.createServiceSettingsMap(taskType);
                }

                @Override
                protected Map<String, Object> createTaskSettingsMap() {
                    return new HashMap<>();
                }

                @Override
                protected Map<String, Object> createSecretSettingsMap() {
                    return NvidiaServiceTests.createSecretSettingsMap();
                }

                @Override
                protected void assertModel(Model model, TaskType taskType, boolean modelIncludesSecrets) {
                    NvidiaServiceTests.assertModel(model, taskType, modelIncludesSecrets);
                }

                @Override
                protected EnumSet<TaskType> supportedStreamingTasks() {
                    return EnumSet.of(TaskType.CHAT_COMPLETION, TaskType.COMPLETION);
                }
            }
        ).enableUpdateModelTests(new UpdateModelConfiguration() {
            @Override
            protected NvidiaEmbeddingsModel createEmbeddingModel(SimilarityMeasure similarityMeasure) {
                return createInternalEmbeddingModel(similarityMeasure);
            }
        }).build();
    }

    private static void assertModel(Model model, TaskType taskType, boolean modelIncludesSecrets) {
        switch (taskType) {
            case TEXT_EMBEDDING -> assertTextEmbeddingModel(model, modelIncludesSecrets);
            case COMPLETION -> assertCompletionModel(model, modelIncludesSecrets);
            case CHAT_COMPLETION -> assertChatCompletionModel(model, modelIncludesSecrets);
            default -> fail("unexpected task type [" + taskType + "]");
        }
    }

    private static NvidiaModel assertCommonModelFields(Model model, boolean modelIncludesSecrets) {
        assertThat(model, instanceOf(NvidiaModel.class));

        var nvidiaModel = (NvidiaModel) model;
        assertThat(nvidiaModel.getServiceSettings().modelId(), is("model_id"));
        assertThat(nvidiaModel.getServiceSettings().uri().toString(), Matchers.is("http://www.abc.com"));

        if (modelIncludesSecrets) {
            assertThat(nvidiaModel.getSecretSettings().apiKey(), Matchers.is(new SecureString("secret".toCharArray())));
        }

        return nvidiaModel;
    }

    private static void assertTextEmbeddingModel(Model model, boolean modelIncludesSecrets) {
        var nvidiaModel = assertCommonModelFields(model, modelIncludesSecrets);
        assertThat(nvidiaModel.getTaskSettings(), Matchers.is(NvidiaEmbeddingsTaskSettings.EMPTY_SETTINGS));
        assertThat(nvidiaModel.getTaskType(), Matchers.is(TaskType.TEXT_EMBEDDING));
    }

    private static void assertCompletionModel(Model model, boolean modelIncludesSecrets) {
        var nvidiaModel = assertCommonModelFields(model, modelIncludesSecrets);
        assertThat(nvidiaModel.getTaskSettings(), Matchers.is(EmptyTaskSettings.INSTANCE));
        assertThat(nvidiaModel.getTaskType(), Matchers.is(TaskType.COMPLETION));
    }

    private static void assertChatCompletionModel(Model model, boolean modelIncludesSecrets) {
        var nvidiaModel = assertCommonModelFields(model, modelIncludesSecrets);
        assertThat(nvidiaModel.getTaskSettings(), Matchers.is(EmptyTaskSettings.INSTANCE));
        assertThat(nvidiaModel.getTaskType(), Matchers.is(TaskType.CHAT_COMPLETION));
    }

    public static SenderService createService(ThreadPool threadPool, HttpClientManager clientManager) {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        return new NvidiaService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty());
    }

    private static Map<String, Object> createServiceSettingsMap(TaskType taskType) {
        Map<String, Object> settingsMap = new HashMap<>(
            Map.of(ServiceFields.URL, "http://www.abc.com", ServiceFields.MODEL_ID, "model_id")
        );

        if (taskType == TaskType.TEXT_EMBEDDING) {
            settingsMap.putAll(
                Map.of(
                    ServiceFields.SIMILARITY,
                    SimilarityMeasure.COSINE.toString(),
                    ServiceFields.DIMENSIONS,
                    1536,
                    ServiceFields.MAX_INPUT_TOKENS,
                    512
                )
            );
        }

        return settingsMap;
    }

    private static Map<String, Object> createSecretSettingsMap() {
        return new HashMap<>(Map.of("api_key", "secret"));
    }

    private static NvidiaEmbeddingsModel createInternalEmbeddingModel(@Nullable SimilarityMeasure similarityMeasure) {
        var inferenceId = "inference_id";

        return new NvidiaEmbeddingsModel(
            inferenceId,
            TaskType.TEXT_EMBEDDING,
            NvidiaService.NAME,
            new NvidiaEmbeddingsServiceSettings(
                "model_id",
                "http://www.abc.com",
                1536,
                similarityMeasure,
                512,
                new RateLimitSettings(10_000)
            ),
            NvidiaEmbeddingsTaskSettings.EMPTY_SETTINGS,
            ChunkingSettingsTests.createRandomChunkingSettings(),
            new DefaultSecretSettings(new SecureString("secret".toCharArray()))
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
        try (var service = createService()) {
            ActionListener<Model> modelVerificationActionListener = ActionListener.wrap(model -> {
                assertThat(model, instanceOf(NvidiaEmbeddingsModel.class));

                var embeddingsModel = (NvidiaEmbeddingsModel) model;
                assertThat(embeddingsModel.getServiceSettings().uri().toString(), is("http://www.abc.com"));
                assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
                assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
            }, e -> fail("parse request should not fail " + e.getMessage()));

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    createServiceSettingsMap(TEXT_EMBEDDING),
                    createRandomChunkingSettingsMap(),
                    getSecretSettingsMap("secret")
                ),
                modelVerificationActionListener
            );
        }
    }

    public void testParseRequestConfig_CreatesAnEmbeddingsModelWhenChunkingSettingsNotProvided() throws IOException {
        try (var service = createService()) {
            ActionListener<Model> modelVerificationActionListener = ActionListener.wrap(model -> {
                assertThat(model, instanceOf(NvidiaEmbeddingsModel.class));

                var embeddingsModel = (NvidiaEmbeddingsModel) model;
                assertThat(embeddingsModel.getServiceSettings().uri().toString(), is("http://www.abc.com"));
                assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
                assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
            }, e -> fail("parse request should not fail " + e.getMessage()));

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(createServiceSettingsMap(TEXT_EMBEDDING), null, getSecretSettingsMap("secret")),
                modelVerificationActionListener
            );
        }
    }

    public void testParseRequestConfig_WithoutModelId_ThrowsException() throws IOException {
        var url = "url";
        var secret = "secret";

        try (var service = createService()) {
            ActionListener<Model> modelVerificationListener = ActionListener.wrap(m -> {
                assertThat(m, instanceOf(NvidiaChatCompletionModel.class));

                var chatCompletionModel = (NvidiaChatCompletionModel) m;

                assertThat(chatCompletionModel.getServiceSettings().uri().toString(), is(url));
                assertNull(chatCompletionModel.getServiceSettings().modelId());
                assertThat(chatCompletionModel.getSecretSettings().apiKey().toString(), is("secret"));

            }, exception -> {
                assertThat(exception, instanceOf(ValidationException.class));
                assertThat(
                    exception.getMessage(),
                    is("Validation Failed: 1: [service_settings] does not contain the required setting [model_id];")
                );
            });

            service.parseRequestConfig(
                "id",
                TaskType.CHAT_COMPLETION,
                getRequestConfigMap(buildServiceSettingsMap(null, url, null, null, null, null), getSecretSettingsMap(secret)),
                modelVerificationListener
            );
        }
    }

    public void testParseRequestConfig_WithoutUrl_Success() throws IOException {
        var model = "model";
        var secret = "secret";

        try (var service = createService()) {
            ActionListener<Model> modelVerificationListener = ActionListener.wrap(m -> {
                assertThat(m, instanceOf(NvidiaChatCompletionModel.class));

                var chatCompletionModel = (NvidiaChatCompletionModel) m;

                assertThat(chatCompletionModel.getServiceSettings().modelId(), is(model));
                assertThat(chatCompletionModel.getSecretSettings().apiKey().toString(), is("secret"));

            }, exception -> fail("parse request should not fail " + exception.getMessage()));

            service.parseRequestConfig(
                "id",
                TaskType.CHAT_COMPLETION,
                getRequestConfigMap(buildServiceSettingsMap(model, null, null, null, null, null), getSecretSettingsMap(secret)),
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
            var model = createChatCompletionModel(getUrl(webServer), "secret", "model");
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.unifiedCompletionInfer(
                model,
                UnifiedCompletionRequest.of(
                    List.of(new UnifiedCompletionRequest.Message(new UnifiedCompletionRequest.ContentString("hello"), "user", null, null))
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
            var model = createChatCompletionModel(getUrl(webServer), "secret", "model");
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
            var model = createChatCompletionModel(getUrl(webServer), "secret", "model");
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.unifiedCompletionInfer(
                model,
                UnifiedCompletionRequest.of(
                    List.of(new UnifiedCompletionRequest.Message(new UnifiedCompletionRequest.ContentString("hello"), "user", null, null))
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
            404 page not found""";
        webServer.enqueue(new MockResponse().setResponseCode(404).setBody(responseJson));

        var e = assertThrows(ElasticsearchStatusException.class, this::streamCompletion);
        assertThat(e.status(), equalTo(RestStatus.NOT_FOUND));
        assertThat(e.getMessage(), equalTo(String.format(Locale.ROOT, """
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

    public void testSupportsStreaming() throws IOException {
        try (var service = new NvidiaService(mock(), createWithEmptySettings(mock()), mockClusterServiceEmpty())) {
            assertThat(service.supportedStreamingTasks(), is(EnumSet.of(TaskType.COMPLETION, TaskType.CHAT_COMPLETION)));
            assertThat(service.canStream(TaskType.ANY), is(false));
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInEmbeddingSecretSettingsMap() throws IOException {
        try (var service = createService()) {
            var secretSettings = getSecretSettingsMap("secret");
            secretSettings.put("extra_key", "value");

            var config = getRequestConfigMap(getEmbeddingsServiceSettingsMap(), secretSettings);

            ActionListener<Model> modelVerificationListener = ActionListener.wrap(
                model -> fail("Expected exception, but got model: " + model),
                exception -> {
                    assertThat(exception, instanceOf(ElasticsearchStatusException.class));
                    assertThat(
                        exception.getMessage(),
                        is("Configuration contains settings [{extra_key=value}] unknown to the [nvidia] service")
                    );
                }
            );

            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, modelVerificationListener);
        }
    }

    public void testChunkedInfer_ChunkingSettingsNotSet() throws IOException {
        var model = NvidiaEmbeddingsModelTests.createModel(getUrl(webServer), "api_key", "model");

        testChunkedInfer(model);
    }

    public void testChunkedInfer_ChunkingSettingsSet() throws IOException {
        var model = NvidiaEmbeddingsModelTests.createModel(getUrl(webServer), "api_key", "model");

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
                List.of(new ChunkInferenceInput("abc"), new ChunkInferenceInput("def")),
                new HashMap<>(),
                InputType.INTERNAL_INGEST,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var results = listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);

            assertThat(results, hasSize(2));
            {
                assertThat(results.getFirst(), instanceOf(ChunkedInferenceEmbedding.class));
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
                assertThat(results.get(1), instanceOf(ChunkedInferenceEmbedding.class));
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
            assertNull(webServer.requests().getFirst().getUri().getQuery());
            assertThat(
                webServer.requests().getFirst().getHeader(HttpHeaders.CONTENT_TYPE),
                equalTo(XContentType.JSON.mediaTypeWithoutParameters())
            );
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer api_key"));

            var requestMap = entityAsMap(webServer.requests().getFirst().getBody());
            assertThat(requestMap.size(), Matchers.is(4));
            assertThat(requestMap.get("input"), Matchers.is(List.of("abc", "def")));
            assertThat(requestMap.get("model"), Matchers.is("model"));
            assertThat(requestMap.get("input_type"), Matchers.is("passage"));
            assertThat(requestMap.get("truncate"), Matchers.is("none"));
        }
    }

    public void testGetConfiguration() throws Exception {
        try (var service = createService()) {
            String content = XContentHelper.stripWhitespace("""
                {
                       "service": "nvidia",
                       "name": "Nvidia",
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
                               "description": "The name of the model to use for the inference task. Refer to the Nvidia models \
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
            var model = NvidiaChatCompletionModelTests.createCompletionModel(getUrl(webServer), "secret", "model");
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

            return InferenceEventsAssertion.assertThat(listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT)).hasFinishedStream();
        }
    }

    private NvidiaService createService() {
        return new NvidiaService(mock(HttpRequestSender.Factory.class), createWithEmptySettings(threadPool), mockClusterServiceEmpty());
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
        return buildServiceSettingsMap("id", "url", SimilarityMeasure.COSINE.toString(), null, null, null);
    }

    @Override
    public InferenceService createInferenceService() {
        return createService();
    }

    @Override
    protected void assertRerankerWindowSize(RerankingInferenceService rerankingInferenceService) {
        assertThat(rerankingInferenceService.rerankerWindowSize("any model"), is(300));
    }

}
