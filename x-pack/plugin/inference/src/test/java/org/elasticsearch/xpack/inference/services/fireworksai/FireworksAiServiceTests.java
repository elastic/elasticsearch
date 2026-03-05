/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.fireworksai;

import org.apache.http.HttpHeaders;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.services.AbstractInferenceServiceTests;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.SenderService;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.fireworksai.completion.FireworksAiChatCompletionModel;
import org.elasticsearch.xpack.inference.services.fireworksai.completion.FireworksAiChatCompletionServiceSettings;
import org.elasticsearch.xpack.inference.services.fireworksai.completion.FireworksAiChatCompletionTaskSettings;
import org.elasticsearch.xpack.inference.services.fireworksai.embeddings.FireworksAiEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.fireworksai.embeddings.FireworksAiEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResultsTests.buildExpectationFloat;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettingsTests.getSecretSettingsMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;

public class FireworksAiServiceTests extends AbstractInferenceServiceTests {

    private static final String MODEL = "model";
    private static final SimilarityMeasure SIMILARITY = SimilarityMeasure.DOT_PRODUCT;
    private static final int DIMENSIONS = 100;
    private static final String SECRET = "secret";
    private static final String INFERENCE_ID = "id";
    private static final String DEFAULT_URL = "https://api.fireworks.ai/inference/v1/embeddings";

    public FireworksAiServiceTests() {
        super(createTestConfiguration());
    }

    public static TestConfiguration createTestConfiguration() {
        return new TestConfiguration.Builder(
            new CommonConfig(
                TaskType.TEXT_EMBEDDING,
                TaskType.RERANK,
                EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.COMPLETION, TaskType.CHAT_COMPLETION)
            ) {
                @Override
                protected SenderService createService(ThreadPool threadPool, HttpClientManager clientManager) {
                    return FireworksAiServiceTests.createService(threadPool, clientManager);
                }

                @Override
                protected Map<String, Object> createServiceSettingsMap(TaskType taskType) {
                    return createServiceSettingsMap(taskType, ConfigurationParseContext.REQUEST);
                }

                @Override
                protected ModelConfigurations createModelConfigurations(TaskType taskType) {
                    return switch (taskType) {
                        case TEXT_EMBEDDING -> new ModelConfigurations(
                            "some_inference_id",
                            taskType,
                            FireworksAiService.NAME,
                            FireworksAiEmbeddingsServiceSettings.fromMap(
                                createServiceSettingsMap(taskType, ConfigurationParseContext.PERSISTENT),
                                ConfigurationParseContext.PERSISTENT
                            ),
                            EmptyTaskSettings.INSTANCE
                        );
                        case COMPLETION, CHAT_COMPLETION -> new ModelConfigurations(
                            "some_inference_id",
                            taskType,
                            FireworksAiService.NAME,
                            FireworksAiChatCompletionServiceSettings.fromMap(
                                createServiceSettingsMap(taskType, ConfigurationParseContext.PERSISTENT),
                                ConfigurationParseContext.PERSISTENT
                            ),
                            new FireworksAiChatCompletionTaskSettings((String) null, null)
                        );
                        case RERANK -> new ModelConfigurations(
                            "some_inference_id",
                            taskType,
                            FireworksAiService.NAME,
                            mock(ServiceSettings.class),
                            EmptyTaskSettings.INSTANCE
                        );
                        default -> throw new IllegalStateException("Unexpected value: " + taskType);
                    };
                }

                @Override
                protected ModelSecrets createModelSecrets() {
                    return new ModelSecrets(DefaultSecretSettings.fromMap(createSecretSettingsMap()));
                }

                @Override
                protected Map<String, Object> createServiceSettingsMap(TaskType taskType, ConfigurationParseContext parseContext) {
                    return FireworksAiServiceTests.createServiceSettingsMap(taskType, parseContext);
                }

                @Override
                protected Map<String, Object> createTaskSettingsMap() {
                    return new HashMap<>();
                }

                @Override
                protected Map<String, Object> createSecretSettingsMap() {
                    return getSecretSettingsMap(SECRET);
                }

                @Override
                protected void assertModel(Model model, TaskType taskType, boolean modelIncludesSecrets) {
                    FireworksAiServiceTests.assertModel(model, taskType, modelIncludesSecrets);
                }

                @Override
                protected EnumSet<TaskType> supportedStreamingTasks() {
                    return EnumSet.of(TaskType.COMPLETION, TaskType.CHAT_COMPLETION);
                }
            }
        ).enableUpdateModelTests(new UpdateModelConfiguration() {
            @Override
            protected Model createEmbeddingModel(@Nullable SimilarityMeasure similarityMeasure) {
                return createInternalEmbeddingModel(similarityMeasure, null);
            }
        }).build();
    }

    @Override
    public void testUpdateModelWithEmbeddingDetails_NullSimilarityInOriginalModel() throws IOException {
        // FireworksAI defaults to COSINE similarity, not DOT_PRODUCT
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var service = new FireworksAiService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            var embeddingSize = randomNonNegativeInt();
            var model = createInternalEmbeddingModel(null, null);

            Model updatedModel = service.updateModelWithEmbeddingDetails(model, embeddingSize);

            assertEquals(SimilarityMeasure.COSINE, updatedModel.getServiceSettings().similarity());
            assertEquals(embeddingSize, updatedModel.getServiceSettings().dimensions().intValue());
        }
    }

    public void testInfer_SendsEmbeddingsRequest() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new FireworksAiService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {

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
                  "model": "nomic-ai/nomic-embed-text-v1.5",
                  "usage": {
                      "prompt_tokens": 8,
                      "total_tokens": 8
                  }
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = createInternalEmbeddingModel(getUrl(webServer), "secret", "model", null, null);
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

            var result = listener.actionGet(TEST_REQUEST_TIMEOUT);

            assertThat(result.asMap(), Matchers.is(buildExpectationFloat(List.of(new float[] { 0.0123F, -0.0123F }))));
            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().get(0).getUri().getQuery());
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.size(), Matchers.is(2));
            assertThat(requestMap.get("input"), Matchers.is(List.of("abc")));
            assertThat(requestMap.get("model"), Matchers.is("model"));
        }
    }

    public void testInfer_SendsChatCompletionRequest() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new FireworksAiService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {

            String responseJson = """
                {
                  "id": "chatcmpl-123",
                  "object": "chat.completion",
                  "created": 1677652288,
                  "model": "accounts/fireworks/models/llama-v3p1-70b-instruct",
                  "choices": [{
                    "index": 0,
                    "message": {
                      "role": "assistant",
                      "content": "Hello! How can I help you?"
                    },
                    "finish_reason": "stop"
                  }],
                  "usage": {
                    "prompt_tokens": 9,
                    "completion_tokens": 12,
                    "total_tokens": 21
                  }
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = createInternalChatCompletionModel(getUrl(webServer), "secret", "test-model");
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                model,
                null,
                null,
                null,
                List.of("Hello"),
                false,
                new HashMap<>(),
                InputType.UNSPECIFIED,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(TEST_REQUEST_TIMEOUT);

            assertThat(webServer.requests(), hasSize(1));
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.get("model"), Matchers.is("test-model"));
        }
    }

    private static Map<String, Object> createServiceSettingsMap(TaskType taskType, ConfigurationParseContext parseContext) {
        var settingsMap = new HashMap<String, Object>(Map.of(ServiceFields.MODEL_ID, MODEL));

        if (taskType == TaskType.TEXT_EMBEDDING) {
            settingsMap.putAll(Map.of(ServiceFields.SIMILARITY, SIMILARITY.toString(), ServiceFields.DIMENSIONS, DIMENSIONS));

            if (parseContext == ConfigurationParseContext.PERSISTENT) {
                settingsMap.put(FireworksAiEmbeddingsServiceSettings.DIMENSIONS_SET_BY_USER, true);
            }
        }

        return settingsMap;
    }

    private static void assertModel(Model model, TaskType taskType, boolean modelIncludesSecrets) {
        if (taskType == TaskType.TEXT_EMBEDDING) {
            assertEmbeddingsModel(model, modelIncludesSecrets);
        } else if (taskType == TaskType.COMPLETION || taskType == TaskType.CHAT_COMPLETION) {
            assertChatCompletionModel(model, modelIncludesSecrets);
        }
    }

    private static void assertEmbeddingsModel(Model model, boolean modelIncludesSecrets) {
        assertThat(model, instanceOf(FireworksAiEmbeddingsModel.class));

        var embeddingsModel = (FireworksAiEmbeddingsModel) model;
        assertThat(
            embeddingsModel.getServiceSettings(),
            is(
                new FireworksAiEmbeddingsServiceSettings(
                    MODEL,
                    URI.create(DEFAULT_URL),
                    SIMILARITY,
                    DIMENSIONS,
                    null,
                    true,
                    FireworksAiEmbeddingsServiceSettings.DEFAULT_RATE_LIMIT_SETTINGS
                )
            )
        );

        assertThat(embeddingsModel.getTaskSettings(), is(EmptyTaskSettings.INSTANCE));

        if (modelIncludesSecrets) {
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is(SECRET));
        } else {
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    private static void assertChatCompletionModel(Model model, boolean modelIncludesSecrets) {
        assertThat(model, instanceOf(FireworksAiChatCompletionModel.class));

        var chatModel = (FireworksAiChatCompletionModel) model;
        assertThat(chatModel.getServiceSettings().modelId(), is(MODEL));

        if (modelIncludesSecrets) {
            assertThat(chatModel.getSecretSettings().apiKey().toString(), is(SECRET));
        } else {
            assertNull(chatModel.getSecretSettings());
        }
    }

    private static FireworksAiEmbeddingsModel createInternalEmbeddingModel(
        @Nullable SimilarityMeasure similarityMeasure,
        @Nullable ChunkingSettings chunkingSettings
    ) {
        return new FireworksAiEmbeddingsModel(
            INFERENCE_ID,
            "service",
            new FireworksAiEmbeddingsServiceSettings(MODEL, URI.create(DEFAULT_URL), similarityMeasure, DIMENSIONS, null, false, null),
            chunkingSettings,
            new DefaultSecretSettings(new SecureString(SECRET.toCharArray()))
        );
    }

    private static FireworksAiEmbeddingsModel createInternalEmbeddingModel(
        String url,
        String apiKey,
        String modelId,
        @Nullable Integer dimensions,
        @Nullable SimilarityMeasure similarity
    ) {
        return new FireworksAiEmbeddingsModel(
            INFERENCE_ID,
            FireworksAiService.NAME,
            new FireworksAiEmbeddingsServiceSettings(modelId, URI.create(url), similarity, dimensions, null, dimensions != null, null),
            null,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    private static FireworksAiChatCompletionModel createInternalChatCompletionModel(String url, String apiKey, String modelId) {
        return new FireworksAiChatCompletionModel(
            INFERENCE_ID,
            TaskType.CHAT_COMPLETION,
            FireworksAiService.NAME,
            new FireworksAiChatCompletionServiceSettings(modelId, URI.create(url), null),
            new FireworksAiChatCompletionTaskSettings((String) null, null),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    private static FireworksAiService createService(ThreadPool threadPool, HttpClientManager clientManager) {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        return new FireworksAiService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty());
    }
}
