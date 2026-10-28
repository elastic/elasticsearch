/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.fireworksai;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.services.AbstractInferenceServiceParameterizedTests;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.fireworksai.completion.FireworksAiChatCompletionModel;
import org.elasticsearch.xpack.inference.services.fireworksai.completion.FireworksAiChatCompletionServiceSettings;
import org.elasticsearch.xpack.inference.services.fireworksai.completion.FireworksAiChatCompletionTaskSettings;
import org.elasticsearch.xpack.inference.services.fireworksai.embeddings.FireworksAiEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.fireworksai.embeddings.FireworksAiEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.openai.OpenAiServiceFields;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;
import org.hamcrest.core.Is;

import java.net.URI;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.test.ESTestCase.assertThat;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettingsTests.getSecretSettingsMap;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class FireworksAiServiceParameterizedTestConfiguration {
    private static final String MODEL_ID_VALUE = "model";
    private static final SimilarityMeasure SIMILARITY_VALUE = SimilarityMeasure.DOT_PRODUCT;
    private static final int DIMENSIONS_VALUE = 100;
    private static final String API_KEY_VALUE = "secret";
    private static final String INFERENCE_ID = "id";
    private static final String DEFAULT_EMBEDDING_URL = "https://api.fireworks.ai/inference/v1/embeddings";
    private static final String DEFAULT_COMPLETION_URL = "https://api.fireworks.ai/inference/v1/chat/completions";
    private static final String URL_VALUE = "https://www.abc.com";
    private static final int REQUESTS_PER_MINUTE = 123;
    private static final int MAX_INPUT_TOKENS_VALUE = 200;
    private static final String USER_VALUE = "some_user";
    private static final HashMap<String, String> HEADERS_MAP = new HashMap<>(Map.of("header_key", "header_value"));

    public static AbstractInferenceServiceParameterizedTests.TestConfiguration createTestConfiguration() {
        return new AbstractInferenceServiceParameterizedTests.TestConfiguration(
            EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.COMPLETION, TaskType.CHAT_COMPLETION),
            FireworksAiService.NAME
        ) {
            @Override
            protected FireworksAiService createService(ThreadPool threadPool, HttpClientManager clientManager) {
                var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
                return new FireworksAiService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty());
            }

            @Override
            protected Map<String, Object> createMinimalServiceSettingsMap(TaskType taskType) {
                return createMinimalServiceSettingsMap(taskType, ConfigurationParseContext.REQUEST);
            }

            @Override
            protected Map<String, Object> createAllServiceSettingsMap(TaskType taskType, ConfigurationParseContext parseContext) {
                var settingsMap = createMinimalServiceSettingsMap(taskType);
                settingsMap.put(URL, URL_VALUE);
                RateLimitSettingsTests.addRateLimitSettingsToMap(settingsMap, REQUESTS_PER_MINUTE);

                if (taskType == TaskType.TEXT_EMBEDDING) {
                    settingsMap.putAll(
                        Map.of(
                            SIMILARITY,
                            SIMILARITY_VALUE.toString(),
                            DIMENSIONS,
                            DIMENSIONS_VALUE,
                            MAX_INPUT_TOKENS,
                            MAX_INPUT_TOKENS_VALUE
                        )
                    );

                    if (parseContext == ConfigurationParseContext.PERSISTENT) {
                        settingsMap.put(FireworksAiEmbeddingsServiceSettings.DIMENSIONS_SET_BY_USER, true);
                    }
                }
                return settingsMap;
            }

            @Override
            protected ServiceSettings getServiceSettings(
                Map<String, Object> serviceSettings,
                TaskType taskType,
                ConfigurationParseContext context
            ) {
                return switch (taskType) {
                    case TEXT_EMBEDDING -> FireworksAiEmbeddingsServiceSettings.fromMap(serviceSettings, context);
                    case COMPLETION, CHAT_COMPLETION -> FireworksAiChatCompletionServiceSettings.fromMap(serviceSettings, context);
                    default -> throw new IllegalStateException("Unexpected value: " + taskType);
                };
            }

            @Override
            protected TaskSettings getEmptyTaskSettings(TaskType taskType) {
                return switch (taskType) {
                    case TEXT_EMBEDDING -> EmptyTaskSettings.INSTANCE;
                    case COMPLETION, CHAT_COMPLETION -> new FireworksAiChatCompletionTaskSettings(Map.of());
                    default -> throw new IllegalStateException("Unexpected value: " + taskType);
                };
            }

            @Override
            protected ModelSecrets createModelSecrets(ConfigurationParseContext context) {
                return new ModelSecrets(DefaultSecretSettings.fromMap(createSecretSettingsMap(), context));
            }

            @Override
            protected Map<String, Object> createMinimalServiceSettingsMap(TaskType taskType, ConfigurationParseContext parseContext) {
                return new HashMap<>(Map.of(ServiceFields.MODEL_ID, MODEL_ID_VALUE));
            }

            @Override
            protected Map<String, Object> createAllTaskSettingsMap(TaskType taskType) {
                var taskSettings = new HashMap<String, Object>();
                if (taskType == TaskType.COMPLETION || taskType == TaskType.CHAT_COMPLETION) {
                    taskSettings.put(OpenAiServiceFields.USER, USER_VALUE);
                    taskSettings.put(OpenAiServiceFields.HEADERS, HEADERS_MAP);
                }
                return taskSettings;
            }

            @Override
            protected Map<String, Object> createSecretSettingsMap() {
                return getSecretSettingsMap(API_KEY_VALUE);
            }

            @Override
            protected void assertModel(Model model, TaskType taskType, boolean modelIncludesSecrets, boolean minimalSettings) {
                switch (taskType) {
                    case TEXT_EMBEDDING -> assertEmbeddingsModel(model, modelIncludesSecrets, minimalSettings);
                    case COMPLETION, CHAT_COMPLETION -> assertChatCompletionModel(model, modelIncludesSecrets, minimalSettings);
                    default -> throw new IllegalStateException("Unexpected value: " + taskType);
                }
            }

            @Override
            protected EnumSet<TaskType> supportedStreamingTasks() {
                return EnumSet.of(TaskType.COMPLETION, TaskType.CHAT_COMPLETION);
            }
        };
    }

    private static void assertEmbeddingsModel(Model model, boolean modelIncludesSecrets, boolean minimalSettings) {
        assertThat(model, instanceOf(FireworksAiEmbeddingsModel.class));
        var embeddingsModel = (FireworksAiEmbeddingsModel) model;

        assertThat(embeddingsModel.getServiceSettings().modelId(), is(MODEL_ID_VALUE));
        assertThat(embeddingsModel.getTaskSettings(), is(EmptyTaskSettings.INSTANCE));
        if (minimalSettings) {
            // Check default values
            assertThat(embeddingsModel.getServiceSettings().uri(), is(URI.create(DEFAULT_EMBEDDING_URL)));
            assertThat(embeddingsModel.getServiceSettings().rateLimitSettings(), is(new RateLimitSettings(6000)));
            assertThat(embeddingsModel.getServiceSettings().similarity(), nullValue());
            assertThat(embeddingsModel.getServiceSettings().dimensions(), nullValue());
            assertThat(embeddingsModel.getServiceSettings().dimensionsSetByUser(), is(false));
            assertThat(embeddingsModel.getServiceSettings().maxInputTokens(), nullValue());
        } else {
            // Check configured values
            assertThat(embeddingsModel.getServiceSettings().uri(), is(URI.create(URL_VALUE)));
            assertThat(embeddingsModel.getServiceSettings().rateLimitSettings(), is(new RateLimitSettings(REQUESTS_PER_MINUTE)));
            assertThat(embeddingsModel.getServiceSettings().similarity(), is(SIMILARITY_VALUE));
            assertThat(embeddingsModel.getServiceSettings().dimensions(), is(DIMENSIONS_VALUE));
            assertThat(embeddingsModel.getServiceSettings().dimensionsSetByUser(), is(true));
            assertThat(embeddingsModel.getServiceSettings().maxInputTokens(), is(MAX_INPUT_TOKENS_VALUE));
        }

        if (modelIncludesSecrets) {
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is(API_KEY_VALUE));
        } else {
            assertThat(embeddingsModel.getSecretSettings(), nullValue());
        }
    }

    private static void assertChatCompletionModel(Model model, boolean modelIncludesSecrets, boolean minimalSettings) {
        assertThat(model, instanceOf(FireworksAiChatCompletionModel.class));

        var completionModel = (FireworksAiChatCompletionModel) model;

        assertThat(completionModel.getServiceSettings().modelId(), is(MODEL_ID_VALUE));
        assertThat(completionModel.getServiceSettings().similarity(), nullValue());
        assertThat(completionModel.getServiceSettings().dimensions(), nullValue());
        assertThat(completionModel.getServiceSettings().dimensionsSetByUser(), nullValue());

        if (minimalSettings) {
            // Check default values
            assertThat(completionModel.uri(), is(URI.create(DEFAULT_COMPLETION_URL)));
            assertThat(completionModel.getServiceSettings().rateLimitSettings(), is(new RateLimitSettings(6000)));
            assertThat(completionModel.getTaskSettings().isEmpty(), is(true));
        } else {
            // Check configured values
            assertThat(completionModel.uri(), is(URI.create(URL_VALUE)));
            assertThat(completionModel.getServiceSettings().rateLimitSettings(), is(new RateLimitSettings(REQUESTS_PER_MINUTE)));
            assertThat(completionModel.getTaskSettings().user(), is(USER_VALUE));
            assertThat(completionModel.getTaskSettings().headers(), is(HEADERS_MAP));
        }

        if (modelIncludesSecrets) {
            assertThat(completionModel.getSecretSettings().apiKey().toString(), Is.is(API_KEY_VALUE));
        } else {
            assertThat(completionModel.getSecretSettings(), nullValue());
        }
    }

    static FireworksAiEmbeddingsModel createInternalEmbeddingModel(
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

    static FireworksAiChatCompletionModel createInternalChatCompletionModel(String url, String apiKey, String modelId) {
        return new FireworksAiChatCompletionModel(
            INFERENCE_ID,
            TaskType.CHAT_COMPLETION,
            FireworksAiService.NAME,
            new FireworksAiChatCompletionServiceSettings(modelId, URI.create(url), null),
            new FireworksAiChatCompletionTaskSettings((String) null, null),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

}
