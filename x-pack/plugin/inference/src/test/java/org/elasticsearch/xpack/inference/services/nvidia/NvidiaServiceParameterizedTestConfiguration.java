/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.common.model.Truncation;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.services.AbstractInferenceServiceParameterizedTests;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.nvidia.completion.NvidiaChatCompletionServiceSettings;
import org.elasticsearch.xpack.inference.services.nvidia.completion.NvidiaChatCompletionServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.nvidia.embeddings.NvidiaEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.nvidia.embeddings.NvidiaEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.nvidia.embeddings.NvidiaEmbeddingsTaskSettings;
import org.elasticsearch.xpack.inference.services.nvidia.rerank.NvidiaRerankServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;
import org.junit.Assert;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.inference.TaskType.CHAT_COMPLETION;
import static org.elasticsearch.inference.TaskType.COMPLETION;
import static org.elasticsearch.inference.TaskType.RERANK;
import static org.elasticsearch.inference.TaskType.TEXT_EMBEDDING;
import static org.elasticsearch.test.ESTestCase.assertThat;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.nvidia.NvidiaServiceFields.INPUT_TYPE_FIELD_NAME;
import static org.elasticsearch.xpack.inference.services.nvidia.NvidiaServiceFields.TRUNCATE_FIELD_NAME;
import static org.elasticsearch.xpack.inference.services.nvidia.embeddings.NvidiaEmbeddingsServiceSettingsTests.buildServiceSettingsMap;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class NvidiaServiceParameterizedTestConfiguration {
    private static final String API_KEY_FIELD_NAME = "api_key";
    private static final SimilarityMeasure SIMILARITY_MEASURE_VALUE = SimilarityMeasure.COSINE;
    private static final int MAX_INPUT_TOKENS_VALUE = 512;
    private static final InputType INPUT_TYPE_VALUE = InputType.INGEST;
    private static final Truncation TRUNCATION_VALUE = Truncation.END;
    private static final int REQUESTS_PER_MINUTE = 123;
    private static final String DEFAULT_COMPLETION_URL_VALUE = "https://integrate.api.nvidia.com/v1/chat/completions";
    private static final String DEFAULT_EMBEDDINGS_URL_VALUE = "https://integrate.api.nvidia.com/v1/embeddings";
    private static final String DEFAULT_RERANK_URL_VALUE = "https://ai.api.nvidia.com/v1/retrieval/nvidia/reranking";
    static final String URL_VALUE = "http://www.abc.com";
    static final String MODEL_VALUE = "some_model";
    static final String API_KEY_VALUE = "test_api_key";
    static final int DIMENSIONS_VALUE = 1536;

    public static AbstractInferenceServiceParameterizedTests.TestConfiguration createTestConfiguration() {
        return new AbstractInferenceServiceParameterizedTests.TestConfiguration(
            EnumSet.of(TEXT_EMBEDDING, COMPLETION, CHAT_COMPLETION, RERANK),
            NvidiaService.NAME
        ) {

            @Override
            protected NvidiaService createService(ThreadPool threadPool, HttpClientManager clientManager) {
                var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
                return new NvidiaService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty());
            }

            @Override
            protected Map<String, Object> createMinimalServiceSettingsMap(TaskType taskType) {
                return createMinimalServiceSettingsMap(taskType, ConfigurationParseContext.REQUEST);
            }

            @Override
            protected Map<String, Object> createAllServiceSettingsMap(TaskType taskType, ConfigurationParseContext parseContext) {
                var serviceSettings = createMinimalServiceSettingsMap(taskType);
                serviceSettings.put(ServiceFields.URL, URL_VALUE);
                RateLimitSettingsTests.addRateLimitSettingsToMap(serviceSettings, REQUESTS_PER_MINUTE);
                if (taskType == TEXT_EMBEDDING) {
                    if (parseContext == ConfigurationParseContext.PERSISTENT) {
                        serviceSettings.put(DIMENSIONS, DIMENSIONS_VALUE);
                    }
                    serviceSettings.put(SIMILARITY, SIMILARITY_MEASURE_VALUE.toString());
                    serviceSettings.put(MAX_INPUT_TOKENS, MAX_INPUT_TOKENS_VALUE);
                }
                return serviceSettings;
            }

            @Override
            protected ServiceSettings getServiceSettings(
                Map<String, Object> serviceSettings,
                TaskType taskType,
                ConfigurationParseContext context
            ) {
                return switch (taskType) {
                    case TEXT_EMBEDDING -> NvidiaEmbeddingsServiceSettings.fromMap(serviceSettings, context);
                    case COMPLETION, CHAT_COMPLETION -> NvidiaChatCompletionServiceSettings.fromMap(serviceSettings, context);
                    case RERANK -> NvidiaRerankServiceSettings.fromMap(serviceSettings, context);
                    default -> throw new IllegalStateException("Unexpected value: " + taskType);
                };
            }

            @Override
            protected TaskSettings getEmptyTaskSettings(TaskType taskType) {
                return switch (taskType) {
                    case TEXT_EMBEDDING -> NvidiaEmbeddingsTaskSettings.fromMap(null);
                    case COMPLETION, CHAT_COMPLETION, RERANK -> EmptyTaskSettings.INSTANCE;
                    default -> throw new IllegalStateException("Unexpected value: " + taskType);
                };
            }

            @Override
            protected ModelSecrets createModelSecrets(ConfigurationParseContext context) {
                return new ModelSecrets(DefaultSecretSettings.fromMap(createSecretSettingsMap(), context));
            }

            @Override
            protected Map<String, Object> createMinimalServiceSettingsMap(TaskType taskType, ConfigurationParseContext parseContext) {
                var settingsMap = new HashMap<String, Object>(Map.of(ServiceFields.MODEL_ID, MODEL_VALUE));
                if (taskType == TEXT_EMBEDDING && parseContext == ConfigurationParseContext.PERSISTENT) {
                    settingsMap.put(DIMENSIONS, DIMENSIONS_VALUE);
                }
                return settingsMap;
            }

            @Override
            protected Map<String, Object> createAllTaskSettingsMap(TaskType taskType) {
                if (taskType.equals(TEXT_EMBEDDING)) {
                    return new HashMap<>(
                        Map.of(INPUT_TYPE_FIELD_NAME, INPUT_TYPE_VALUE.toString(), TRUNCATE_FIELD_NAME, TRUNCATION_VALUE.toString())
                    );
                } else {
                    return new HashMap<>();
                }
            }

            @Override
            protected Map<String, Object> createSecretSettingsMap() {
                return new HashMap<>(Map.of(API_KEY_FIELD_NAME, API_KEY_VALUE));
            }

            @Override
            protected void assertModel(Model model, TaskType taskType, boolean modelIncludesSecrets, boolean minimalSettings) {
                assertModel(model, taskType, modelIncludesSecrets, true, ConfigurationParseContext.REQUEST);
            }

            @Override
            protected void assertModel(
                Model model,
                TaskType taskType,
                boolean modelIncludesSecrets,
                boolean minimalSettings,
                ConfigurationParseContext parseContext
            ) {
                switch (taskType) {
                    case TEXT_EMBEDDING -> assertTextEmbeddingModel(model, modelIncludesSecrets, minimalSettings, parseContext);
                    case COMPLETION -> assertCompletionModel(model, modelIncludesSecrets, minimalSettings);
                    case CHAT_COMPLETION -> assertChatCompletionModel(model, modelIncludesSecrets, minimalSettings);
                    case RERANK -> assertRerankModel(model, modelIncludesSecrets, minimalSettings);
                    default -> Assert.fail("unexpected task type [" + taskType + "]");
                }
            }

            @Override
            protected EnumSet<TaskType> supportedStreamingTasks() {
                return EnumSet.of(CHAT_COMPLETION, COMPLETION);
            }
        };
    }

    private static NvidiaModel assertCommonModelFields(Model model, boolean modelIncludesSecrets, boolean minimalSettings) {
        assertThat(model, instanceOf(NvidiaModel.class));

        var nvidiaModel = (NvidiaModel) model;
        assertThat(nvidiaModel.getServiceSettings().modelId(), is(MODEL_VALUE));
        // Nvidia service does not support the user setting dimensions
        assertThat(nvidiaModel.getServiceSettings().dimensionsSetByUser(), is(nullValue()));

        if (minimalSettings) {
            // Check common default values
            assertThat(nvidiaModel.getServiceSettings().rateLimitSettings(), is(new RateLimitSettings(3000)));
            assertThat(nvidiaModel.getTaskSettings().isEmpty(), is(true));
        } else {
            // Check common configured values
            assertThat(nvidiaModel.getServiceSettings().uri().toString(), is(URL_VALUE));
            assertThat(nvidiaModel.getServiceSettings().rateLimitSettings(), is(new RateLimitSettings(REQUESTS_PER_MINUTE)));
        }

        if (modelIncludesSecrets) {
            assertThat(nvidiaModel.getSecretSettings().apiKey(), is(new SecureString(API_KEY_VALUE.toCharArray())));
        } else {
            assertThat(nvidiaModel.getSecretSettings(), is(nullValue()));
        }
        return nvidiaModel;
    }

    private static void assertTextEmbeddingModel(
        Model model,
        boolean modelIncludesSecrets,
        boolean minimalSettings,
        ConfigurationParseContext parseContext
    ) {
        var nvidiaModel = assertCommonModelFields(model, modelIncludesSecrets, minimalSettings);

        assertThat(nvidiaModel.getTaskType(), is(TEXT_EMBEDDING));
        assertThat(model, instanceOf(NvidiaEmbeddingsModel.class));
        var embeddingsModel = (NvidiaEmbeddingsModel) model;

        if (parseContext == ConfigurationParseContext.PERSISTENT) {
            assertThat(embeddingsModel.getServiceSettings().dimensions(), is(DIMENSIONS_VALUE));
        } else {
            assertThat(embeddingsModel.getServiceSettings().dimensions(), is(nullValue()));
        }

        if (minimalSettings) {
            // Check default values
            assertThat(embeddingsModel.getServiceSettings().uri().toString(), is(DEFAULT_EMBEDDINGS_URL_VALUE));
            assertThat(embeddingsModel.getServiceSettings().dimensionsSetByUser(), is(nullValue()));
            assertThat(embeddingsModel.getServiceSettings().similarity(), is(nullValue()));
            assertThat(embeddingsModel.getServiceSettings().maxInputTokens(), is(nullValue()));
        } else {
            // Check configured values
            assertThat(embeddingsModel.getServiceSettings().similarity(), is(SIMILARITY_MEASURE_VALUE));
            assertThat(embeddingsModel.getServiceSettings().maxInputTokens(), is(MAX_INPUT_TOKENS_VALUE));
            assertThat(embeddingsModel.getTaskSettings().getInputType(), is(INPUT_TYPE_VALUE));
            assertThat(embeddingsModel.getTaskSettings().getTruncation(), is(TRUNCATION_VALUE));
        }
    }

    private static void assertCompletionModel(Model model, boolean modelIncludesSecrets, boolean minimalSettings) {
        var nvidiaModel = assertCommonModelFields(model, modelIncludesSecrets, minimalSettings);
        assertThat(nvidiaModel.getTaskType(), is(COMPLETION));
        if (minimalSettings) {
            assertThat(nvidiaModel.getServiceSettings().uri().toString(), is(DEFAULT_COMPLETION_URL_VALUE));
        }
        assertThat(nvidiaModel.getServiceSettings().dimensions(), is(nullValue()));
        assertThat(nvidiaModel.getServiceSettings().similarity(), is(nullValue()));
    }

    private static void assertChatCompletionModel(Model model, boolean modelIncludesSecrets, boolean minimalSettings) {
        var nvidiaModel = assertCommonModelFields(model, modelIncludesSecrets, minimalSettings);
        assertThat(nvidiaModel.getTaskType(), is(CHAT_COMPLETION));
        if (minimalSettings) {
            assertThat(nvidiaModel.getServiceSettings().uri().toString(), is(DEFAULT_COMPLETION_URL_VALUE));
        }
        assertThat(nvidiaModel.getServiceSettings().dimensions(), is(nullValue()));
        assertThat(nvidiaModel.getServiceSettings().similarity(), is(nullValue()));
    }

    private static void assertRerankModel(Model model, boolean modelIncludesSecrets, boolean minimalSettings) {
        var nvidiaModel = assertCommonModelFields(model, modelIncludesSecrets, minimalSettings);
        assertThat(nvidiaModel.getTaskType(), is(RERANK));
        if (minimalSettings) {
            assertThat(nvidiaModel.getServiceSettings().uri().toString(), is(DEFAULT_RERANK_URL_VALUE));
        }
        assertThat(nvidiaModel.getServiceSettings().dimensions(), is(nullValue()));
        assertThat(nvidiaModel.getServiceSettings().similarity(), is(nullValue()));
    }

    static Map<String, Object> createServiceSettingsMap(TaskType taskType, ConfigurationParseContext parseContext) {
        if (Objects.requireNonNull(taskType) == TEXT_EMBEDDING) {
            if (parseContext.equals(ConfigurationParseContext.REQUEST)) {
                return buildServiceSettingsMap(
                    MODEL_VALUE,
                    URL_VALUE,
                    null,
                    SIMILARITY_MEASURE_VALUE.toString(),
                    MAX_INPUT_TOKENS_VALUE,
                    null
                );
            } else {
                return buildServiceSettingsMap(
                    MODEL_VALUE,
                    URL_VALUE,
                    DIMENSIONS_VALUE,
                    SIMILARITY_MEASURE_VALUE.toString(),
                    MAX_INPUT_TOKENS_VALUE,
                    null
                );
            }
        }
        return NvidiaChatCompletionServiceSettingsTests.buildServiceSettingsMap(MODEL_VALUE, URL_VALUE, null);
    }
}
