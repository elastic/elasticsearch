/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.llama;

import org.elasticsearch.common.settings.SecureString;
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
import org.elasticsearch.xpack.inference.services.llama.completion.LlamaChatCompletionServiceSettings;
import org.elasticsearch.xpack.inference.services.llama.embeddings.LlamaEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.llama.embeddings.LlamaEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;
import org.hamcrest.Matchers;
import org.junit.Assert;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.inference.TaskType.CHAT_COMPLETION;
import static org.elasticsearch.inference.TaskType.COMPLETION;
import static org.elasticsearch.inference.TaskType.TEXT_EMBEDDING;
import static org.elasticsearch.test.ESTestCase.assertThat;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings.API_KEY;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class LlamaServiceParameterizedTestConfiguration {

    private static final String URL_VALUE = "http://www.abc.com";
    private static final String MODEL_ID_VALUE = "model_id";
    private static final SimilarityMeasure SIMILARITY_MEASURE_VALUE = SimilarityMeasure.COSINE;
    private static final int DIMENSIONS_VALUE = 1536;
    private static final int MAX_INPUT_TOKENS_VALUE = 512;
    private static final int REQUESTS_PER_MINUTE = 123;
    private static final String API_KEY_VALUE = "secret";

    public static AbstractInferenceServiceParameterizedTests.TestConfiguration createTestConfiguration() {
        return new AbstractInferenceServiceParameterizedTests.TestConfiguration(
            EnumSet.of(TEXT_EMBEDDING, COMPLETION, CHAT_COMPLETION),
            LlamaService.NAME
        ) {

            @Override
            protected LlamaService createService(ThreadPool threadPool, HttpClientManager clientManager) {
                var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
                return new LlamaService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty());
            }

            @Override
            protected Map<String, Object> createMinimalServiceSettingsMap(TaskType taskType) {
                return new HashMap<>(Map.of(ServiceFields.URL, URL_VALUE, ServiceFields.MODEL_ID, MODEL_ID_VALUE));
            }

            @Override
            protected Map<String, Object> createAllServiceSettingsMap(TaskType taskType, ConfigurationParseContext parseContext) {
                var settingsMap = createMinimalServiceSettingsMap(taskType);
                RateLimitSettingsTests.addRateLimitSettingsToMap(settingsMap, REQUESTS_PER_MINUTE);

                if (taskType == TEXT_EMBEDDING) {
                    settingsMap.putAll(
                        Map.of(
                            ServiceFields.SIMILARITY,
                            SIMILARITY_MEASURE_VALUE.toString(),
                            ServiceFields.DIMENSIONS,
                            DIMENSIONS_VALUE,
                            ServiceFields.MAX_INPUT_TOKENS,
                            MAX_INPUT_TOKENS_VALUE
                        )
                    );
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
                    case TEXT_EMBEDDING -> LlamaEmbeddingsServiceSettings.fromMap(serviceSettings, context);
                    case COMPLETION, CHAT_COMPLETION -> LlamaChatCompletionServiceSettings.fromMap(serviceSettings, context);
                    default -> throw new IllegalStateException("Unexpected value: " + taskType);
                };
            }

            @Override
            protected TaskSettings getEmptyTaskSettings(TaskType taskType) {
                return switch (taskType) {
                    case TEXT_EMBEDDING, COMPLETION, CHAT_COMPLETION -> EmptyTaskSettings.INSTANCE;
                    default -> throw new IllegalStateException("Unexpected value: " + taskType);
                };
            }

            @Override
            protected ModelSecrets createModelSecrets(ConfigurationParseContext context) {
                return new ModelSecrets(DefaultSecretSettings.fromMap(createSecretSettingsMap(), context));
            }

            @Override
            protected Map<String, Object> createAllTaskSettingsMap(TaskType taskType) {
                return new HashMap<>();
            }

            @Override
            protected Map<String, Object> createSecretSettingsMap() {
                return new HashMap<>(Map.of(API_KEY, API_KEY_VALUE));
            }

            @Override
            protected void assertModel(Model model, TaskType taskType, boolean modelIncludesSecrets, boolean minimalSettings) {
                switch (taskType) {
                    case TEXT_EMBEDDING -> assertTextEmbeddingModel(model, modelIncludesSecrets, minimalSettings);
                    case COMPLETION -> assertCompletionModel(model, modelIncludesSecrets, minimalSettings);
                    case CHAT_COMPLETION -> assertChatCompletionModel(model, modelIncludesSecrets, minimalSettings);
                    default -> Assert.fail("unexpected task type [" + taskType + "]");
                }
            }

            @Override
            protected EnumSet<TaskType> supportedStreamingTasks() {
                return EnumSet.of(CHAT_COMPLETION, COMPLETION);
            }
        };
    }

    private static LlamaModel assertCommonModelFields(Model model, boolean modelIncludesSecrets, boolean minimalSettings) {
        assertThat(model, instanceOf(LlamaModel.class));
        var llamaModel = (LlamaModel) model;
        assertThat(llamaModel.getServiceSettings().modelId(), is(MODEL_ID_VALUE));
        assertThat(llamaModel.uri().toString(), Matchers.is(URL_VALUE));
        assertThat(llamaModel.getTaskSettings(), Matchers.is(EmptyTaskSettings.INSTANCE));
        if (minimalSettings) {
            // Check default value
            assertThat(llamaModel.rateLimitSettings(), is(new RateLimitSettings(3000)));
        } else {
            // Check configured value
            assertThat(llamaModel.rateLimitSettings(), is(new RateLimitSettings(REQUESTS_PER_MINUTE)));
        }

        if (modelIncludesSecrets) {
            assertThat(
                ((DefaultSecretSettings) llamaModel.getSecretSettings()).apiKey(),
                is(new SecureString(API_KEY_VALUE.toCharArray()))
            );
        } else {
            assertThat(llamaModel.getSecretSettings(), is(nullValue()));
        }

        return llamaModel;
    }

    private static void assertTextEmbeddingModel(Model model, boolean modelIncludesSecrets, boolean minimalSettings) {
        var llamaModel = assertCommonModelFields(model, modelIncludesSecrets, minimalSettings);

        assertThat(llamaModel.getTaskType(), Matchers.is(TEXT_EMBEDDING));
        var embeddingModel = (LlamaEmbeddingsModel) llamaModel;
        if (minimalSettings) {
            assertThat(embeddingModel.getServiceSettings().dimensions(), is(nullValue()));
            assertThat(embeddingModel.getServiceSettings().similarity(), is(nullValue()));
            assertThat(embeddingModel.getServiceSettings().maxInputTokens(), is(nullValue()));
        } else {
            assertThat(embeddingModel.getServiceSettings().dimensions(), is(DIMENSIONS_VALUE));
            assertThat(embeddingModel.getServiceSettings().similarity(), is(SIMILARITY_MEASURE_VALUE));
            assertThat(embeddingModel.getServiceSettings().maxInputTokens(), is(MAX_INPUT_TOKENS_VALUE));
        }
    }

    private static void assertCompletionModel(Model model, boolean modelIncludesSecrets, boolean minimalSettings) {
        var llamaModel = assertCommonModelFields(model, modelIncludesSecrets, minimalSettings);
        assertThat(llamaModel.getTaskType(), Matchers.is(COMPLETION));
    }

    private static void assertChatCompletionModel(Model model, boolean modelIncludesSecrets, boolean minimalSettings) {
        var llamaModel = assertCommonModelFields(model, modelIncludesSecrets, minimalSettings);
        assertThat(llamaModel.getTaskType(), Matchers.is(CHAT_COMPLETION));
    }
}
