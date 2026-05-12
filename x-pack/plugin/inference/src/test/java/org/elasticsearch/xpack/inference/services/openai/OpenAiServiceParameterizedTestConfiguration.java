/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai;

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
import org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionModel;
import org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionServiceSettings;
import org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionTaskSettings;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsTaskSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;
import org.junit.Assert;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.test.ESTestCase.assertThat;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.services.ConfigurationParseContext.PERSISTENT;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS_SET_BY_USER;
import static org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettingsTests.getSecretSettingsMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.instanceOf;

public class OpenAiServiceParameterizedTestConfiguration {
    private static final String MODEL_VALUE = "model";
    private static final String URL_VALUE = "http://www.elastic.co";
    private static final String ORGANIZATION_VALUE = "org";
    private static final int MAX_INPUT_TOKENS_VALUE = 123;
    private static final SimilarityMeasure SIMILARITY_VALUE = SimilarityMeasure.DOT_PRODUCT;
    private static final int DIMENSIONS_VALUE = 100;
    private static final boolean DIMENSIONS_SET_BY_USER_VALUE = true;
    private static final String USER_VALUE = "user";
    private static final Map<String, String> HEADERS_VALUE = Map.of("header_key", "header_value");
    private static final String API_KEY_VALUE = "secret";
    private static final int REQUESTS_PER_MINUTE = 123;
    private static final String DEFAULT_EMBEDDINGS_URL = "https://api.openai.com/v1/embeddings";
    private static final String DEFAULT_COMPLETION_URL = "https://api.openai.com/v1/chat/completions";

    public static AbstractInferenceServiceParameterizedTests.TestConfiguration createTestConfiguration() {
        return new AbstractInferenceServiceParameterizedTests.TestConfiguration(
            EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.COMPLETION, TaskType.CHAT_COMPLETION),
            OpenAiService.NAME
        ) {
            @Override
            protected OpenAiService createService(ThreadPool threadPool, HttpClientManager clientManager) {
                var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
                return new OpenAiService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty());
            }

            @Override
            protected Map<String, Object> createMinimalServiceSettingsMap(TaskType taskType) {
                return createMinimalServiceSettingsMap(taskType, ConfigurationParseContext.REQUEST);
            }

            @Override
            protected Map<String, Object> createMinimalServiceSettingsMap(TaskType taskType, ConfigurationParseContext parseContext) {
                return new HashMap<>(Map.of(ServiceFields.MODEL_ID, MODEL_VALUE));
            }

            @Override
            protected Map<String, Object> createAllServiceSettingsMap(TaskType taskType, ConfigurationParseContext parseContext) {
                var serviceSettings = createMinimalServiceSettingsMap(taskType);
                RateLimitSettingsTests.addRateLimitSettingsToMap(serviceSettings, REQUESTS_PER_MINUTE);
                serviceSettings.putAll(
                    Map.of(
                        ServiceFields.URL,
                        URL_VALUE,
                        OpenAiServiceFields.ORGANIZATION,
                        ORGANIZATION_VALUE,
                        ServiceFields.MAX_INPUT_TOKENS,
                        MAX_INPUT_TOKENS_VALUE
                    )
                );
                if (taskType == TaskType.TEXT_EMBEDDING) {
                    serviceSettings.putAll(
                        Map.of(ServiceFields.DIMENSIONS, DIMENSIONS_VALUE, ServiceFields.SIMILARITY, SIMILARITY_VALUE.toString())
                    );
                    if (parseContext == PERSISTENT) {
                        serviceSettings.put(DIMENSIONS_SET_BY_USER, DIMENSIONS_SET_BY_USER_VALUE);
                    }
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
                    case TEXT_EMBEDDING -> OpenAiEmbeddingsServiceSettings.fromMap(serviceSettings, context);
                    case COMPLETION, CHAT_COMPLETION -> OpenAiChatCompletionServiceSettings.fromMap(serviceSettings, context);
                    default -> throw new IllegalStateException("Unexpected value: " + taskType);
                };
            }

            @Override
            protected TaskSettings getEmptyTaskSettings(TaskType taskType) {
                return switch (taskType) {
                    case TEXT_EMBEDDING -> new OpenAiEmbeddingsTaskSettings(Map.of());
                    case COMPLETION, CHAT_COMPLETION -> new OpenAiChatCompletionTaskSettings(Map.of());
                    default -> throw new IllegalStateException("Unexpected value: " + taskType);
                };
            }

            @Override
            protected ModelSecrets createModelSecrets(ConfigurationParseContext context) {
                return new ModelSecrets(DefaultSecretSettings.fromMap(createSecretSettingsMap(), context));
            }

            @Override
            protected Map<String, Object> createAllTaskSettingsMap(TaskType taskType) {
                return new HashMap<>(Map.of(OpenAiServiceFields.USER, USER_VALUE, OpenAiServiceFields.HEADERS, HEADERS_VALUE));
            }

            @Override
            protected Map<String, Object> createSecretSettingsMap() {
                return getSecretSettingsMap(API_KEY_VALUE);
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
                    case COMPLETION, CHAT_COMPLETION -> assertCompletionModel(model, modelIncludesSecrets, minimalSettings);
                    default -> Assert.fail("unexpected task type: " + taskType);
                }
            }

            @Override
            protected void assertModel(Model model, TaskType taskType, boolean modelIncludesSecrets, boolean minimalSettings) {
                assertModel(model, taskType, modelIncludesSecrets, minimalSettings, ConfigurationParseContext.REQUEST);
            }

            @Override
            protected EnumSet<TaskType> supportedStreamingTasks() {
                return EnumSet.of(TaskType.CHAT_COMPLETION, TaskType.COMPLETION);
            }
        };
    }

    private static void assertCommonSettings(OpenAiModel model, boolean modelIncludesSecrets, boolean minimalSettings) {
        assertThat(model.getServiceSettings().modelId(), is(MODEL_VALUE));

        if (modelIncludesSecrets) {
            assertThat(((DefaultSecretSettings) model.getSecretSettings()).apiKey().toString(), is(API_KEY_VALUE));
        } else {
            assertThat(model.getSecretSettings(), is(nullValue()));
        }
    }

    private static void assertTextEmbeddingModel(
        Model model,
        boolean modelIncludesSecrets,
        boolean minimalSettings,
        ConfigurationParseContext parseContext
    ) {
        assertThat(model, instanceOf(OpenAiEmbeddingsModel.class));
        var embeddingsModel = (OpenAiEmbeddingsModel) model;
        assertCommonSettings(embeddingsModel, modelIncludesSecrets, minimalSettings);

        if (minimalSettings) {
            assertThat(embeddingsModel.getServiceSettings().dimensionsSetByUser(), is(false));
        } else {
            assertThat(embeddingsModel.getServiceSettings().dimensionsSetByUser(), is(DIMENSIONS_SET_BY_USER_VALUE));
        }

        if (minimalSettings) {
            // Check default values
            assertThat(embeddingsModel.uri().toString(), is(DEFAULT_EMBEDDINGS_URL));
            assertThat(embeddingsModel.getServiceSettings().organizationId(), is(nullValue()));
            assertThat(embeddingsModel.getServiceSettings().maxInputTokens(), is(nullValue()));
            assertThat(embeddingsModel.getServiceSettings().dimensions(), is(nullValue()));
            assertThat(embeddingsModel.getServiceSettings().similarity(), is(nullValue()));
            assertThat(embeddingsModel.getServiceSettings().rateLimitSettings(), is(new RateLimitSettings(3000)));
            assertThat(embeddingsModel.getTaskSettings().isEmpty(), is(true));
        } else {
            // Check configured values
            assertThat(embeddingsModel.uri().toString(), is(URL_VALUE));
            assertThat(embeddingsModel.getServiceSettings().organizationId(), is(ORGANIZATION_VALUE));
            assertThat(embeddingsModel.getServiceSettings().maxInputTokens(), is(MAX_INPUT_TOKENS_VALUE));
            assertThat(embeddingsModel.getServiceSettings().dimensions(), is(DIMENSIONS_VALUE));
            assertThat(embeddingsModel.getServiceSettings().similarity(), is(SIMILARITY_VALUE));
            assertThat(embeddingsModel.getServiceSettings().rateLimitSettings(), is(new RateLimitSettings(REQUESTS_PER_MINUTE)));
            assertThat(embeddingsModel.getTaskSettings().user(), is(USER_VALUE));
            assertThat(embeddingsModel.getTaskSettings().headers(), is(HEADERS_VALUE));
        }
    }

    private static void assertCompletionModel(Model model, boolean modelIncludesSecrets, boolean minimalSettings) {
        assertThat(model, instanceOf(OpenAiChatCompletionModel.class));
        var completionModel = (OpenAiChatCompletionModel) model;

        assertCommonSettings(completionModel, modelIncludesSecrets, minimalSettings);

        assertThat(completionModel.getServiceSettings().dimensions(), is(nullValue()));
        assertThat(completionModel.getServiceSettings().similarity(), is(nullValue()));

        if (minimalSettings) {
            // Check default values
            assertThat(completionModel.uri().toString(), is(DEFAULT_COMPLETION_URL));
            assertThat(completionModel.getServiceSettings().organizationId(), is(nullValue()));
            assertThat(completionModel.getServiceSettings().maxInputTokens(), is(nullValue()));
            assertThat(completionModel.getServiceSettings().rateLimitSettings(), is(new RateLimitSettings(500)));
            assertThat(completionModel.getTaskSettings().isEmpty(), is(true));
        } else {
            // Check configured values
            assertThat(completionModel.uri().toString(), is(URL_VALUE));
            assertThat(completionModel.getServiceSettings().organizationId(), is(ORGANIZATION_VALUE));
            assertThat(completionModel.getServiceSettings().maxInputTokens(), is(MAX_INPUT_TOKENS_VALUE));
            assertThat(completionModel.getServiceSettings().rateLimitSettings(), is(new RateLimitSettings(REQUESTS_PER_MINUTE)));
            assertThat(completionModel.getTaskSettings().user(), is(USER_VALUE));
            assertThat(completionModel.getTaskSettings().headers(), is(HEADERS_VALUE));
        }
    }

}
