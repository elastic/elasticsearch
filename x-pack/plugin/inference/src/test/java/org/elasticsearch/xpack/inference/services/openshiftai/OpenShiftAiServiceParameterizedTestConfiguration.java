/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openshiftai;

import org.elasticsearch.common.Strings;
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
import org.elasticsearch.xpack.inference.services.openshiftai.completion.OpenShiftAiChatCompletionServiceSettings;
import org.elasticsearch.xpack.inference.services.openshiftai.embeddings.OpenShiftAiEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.openshiftai.embeddings.OpenShiftAiEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.openshiftai.rerank.OpenShiftAiRerankModel;
import org.elasticsearch.xpack.inference.services.openshiftai.rerank.OpenShiftAiRerankServiceSettings;
import org.elasticsearch.xpack.inference.services.openshiftai.rerank.OpenShiftAiRerankTaskSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;
import org.junit.Assert;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.inference.TaskType.CHAT_COMPLETION;
import static org.elasticsearch.inference.TaskType.COMPLETION;
import static org.elasticsearch.inference.TaskType.RERANK;
import static org.elasticsearch.inference.TaskType.TEXT_EMBEDDING;
import static org.elasticsearch.test.ESTestCase.assertThat;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.openshiftai.rerank.OpenShiftAiRerankTaskSettings.RETURN_DOCUMENTS;
import static org.elasticsearch.xpack.inference.services.openshiftai.rerank.OpenShiftAiRerankTaskSettings.TOP_N;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class OpenShiftAiServiceParameterizedTestConfiguration {

    private static final String API_KEY_FIELD_NAME = "api_key";
    private static final String URL_VALUE = "http://www.abc.com";
    private static final int MAX_INPUT_TOKENS_VALUE = 512;
    private static final boolean DIMENSIONS_SET_BY_USER_VALUE = true;
    private static final SimilarityMeasure SIMILARITY_VALUE = SimilarityMeasure.COSINE;
    private static final int REQUESTS_PER_MINUTE = 123;
    private static final int TOP_N_VALUE = 987;
    private static final boolean RETURN_DOCUMENTS_VALUE = true;
    static final String MODEL_VALUE = "some_model";
    static final String API_KEY_VALUE = "test_api_key";
    static final int DIMENSIONS_VALUE = 1536;

    public static AbstractInferenceServiceParameterizedTests.TestConfiguration createTestConfiguration() {
        return new AbstractInferenceServiceParameterizedTests.TestConfiguration(
            EnumSet.of(TEXT_EMBEDDING, COMPLETION, CHAT_COMPLETION, RERANK),
            OpenShiftAiService.NAME
        ) {

            @Override
            protected OpenShiftAiService createService(ThreadPool threadPool, HttpClientManager clientManager) {
                var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
                return new OpenShiftAiService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty());
            }

            @Override
            protected Map<String, Object> createMinimalServiceSettingsMap(TaskType taskType) {
                return createMinimalServiceSettingsMap(taskType, ConfigurationParseContext.REQUEST);
            }

            @Override
            protected Map<String, Object> createMinimalServiceSettingsMap(TaskType taskType, ConfigurationParseContext parseContext) {
                Map<String, Object> settingsMap = new HashMap<>(Map.of(ServiceFields.URL, URL_VALUE));

                if (taskType == TEXT_EMBEDDING && parseContext == ConfigurationParseContext.PERSISTENT) {
                    settingsMap.put(ServiceFields.DIMENSIONS_SET_BY_USER, false);
                }

                return settingsMap;
            }

            @Override
            protected Map<String, Object> createAllServiceSettingsMap(TaskType taskType, ConfigurationParseContext parseContext) {
                var serviceSettings = createMinimalServiceSettingsMap(taskType, parseContext);
                RateLimitSettingsTests.addRateLimitSettingsToMap(serviceSettings, REQUESTS_PER_MINUTE);

                serviceSettings.put(MODEL_ID, MODEL_VALUE);
                if (taskType == TEXT_EMBEDDING) {
                    serviceSettings.put(DIMENSIONS, DIMENSIONS_VALUE);
                    serviceSettings.put(SIMILARITY, SIMILARITY_VALUE.toString());
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
                    case TEXT_EMBEDDING -> OpenShiftAiEmbeddingsServiceSettings.fromMap(serviceSettings, context);
                    case COMPLETION, CHAT_COMPLETION -> OpenShiftAiChatCompletionServiceSettings.fromMap(serviceSettings, context);
                    case RERANK -> OpenShiftAiRerankServiceSettings.fromMap(serviceSettings, context);
                    default -> throw new IllegalStateException("Unexpected value: " + taskType);
                };
            }

            @Override
            protected TaskSettings getEmptyTaskSettings(TaskType taskType) {
                return switch (taskType) {
                    case TEXT_EMBEDDING, COMPLETION, CHAT_COMPLETION -> EmptyTaskSettings.INSTANCE;
                    case RERANK -> OpenShiftAiRerankTaskSettings.fromMap(null);
                    default -> throw new IllegalStateException("Unexpected value: " + taskType);
                };
            }

            @Override
            protected ModelSecrets createModelSecrets(ConfigurationParseContext context) {
                return new ModelSecrets(DefaultSecretSettings.fromMap(createSecretSettingsMap(), context));
            }

            @Override
            protected Map<String, Object> createAllTaskSettingsMap(TaskType taskType) {
                HashMap<String, Object> taskSettingsMap = new HashMap<>();
                if (taskType == RERANK) {
                    taskSettingsMap.put(RETURN_DOCUMENTS, RETURN_DOCUMENTS_VALUE);
                    taskSettingsMap.put(TOP_N, TOP_N_VALUE);
                }
                return taskSettingsMap;
            }

            @Override
            protected Map<String, Object> createSecretSettingsMap() {
                return new HashMap<>(Map.of(API_KEY_FIELD_NAME, API_KEY_VALUE));
            }

            @Override
            protected void assertModel(Model model, TaskType taskType, boolean modelIncludesSecrets, boolean minimalSettings) {
                switch (taskType) {
                    case TEXT_EMBEDDING -> assertTextEmbeddingModel(model, modelIncludesSecrets, minimalSettings);
                    case COMPLETION -> assertCompletionModel(model, modelIncludesSecrets, minimalSettings);
                    case CHAT_COMPLETION -> assertChatCompletionModel(model, modelIncludesSecrets, minimalSettings);
                    case RERANK -> assertRerankModel(model, modelIncludesSecrets, minimalSettings);
                    default -> Assert.fail(Strings.format("unexpected task type [%s]", taskType));
                }
            }

            @Override
            protected EnumSet<TaskType> supportedStreamingTasks() {
                return EnumSet.of(CHAT_COMPLETION, COMPLETION);
            }
        };
    }

    private static OpenShiftAiModel assertCommonModelFields(Model model, boolean modelIncludesSecrets, boolean minimalSettings) {
        assertThat(model, instanceOf(OpenShiftAiModel.class));

        var openShiftAiModel = (OpenShiftAiModel) model;
        assertThat(openShiftAiModel.getServiceSettings().uri().toString(), is(URL_VALUE));
        if (minimalSettings) {
            // Check default values
            assertThat(openShiftAiModel.getServiceSettings().modelId(), is(nullValue()));
            assertThat(openShiftAiModel.getServiceSettings().rateLimitSettings(), is(new RateLimitSettings(3000)));
            assertThat(openShiftAiModel.getServiceSettings().dimensions(), is(nullValue()));
            assertThat(openShiftAiModel.getServiceSettings().similarity(), is(nullValue()));
        } else {
            // Check configured values
            assertThat(openShiftAiModel.getServiceSettings().modelId(), is(MODEL_VALUE));
            assertThat(openShiftAiModel.getServiceSettings().rateLimitSettings(), is(new RateLimitSettings(REQUESTS_PER_MINUTE)));
        }

        if (modelIncludesSecrets) {
            assertThat(openShiftAiModel.getSecretSettings().apiKey(), is(new SecureString(API_KEY_VALUE.toCharArray())));
        } else {
            assertThat(openShiftAiModel.getSecretSettings(), is(nullValue()));
        }

        return openShiftAiModel;
    }

    private static void assertTextEmbeddingModel(Model model, boolean modelIncludesSecrets, boolean minimalSettings) {
        var openShiftAiModel = assertCommonModelFields(model, modelIncludesSecrets, minimalSettings);

        assertThat(openShiftAiModel.getTaskSettings(), is(EmptyTaskSettings.INSTANCE));
        assertThat(openShiftAiModel.getTaskType(), is(TaskType.TEXT_EMBEDDING));
        assertThat(model, instanceOf(OpenShiftAiEmbeddingsModel.class));
        var embeddingsModel = (OpenShiftAiEmbeddingsModel) model;

        if (minimalSettings) {
            assertThat(openShiftAiModel.getServiceSettings().dimensionsSetByUser(), is(false));
            assertThat(embeddingsModel.getServiceSettings().maxInputTokens(), is(nullValue()));
        } else {
            assertThat(embeddingsModel.getServiceSettings().dimensions(), is(DIMENSIONS_VALUE));
            assertThat(embeddingsModel.getServiceSettings().dimensionsSetByUser(), is(DIMENSIONS_SET_BY_USER_VALUE));
            assertThat(embeddingsModel.getServiceSettings().similarity(), is(SIMILARITY_VALUE));
            assertThat(embeddingsModel.getServiceSettings().maxInputTokens(), is(MAX_INPUT_TOKENS_VALUE));
        }
    }

    private static void assertCompletionModel(Model model, boolean modelIncludesSecrets, boolean minimalSettings) {
        var openShiftAiModel = assertCommonModelFields(model, modelIncludesSecrets, minimalSettings);
        assertThat(openShiftAiModel.getTaskSettings(), is(EmptyTaskSettings.INSTANCE));
        assertThat(openShiftAiModel.getTaskType(), is(COMPLETION));

        assertThat(openShiftAiModel.getServiceSettings().dimensionsSetByUser(), is(nullValue()));
    }

    private static void assertRerankModel(Model model, boolean modelIncludesSecrets, boolean minimalSettings) {
        var openShiftAiModel = assertCommonModelFields(model, modelIncludesSecrets, minimalSettings);
        assertThat(openShiftAiModel.getTaskType(), is(TaskType.RERANK));
        assertThat(model, instanceOf(OpenShiftAiRerankModel.class));
        var rerankModel = (OpenShiftAiRerankModel) model;

        assertThat(openShiftAiModel.getServiceSettings().dimensionsSetByUser(), is(nullValue()));
        if (minimalSettings) {
            assertThat(rerankModel.getTaskSettings().isEmpty(), is(true));
        } else {
            assertThat(rerankModel.getTaskSettings().getReturnDocuments(), is(RETURN_DOCUMENTS_VALUE));
            assertThat(rerankModel.getTaskSettings().getTopN(), is(TOP_N_VALUE));
        }
    }

    private static void assertChatCompletionModel(Model model, boolean modelIncludesSecrets, boolean minimalSettings) {
        var openShiftAiModel = assertCommonModelFields(model, modelIncludesSecrets, minimalSettings);
        assertThat(openShiftAiModel.getTaskSettings(), is(EmptyTaskSettings.INSTANCE));
        assertThat(openShiftAiModel.getTaskType(), is(TaskType.CHAT_COMPLETION));

        assertThat(openShiftAiModel.getServiceSettings().dimensionsSetByUser(), is(nullValue()));
    }
}
