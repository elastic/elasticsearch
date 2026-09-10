/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud;

import org.elasticsearch.common.settings.SecureString;
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
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;
import org.elasticsearch.xpack.inference.services.tencentcloud.completion.TencentCloudChatCompletionModel;
import org.elasticsearch.xpack.inference.services.tencentcloud.completion.TencentCloudChatCompletionServiceSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.completion.TencentCloudChatCompletionTaskSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.embeddings.TencentCloudEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.tencentcloud.embeddings.TencentCloudEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.embeddings.TencentCloudEmbeddingsTaskSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.rerank.TencentCloudRerankModel;
import org.elasticsearch.xpack.inference.services.tencentcloud.rerank.TencentCloudRerankServiceSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.rerank.TencentCloudRerankTaskSettings;
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
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class TencentCloudServiceParameterizedTestConfiguration {

    private static final String API_KEY_FIELD_NAME = "api_key";
    private static final String REGION_FIELD_NAME = "region";

    static final String MODEL_ID_VALUE = "some_model";
    static final String API_KEY_VALUE = "sk-test-api-key";
    private static final String REGION_VALUE = "sh";
    private static final String DEFAULT_REGION_VALUE = "bj";
    static final int DIMENSIONS_VALUE = 1536;
    private static final int MAX_INPUT_TOKENS_VALUE = 512;
    private static final SimilarityMeasure SIMILARITY_VALUE = SimilarityMeasure.COSINE;
    private static final int REQUESTS_PER_MINUTE = 123;
    private static final int TOP_N_VALUE = 5;
    private static final boolean RETURN_DOCUMENTS_VALUE = true;

    private static final int DEFAULT_REQUESTS_PER_MINUTE = 20;
    private static final int DEFAULT_CHAT_COMPLETION_REQUESTS_PER_MINUTE = 5;

    private static final String URI_TEMPLATE = "https://%s.aisearch.tencentelasticsearch.com/v1/%s";
    private static final String EMBEDDINGS_PATH = "embeddings";
    private static final String CHAT_COMPLETIONS_PATH = "chat/completions";
    private static final String RERANK_PATH = "rerank";

    public static AbstractInferenceServiceParameterizedTests.TestConfiguration createTestConfiguration() {
        return new AbstractInferenceServiceParameterizedTests.TestConfiguration(
            EnumSet.of(TEXT_EMBEDDING, COMPLETION, CHAT_COMPLETION, RERANK),
            TencentCloudService.NAME
        ) {

            @Override
            protected TencentCloudService createService(ThreadPool threadPool, HttpClientManager clientManager) {
                var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
                return new TencentCloudService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty());
            }

            @Override
            protected Map<String, Object> createMinimalServiceSettingsMap(TaskType taskType) {
                return createMinimalServiceSettingsMap(taskType, ConfigurationParseContext.REQUEST);
            }

            @Override
            protected Map<String, Object> createMinimalServiceSettingsMap(TaskType taskType, ConfigurationParseContext parseContext) {
                var map = new HashMap<String, Object>(Map.of(MODEL_ID, MODEL_ID_VALUE));
                // dimensions is only readable in a PERSISTENT context; putting it in a REQUEST map fails with unknown field
                if (taskType == TEXT_EMBEDDING && parseContext == ConfigurationParseContext.PERSISTENT) {
                    map.put(DIMENSIONS, DIMENSIONS_VALUE);
                }
                return map;
            }

            @Override
            protected Map<String, Object> createAllServiceSettingsMap(TaskType taskType, ConfigurationParseContext parseContext) {
                var map = createMinimalServiceSettingsMap(taskType, parseContext);
                map.put(REGION_FIELD_NAME, REGION_VALUE);
                RateLimitSettingsTests.addRateLimitSettingsToMap(map, REQUESTS_PER_MINUTE);
                if (taskType == TEXT_EMBEDDING) {
                    map.put(SIMILARITY, SIMILARITY_VALUE.toString());
                    map.put(MAX_INPUT_TOKENS, MAX_INPUT_TOKENS_VALUE);
                }
                return map;
            }

            @Override
            protected ServiceSettings getServiceSettings(
                Map<String, Object> serviceSettings,
                TaskType taskType,
                ConfigurationParseContext context
            ) {
                return switch (taskType) {
                    case TEXT_EMBEDDING -> TencentCloudEmbeddingsServiceSettings.fromMap(serviceSettings, context);
                    case COMPLETION, CHAT_COMPLETION -> TencentCloudChatCompletionServiceSettings.fromMap(serviceSettings, context);
                    case RERANK -> TencentCloudRerankServiceSettings.fromMap(serviceSettings, context);
                    default -> throw new IllegalStateException("Unexpected value: " + taskType);
                };
            }

            @Override
            protected TaskSettings getEmptyTaskSettings(TaskType taskType) {
                return switch (taskType) {
                    case TEXT_EMBEDDING -> TencentCloudEmbeddingsTaskSettings.EMPTY_SETTINGS;
                    case COMPLETION, CHAT_COMPLETION -> TencentCloudChatCompletionTaskSettings.EMPTY_SETTINGS;
                    case RERANK -> TencentCloudRerankTaskSettings.EMPTY_SETTINGS;
                    default -> throw new IllegalStateException("Unexpected value: " + taskType);
                };
            }

            @Override
            protected ModelSecrets createModelSecrets(ConfigurationParseContext context) {
                return new ModelSecrets(DefaultSecretSettings.fromMap(createSecretSettingsMap(), context));
            }

            @Override
            protected Map<String, Object> createAllTaskSettingsMap(TaskType taskType) {
                var map = new HashMap<String, Object>();
                if (taskType == RERANK) {
                    map.put(TencentCloudRerankTaskSettings.TOP_N, TOP_N_VALUE);
                    map.put(TencentCloudRerankTaskSettings.RETURN_DOCUMENTS, RETURN_DOCUMENTS_VALUE);
                }
                return map;
            }

            @Override
            protected Map<String, Object> createSecretSettingsMap() {
                return new HashMap<>(Map.of(API_KEY_FIELD_NAME, API_KEY_VALUE));
            }

            @Override
            protected void assertModel(Model model, TaskType taskType, boolean modelIncludesSecrets, boolean minimalSettings) {
                assertModel(model, taskType, modelIncludesSecrets, minimalSettings, ConfigurationParseContext.REQUEST);
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
                    case COMPLETION, CHAT_COMPLETION -> assertChatCompletionModel(model, taskType, modelIncludesSecrets, minimalSettings);
                    case RERANK -> assertRerankModel(model, modelIncludesSecrets, minimalSettings);
                    default -> Assert.fail("unexpected task type [" + taskType + "]");
                }
            }

            @Override
            protected EnumSet<TaskType> supportedStreamingTasks() {
                return EnumSet.of(COMPLETION, CHAT_COMPLETION);
            }
        };
    }

    private static void assertCommonSettings(
        TencentCloudModel model,
        boolean modelIncludesSecrets,
        String taskPath,
        boolean minimalSettings
    ) {
        var serviceSettings = (TencentCloudCommonServiceSettings) model.getServiceSettings();
        assertThat(serviceSettings.modelId(), is(MODEL_ID_VALUE));

        var expectedRegion = minimalSettings ? DEFAULT_REGION_VALUE : REGION_VALUE;
        assertThat(serviceSettings.region(), is(expectedRegion));
        assertThat(model.uri().toString(), is(String.format(java.util.Locale.ROOT, URI_TEMPLATE, expectedRegion, taskPath)));

        if (minimalSettings) {
            // rate limit default depends on the settings type - checked in the task-specific helpers
        } else {
            assertThat(serviceSettings.rateLimitSettings(), is(new RateLimitSettings(REQUESTS_PER_MINUTE)));
        }

        if (modelIncludesSecrets) {
            assertThat(model.getSecretSettings().apiKey(), is(new SecureString(API_KEY_VALUE.toCharArray())));
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
        assertThat(model, instanceOf(TencentCloudEmbeddingsModel.class));
        var embeddingsModel = (TencentCloudEmbeddingsModel) model;
        assertThat(embeddingsModel.getTaskType(), is(TEXT_EMBEDDING));
        assertCommonSettings(embeddingsModel, modelIncludesSecrets, EMBEDDINGS_PATH, minimalSettings);

        var ss = embeddingsModel.getServiceSettings();
        if (minimalSettings) {
            if (parseContext == ConfigurationParseContext.PERSISTENT) {
                assertThat(ss.dimensions(), is(DIMENSIONS_VALUE));
            } else {
                assertThat(ss.dimensions(), is(nullValue()));
            }
            assertThat(ss.similarity(), is(nullValue()));
            assertThat(ss.maxInputTokens(), is(nullValue()));
            assertThat(ss.rateLimitSettings(), is(new RateLimitSettings(DEFAULT_REQUESTS_PER_MINUTE)));
        } else {
            if (parseContext == ConfigurationParseContext.PERSISTENT) {
                assertThat(ss.dimensions(), is(DIMENSIONS_VALUE));
            } else {
                assertThat(ss.dimensions(), is(nullValue()));
            }
            assertThat(ss.similarity(), is(SIMILARITY_VALUE));
            assertThat(ss.maxInputTokens(), is(MAX_INPUT_TOKENS_VALUE));
        }
        assertThat(embeddingsModel.getTaskSettings(), is(TencentCloudEmbeddingsTaskSettings.EMPTY_SETTINGS));
    }

    private static void assertChatCompletionModel(Model model, TaskType taskType, boolean modelIncludesSecrets, boolean minimalSettings) {
        assertThat(model, instanceOf(TencentCloudChatCompletionModel.class));
        var chatModel = (TencentCloudChatCompletionModel) model;
        assertThat(chatModel.getTaskType(), is(taskType));
        assertCommonSettings(chatModel, modelIncludesSecrets, CHAT_COMPLETIONS_PATH, minimalSettings);
        assertThat(chatModel.model(), is(MODEL_ID_VALUE));
        if (minimalSettings) {
            assertThat(
                chatModel.getServiceSettings().rateLimitSettings(),
                is(new RateLimitSettings(DEFAULT_CHAT_COMPLETION_REQUESTS_PER_MINUTE))
            );
        }
    }

    private static void assertRerankModel(Model model, boolean modelIncludesSecrets, boolean minimalSettings) {
        assertThat(model, instanceOf(TencentCloudRerankModel.class));
        var rerankModel = (TencentCloudRerankModel) model;
        assertThat(rerankModel.getTaskType(), is(RERANK));
        assertCommonSettings(rerankModel, modelIncludesSecrets, RERANK_PATH, minimalSettings);
        if (minimalSettings) {
            assertThat(rerankModel.getServiceSettings().rateLimitSettings(), is(new RateLimitSettings(DEFAULT_REQUESTS_PER_MINUTE)));
            assertThat(rerankModel.getTaskSettings(), is(TencentCloudRerankTaskSettings.EMPTY_SETTINGS));
        } else {
            assertThat(rerankModel.getTaskSettings().getTopN(), is(TOP_N_VALUE));
            assertThat(rerankModel.getTaskSettings().getReturnDocuments(), is(RETURN_DOCUMENTS_VALUE));
        }
    }
}
