/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
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
import org.elasticsearch.xpack.inference.services.custom.response.CompletionResponseParser;
import org.elasticsearch.xpack.inference.services.custom.response.CustomResponseParser;
import org.elasticsearch.xpack.inference.services.custom.response.DenseEmbeddingResponseParser;
import org.elasticsearch.xpack.inference.services.custom.response.RerankResponseParser;
import org.elasticsearch.xpack.inference.services.custom.response.SparseEmbeddingResponseParser;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.ESTestCase.assertThat;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.custom.CustomServiceSettings.BATCH_SIZE;
import static org.elasticsearch.xpack.inference.services.custom.CustomServiceSettings.HEADERS;
import static org.elasticsearch.xpack.inference.services.custom.InputTypeTranslator.DEFAULT;
import static org.elasticsearch.xpack.inference.services.custom.InputTypeTranslator.EMPTY_TRANSLATOR;
import static org.elasticsearch.xpack.inference.services.custom.InputTypeTranslator.INPUT_TYPE_TRANSLATOR;
import static org.elasticsearch.xpack.inference.services.custom.QueryParameters.QUERY_PARAMETERS;
import static org.elasticsearch.xpack.inference.services.custom.response.RerankResponseParser.RERANK_PARSER_DOCUMENT_TEXT;
import static org.elasticsearch.xpack.inference.services.custom.response.RerankResponseParser.RERANK_PARSER_INDEX;
import static org.elasticsearch.xpack.inference.services.custom.response.RerankResponseParser.RERANK_PARSER_SCORE;
import static org.elasticsearch.xpack.inference.services.custom.response.SparseEmbeddingResponseParser.SPARSE_EMBEDDING_TOKEN_PATH;
import static org.elasticsearch.xpack.inference.services.custom.response.SparseEmbeddingResponseParser.SPARSE_EMBEDDING_WEIGHT_PATH;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.fail;

public class CustomServiceParameterizedTestConfiguration {

    private static final String REQUEST_VALUE = "request body";
    private static final String SECRET_SETTINGS_KEY = "secret_settings_key";
    private static final String SECRET_SETTINGS_VALUE = "secret_settings_value";
    private static final String TASK_SETTINGS_KEY = "task_settings_key";
    private static final String TASK_SETTINGS_VALUE = "task_settings_value";
    private static final String SIMILARITY_VALUE = SimilarityMeasure.DOT_PRODUCT.toString();
    private static final int DIMENSIONS_VALUE = 1536;
    private static final int MAX_INPUT_TOKENS_VALUE = 512;
    private static final int REQUESTS_PER_MINUTE_VALUE = 123;
    private static final int BATCH_SIZE_VALUE = 987;
    private static final String TRANSLATION_DEFAULT_VALUE = "translation_default_value";
    private static final String QUERY_KEY = "query_key";
    private static final String QUERY_VALUE = "query_value";
    private static final Map<String, String> HEADERS_VALUE = Map.of("header_key", "header_value");
    static final String URL_VALUE = "http://www.abc.com";

    public static AbstractInferenceServiceParameterizedTests.TestConfiguration createTestConfiguration() {
        return new AbstractInferenceServiceParameterizedTests.TestConfiguration(
            EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.SPARSE_EMBEDDING, TaskType.RERANK, TaskType.COMPLETION),
            CustomService.NAME
        ) {
            @Override
            protected CustomService createService(ThreadPool threadPool, HttpClientManager clientManager) {
                return CustomServiceParameterizedTestConfiguration.createService(threadPool, clientManager);
            }

            @Override
            protected Map<String, Object> createMinimalServiceSettingsMap(TaskType taskType) {
                return new HashMap<>(
                    Map.of(
                        ServiceFields.URL,
                        URL_VALUE,
                        CustomServiceSettings.REQUEST,
                        REQUEST_VALUE,
                        CustomServiceSettings.RESPONSE,
                        new HashMap<>(Map.of(CustomServiceSettings.JSON_PARSER, createResponseParserMap(taskType)))
                    )
                );
            }

            @Override
            protected Map<String, Object> createAllServiceSettingsMap(TaskType taskType, ConfigurationParseContext parseContext) {
                var settingsMap = createMinimalServiceSettingsMap(taskType);

                RateLimitSettingsTests.addRateLimitSettingsToMap(settingsMap, REQUESTS_PER_MINUTE_VALUE);

                settingsMap.put(QUERY_PARAMETERS, List.of(List.of(QUERY_KEY, QUERY_VALUE)));
                settingsMap.put(HEADERS, HEADERS_VALUE);
                settingsMap.put(INPUT_TYPE_TRANSLATOR, new HashMap<>(Map.of(DEFAULT, TRANSLATION_DEFAULT_VALUE)));
                settingsMap.put(BATCH_SIZE, BATCH_SIZE_VALUE);

                if (taskType == TaskType.TEXT_EMBEDDING) {
                    settingsMap.putAll(
                        Map.of(
                            ServiceFields.SIMILARITY,
                            SIMILARITY_VALUE,
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
                    case TEXT_EMBEDDING, SPARSE_EMBEDDING, RERANK, COMPLETION -> CustomServiceSettings.fromMap(
                        serviceSettings,
                        context,
                        taskType
                    );
                    default -> throw new IllegalArgumentException("unexpected task type [" + taskType + "]");
                };
            }

            @Override
            protected TaskSettings getEmptyTaskSettings(TaskType taskType) {
                return switch (taskType) {
                    case TEXT_EMBEDDING, SPARSE_EMBEDDING, RERANK, COMPLETION -> CustomTaskSettings.EMPTY_SETTINGS;
                    default -> throw new IllegalArgumentException("unexpected task type [" + taskType + "]");
                };
            }

            @Override
            protected ModelSecrets createModelSecrets(ConfigurationParseContext context) {
                return new ModelSecrets(CustomSecretSettings.fromMap(createSecretSettingsMap()));
            }

            @Override
            protected Map<String, Object> createAllTaskSettingsMap(TaskType taskType) {
                return new HashMap<>(Map.of(CustomTaskSettings.PARAMETERS, new HashMap<>(Map.of(TASK_SETTINGS_KEY, TASK_SETTINGS_VALUE))));
            }

            @Override
            protected Map<String, Object> createSecretSettingsMap() {
                return new HashMap<>(
                    Map.of(CustomSecretSettings.SECRET_PARAMETERS, new HashMap<>(Map.of(SECRET_SETTINGS_KEY, SECRET_SETTINGS_VALUE)))
                );
            }

            @Override
            protected void assertModel(Model model, TaskType taskType, boolean modelIncludesSecrets, boolean minimalSettings) {
                switch (taskType) {
                    case TEXT_EMBEDDING -> assertTextEmbeddingModel(model, modelIncludesSecrets, minimalSettings);
                    case SPARSE_EMBEDDING -> assertSparseEmbeddingModel(model, modelIncludesSecrets, minimalSettings);
                    case COMPLETION -> assertCompletionModel(model, modelIncludesSecrets, minimalSettings);
                    case RERANK -> assertRerankModel(model, modelIncludesSecrets, minimalSettings);
                    default -> fail("unexpected task type [" + taskType + "]");
                }
            }

            @Override
            protected EnumSet<TaskType> supportedStreamingTasks() {
                return EnumSet.noneOf(TaskType.class);
            }
        };
    }

    private static CustomModel assertCommonModelFields(Model model, boolean modelIncludesSecrets, boolean minimalSettings) {
        assertThat(model, instanceOf(CustomModel.class));

        var customModel = (CustomModel) model;

        assertThat(customModel.getServiceSettings().getUrl(), is(URL_VALUE));
        assertThat(customModel.getServiceSettings().getRequestContentString(), is(REQUEST_VALUE));
        if (minimalSettings) {
            // Check default values
            assertThat(customModel.getServiceSettings().getQueryParameters(), is(QueryParameters.EMPTY));
            assertThat(customModel.getServiceSettings().getHeaders(), is(Map.of()));
            assertThat(customModel.getServiceSettings().getInputTypeTranslator(), is(EMPTY_TRANSLATOR));
            assertThat(customModel.getServiceSettings().getBatchSize(), is(10));
        } else {
            // Check specified values
            assertThat(
                customModel.getServiceSettings().getQueryParameters().parameters(),
                is(List.of(new QueryParameters.Parameter(QUERY_KEY, QUERY_VALUE)))
            );
            assertThat(customModel.getServiceSettings().getHeaders(), is(HEADERS_VALUE));
            assertThat(customModel.getServiceSettings().getInputTypeTranslator().getDefaultValue(), is(TRANSLATION_DEFAULT_VALUE));
            assertThat(customModel.getServiceSettings().getBatchSize(), is(BATCH_SIZE_VALUE));
        }

        if (modelIncludesSecrets) {
            assertThat(
                customModel.getSecretSettings().getSecretParameters(),
                is(Map.of(SECRET_SETTINGS_KEY, new SecureString(SECRET_SETTINGS_VALUE.toCharArray())))
            );
        }

        return customModel;
    }

    private static void assertTextEmbeddingModel(Model model, boolean modelIncludesSecrets, boolean minimalSettings) {
        var customModel = assertCommonModelFields(model, modelIncludesSecrets, minimalSettings);

        assertThat(customModel.getTaskType(), is(TaskType.TEXT_EMBEDDING));
        assertThat(customModel.getServiceSettings().getResponseJsonParser(), instanceOf(DenseEmbeddingResponseParser.class));

        if (minimalSettings) {
            assertThat(customModel.getServiceSettings().dimensions(), nullValue());
            assertThat(customModel.getServiceSettings().similarity(), nullValue());
            assertThat(customModel.getServiceSettings().getMaxInputTokens(), nullValue());
        } else {
            assertThat(customModel.getServiceSettings().dimensions(), is(DIMENSIONS_VALUE));
            assertThat(customModel.getServiceSettings().similarity(), is(SimilarityMeasure.fromString(SIMILARITY_VALUE)));
            assertThat(customModel.getServiceSettings().getMaxInputTokens(), is(MAX_INPUT_TOKENS_VALUE));
        }
    }

    private static void assertSparseEmbeddingModel(Model model, boolean modelIncludesSecrets, boolean minimalSettings) {
        var customModel = assertCommonModelFields(model, modelIncludesSecrets, minimalSettings);

        assertThat(customModel.getTaskType(), is(TaskType.SPARSE_EMBEDDING));
        assertThat(customModel.getServiceSettings().getResponseJsonParser(), instanceOf(SparseEmbeddingResponseParser.class));

        assertThat(customModel.getServiceSettings().dimensions(), nullValue());
        assertThat(customModel.getServiceSettings().similarity(), nullValue());
        assertThat(customModel.getServiceSettings().getMaxInputTokens(), nullValue());
    }

    private static void assertCompletionModel(Model model, boolean modelIncludesSecrets, boolean minimalSettings) {
        var customModel = assertCommonModelFields(model, modelIncludesSecrets, minimalSettings);
        assertThat(customModel.getTaskType(), is(TaskType.COMPLETION));
        assertThat(customModel.getServiceSettings().getResponseJsonParser(), instanceOf(CompletionResponseParser.class));

        assertThat(customModel.getServiceSettings().dimensions(), nullValue());
        assertThat(customModel.getServiceSettings().similarity(), nullValue());
        assertThat(customModel.getServiceSettings().getMaxInputTokens(), nullValue());
    }

    private static void assertRerankModel(Model model, boolean modelIncludesSecrets, boolean minimalSettings) {
        var customModel = assertCommonModelFields(model, modelIncludesSecrets, minimalSettings);
        assertThat(customModel.getTaskType(), is(TaskType.RERANK));
        assertThat(customModel.getServiceSettings().getResponseJsonParser(), instanceOf(RerankResponseParser.class));

        assertThat(customModel.getServiceSettings().dimensions(), nullValue());
        assertThat(customModel.getServiceSettings().similarity(), nullValue());
        assertThat(customModel.getServiceSettings().getMaxInputTokens(), nullValue());
    }

    public static CustomService createService(ThreadPool threadPool, HttpClientManager clientManager) {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        return new CustomService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty());
    }

    static Map<String, Object> createResponseParserMap(TaskType taskType) {
        return switch (taskType) {
            case TEXT_EMBEDDING -> new HashMap<>(
                Map.of(DenseEmbeddingResponseParser.TEXT_EMBEDDING_PARSER_EMBEDDINGS, "$.result.embeddings[*].embedding")
            );
            case COMPLETION -> new HashMap<>(Map.of(CompletionResponseParser.COMPLETION_PARSER_RESULT, "$.result.text"));
            case SPARSE_EMBEDDING -> new HashMap<>(
                Map.of(
                    SPARSE_EMBEDDING_TOKEN_PATH,
                    "$.result[*].embeddings[*].token",
                    SPARSE_EMBEDDING_WEIGHT_PATH,
                    "$.result[*].embeddings[*].weight"
                )
            );
            case RERANK -> new HashMap<>(
                Map.of(
                    RERANK_PARSER_SCORE,
                    "$.result.scores[*].score",
                    RERANK_PARSER_INDEX,
                    "$.result.scores[*].index",
                    RERANK_PARSER_DOCUMENT_TEXT,
                    "$.result.scores[*].document_text"
                )
            );
            default -> throw new IllegalArgumentException("unexpected task type [" + taskType + "]");
        };
    }

    static CustomModel createInternalEmbeddingModel(DenseEmbeddingResponseParser parser, String url) {
        return createInternalEmbeddingModel(null, parser, url, null, null);
    }

    static CustomModel createInternalEmbeddingModel(
        @Nullable SimilarityMeasure similarityMeasure,
        DenseEmbeddingResponseParser parser,
        String url,
        @Nullable ChunkingSettings chunkingSettings,
        @Nullable Integer batchSize
    ) {

        return new CustomModel(
            "inference_id",
            TaskType.TEXT_EMBEDDING,
            CustomService.NAME,
            new CustomServiceSettings(
                new CustomServiceSettings.TextEmbeddingSettings(similarityMeasure, REQUESTS_PER_MINUTE_VALUE, 456),
                url,
                Map.of("key", "value"),
                QueryParameters.EMPTY,
                "{\"input\":${input}}",
                parser,
                new RateLimitSettings(10_000),
                batchSize,
                EMPTY_TRANSLATOR
            ),
            new CustomTaskSettings(Map.of("key", "test_value")),
            new CustomSecretSettings(Map.of("test_key", new SecureString("test_value".toCharArray()))),
            chunkingSettings
        );
    }

    static CustomModel createCustomModel(
        TaskType taskType,
        CustomResponseParser customResponseParser,
        String url,
        @Nullable ChunkingSettings chunkingSettings
    ) {
        return new CustomModel(
            "model_id",
            taskType,
            CustomService.NAME,
            new CustomServiceSettings(
                getDefaultTextEmbeddingSettings(taskType),
                url,
                Map.of("key", "value"),
                QueryParameters.EMPTY,
                "{\"input\":${input}}",
                customResponseParser,
                new RateLimitSettings(10_000)
            ),
            new CustomTaskSettings(Map.of("key", "test_value")),
            new CustomSecretSettings(Map.of("test_key", new SecureString("test_value".toCharArray()))),
            chunkingSettings
        );
    }

    static CustomServiceSettings.TextEmbeddingSettings getDefaultTextEmbeddingSettings(TaskType taskType) {
        return taskType == TaskType.TEXT_EMBEDDING
            ? CustomServiceSettings.TextEmbeddingSettings.DEFAULT_FLOAT
            : CustomServiceSettings.TextEmbeddingSettings.NON_TEXT_EMBEDDING_TASK_TYPE_SETTINGS;
    }
}
