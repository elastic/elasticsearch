/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.services.AbstractServiceTests;
import org.elasticsearch.xpack.inference.services.SenderService;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.custom.response.CompletionResponseParser;
import org.elasticsearch.xpack.inference.services.custom.response.ErrorResponseParser;
import org.elasticsearch.xpack.inference.services.custom.response.TextEmbeddingResponseParser;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.SerializableSecureString;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.custom.response.RerankResponseParser.RERANK_PARSER_DOCUMENT_TEXT;
import static org.elasticsearch.xpack.inference.services.custom.response.RerankResponseParser.RERANK_PARSER_INDEX;
import static org.elasticsearch.xpack.inference.services.custom.response.RerankResponseParser.RERANK_PARSER_SCORE;
import static org.elasticsearch.xpack.inference.services.custom.response.SparseEmbeddingResponseParser.SPARSE_EMBEDDING_TOKEN_PATH;
import static org.elasticsearch.xpack.inference.services.custom.response.SparseEmbeddingResponseParser.SPARSE_EMBEDDING_WEIGHT_PATH;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class CustomServiceTests extends AbstractServiceTests {

    public CustomServiceTests() {
        super(createTestConfiguration());
    }

    private static TestConfiguration createTestConfiguration() {
        return new TestConfiguration.Builder(new CommonConfig(TaskType.TEXT_EMBEDDING, TaskType.CHAT_COMPLETION) {
            @Override
            protected SenderService createService(ThreadPool threadPool, HttpClientManager clientManager) {
                return CustomServiceTests.createService(threadPool, clientManager);
            }

            @Override
            protected Map<String, Object> createServiceSettingsMap(TaskType taskType) {
                return CustomServiceTests.createServiceSettingsMap(taskType);
            }

            @Override
            protected Map<String, Object> createTaskSettingsMap() {
                return CustomServiceTests.createTaskSettingsMap();
            }

            @Override
            protected Map<String, Object> createSecretSettingsMap() {
                return CustomServiceTests.createSecretSettingsMap();
            }

            @Override
            protected void assertModel(Model model, TaskType taskType) {
                CustomServiceTests.assertModel(model, taskType);
            }

            @Override
            protected CustomModel createEmbeddingModel(TextEmbeddingResponseParser textEmbeddingResponseParser, String url) {
                return CustomServiceTests.createInternalEmbeddingModel(textEmbeddingResponseParser, url);
            }

            @Override
            protected EnumSet<TaskType> supportedStreamingTasks() {
                return EnumSet.noneOf(TaskType.class);
            }
        }).enableUpdateModelTests(new UpdateModelConfiguration() {
            @Override
            protected CustomModel createEmbeddingModel(SimilarityMeasure similarityMeasure) {
                return createInternalEmbeddingModel(similarityMeasure);
            }
        }).build();
    }

    private static void assertModel(Model model, TaskType taskType) {
        switch (taskType) {
            case TEXT_EMBEDDING -> assertTextEmbeddingModel(model);
            case COMPLETION -> assertCompletionModel(model);
            default -> fail("unexpected task type [" + taskType + "]");
        }
    }

    private static void assertTextEmbeddingModel(Model model) {
        var customModel = assertCommonModelFields(model);

        assertThat(customModel.getTaskType(), is(TaskType.TEXT_EMBEDDING));
        assertThat(customModel.getServiceSettings().getResponseJsonParser(), instanceOf(TextEmbeddingResponseParser.class));
    }

    private static CustomModel assertCommonModelFields(Model model) {
        assertThat(model, instanceOf(CustomModel.class));

        var customModel = (CustomModel) model;

        assertThat(customModel.getServiceSettings().getUrl(), is("http://www.abc.com"));
        assertThat(customModel.getTaskSettings().getParameters(), is(Map.of("test_key", "test_value")));
        assertThat(
            customModel.getSecretSettings().getSecretParameters(),
            is(Map.of("test_key", new SerializableSecureString("test_value")))
        );

        return customModel;
    }

    private static void assertCompletionModel(Model model) {
        var customModel = assertCommonModelFields(model);
        assertThat(customModel.getTaskType(), is(TaskType.COMPLETION));
        assertThat(customModel.getServiceSettings().getResponseJsonParser(), instanceOf(CompletionResponseParser.class));
    }

    private static SenderService createService(ThreadPool threadPool, HttpClientManager clientManager) {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        return new CustomService(senderFactory, createWithEmptySettings(threadPool));
    }

    private static Map<String, Object> createServiceSettingsMap(TaskType taskType) {
        return new HashMap<>(
            Map.of(
                ServiceFields.SIMILARITY,
                SimilarityMeasure.DOT_PRODUCT.toString(),
                ServiceFields.DIMENSIONS,
                1536,
                ServiceFields.MAX_INPUT_TOKENS,
                512,
                CustomServiceSettings.URL,
                "http://www.abc.com",
                CustomServiceSettings.HEADERS,
                Map.of("key", "value"),
                QueryParameters.QUERY_PARAMETERS,
                List.of(List.of("key", "value")),
                CustomServiceSettings.REQUEST,
                new HashMap<>(Map.of(CustomServiceSettings.REQUEST_CONTENT, "request body")),
                CustomServiceSettings.RESPONSE,
                new HashMap<>(
                    Map.of(
                        CustomServiceSettings.JSON_PARSER,
                        createResponseParserMap(taskType),
                        CustomServiceSettings.ERROR_PARSER,
                        new HashMap<>(Map.of(ErrorResponseParser.MESSAGE_PATH, "$.error.message"))
                    )
                )
            )
        );
    }

    private static Map<String, Object> createResponseParserMap(TaskType taskType) {
        return switch (taskType) {
            case TEXT_EMBEDDING -> new HashMap<>(
                Map.of(TextEmbeddingResponseParser.TEXT_EMBEDDING_PARSER_EMBEDDINGS, "$.result.embeddings[*].embedding")
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

    private static Map<String, Object> createTaskSettingsMap() {
        return new HashMap<>(Map.of(CustomTaskSettings.PARAMETERS, new HashMap<>(Map.of("test_key", "test_value"))));
    }

    private static Map<String, Object> createSecretSettingsMap() {
        return new HashMap<>(Map.of(CustomSecretSettings.SECRET_PARAMETERS, new HashMap<>(Map.of("test_key", "test_value"))));
    }

    private static CustomModel createInternalEmbeddingModel(SimilarityMeasure similarityMeasure) {
        return createInternalEmbeddingModel(
            similarityMeasure,
            new TextEmbeddingResponseParser("$.result.embeddings[*].embedding"),
            "http://www.abc.com"
        );
    }

    private static CustomModel createInternalEmbeddingModel(TextEmbeddingResponseParser parser, String url) {
        return createInternalEmbeddingModel(SimilarityMeasure.DOT_PRODUCT, parser, url);
    }

    private static CustomModel createInternalEmbeddingModel(
        @Nullable SimilarityMeasure similarityMeasure,
        TextEmbeddingResponseParser parser,
        String url
    ) {
        return new CustomModel(
            "model_id",
            TaskType.TEXT_EMBEDDING,
            CustomService.NAME,
            new CustomServiceSettings(
                similarityMeasure,
                123,
                456,
                url,
                Map.of("key", "value"),
                QueryParameters.EMPTY,
                "\"input\":\"${input}\"",
                parser,
                new RateLimitSettings(10_000),
                new ErrorResponseParser("$.error.message")
            ),
            new CustomTaskSettings(Map.of("key", "test_value")),
            new CustomSecretSettings(Map.of("test_key", new SerializableSecureString("test_value")))
        );
    }
}
