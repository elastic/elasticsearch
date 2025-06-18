/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.WeightedToken;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceEmbedding;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.chunking.ChunkingSettingsTests;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.services.AbstractInferenceServiceTests;
import org.elasticsearch.xpack.inference.services.SenderService;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.custom.response.CompletionResponseParser;
import org.elasticsearch.xpack.inference.services.custom.response.CustomResponseParser;
import org.elasticsearch.xpack.inference.services.custom.response.RerankResponseParser;
import org.elasticsearch.xpack.inference.services.custom.response.SparseEmbeddingResponseParser;
import org.elasticsearch.xpack.inference.services.custom.response.TextEmbeddingResponseParser;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.Utils.TIMEOUT;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.custom.response.RerankResponseParser.RERANK_PARSER_DOCUMENT_TEXT;
import static org.elasticsearch.xpack.inference.services.custom.response.RerankResponseParser.RERANK_PARSER_INDEX;
import static org.elasticsearch.xpack.inference.services.custom.response.RerankResponseParser.RERANK_PARSER_SCORE;
import static org.elasticsearch.xpack.inference.services.custom.response.SparseEmbeddingResponseParser.SPARSE_EMBEDDING_TOKEN_PATH;
import static org.elasticsearch.xpack.inference.services.custom.response.SparseEmbeddingResponseParser.SPARSE_EMBEDDING_WEIGHT_PATH;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class CustomServiceTests extends AbstractInferenceServiceTests {

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
            is(Map.of("test_key", new SecureString("test_value".toCharArray())))
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
        var settingsMap = new HashMap<>(
            Map.of(
                CustomServiceSettings.URL,
                "http://www.abc.com",
                CustomServiceSettings.HEADERS,
                Map.of("key", "value"),
                QueryParameters.QUERY_PARAMETERS,
                List.of(List.of("key", "value")),
                CustomServiceSettings.REQUEST,
                "request body",
                CustomServiceSettings.RESPONSE,
                new HashMap<>(Map.of(CustomServiceSettings.JSON_PARSER, createResponseParserMap(taskType)))
            )
        );

        if (taskType == TaskType.TEXT_EMBEDDING) {
            settingsMap.putAll(
                Map.of(
                    ServiceFields.SIMILARITY,
                    SimilarityMeasure.DOT_PRODUCT.toString(),
                    ServiceFields.DIMENSIONS,
                    1536,
                    ServiceFields.MAX_INPUT_TOKENS,
                    512
                )
            );
        }

        return settingsMap;
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
        var inferenceId = "inference_id";

        return new CustomModel(
            inferenceId,
            TaskType.TEXT_EMBEDDING,
            CustomService.NAME,
            new CustomServiceSettings(
                new CustomServiceSettings.TextEmbeddingSettings(similarityMeasure, 123, 456, DenseVectorFieldMapper.ElementType.FLOAT),
                url,
                Map.of("key", "value"),
                QueryParameters.EMPTY,
                "{\"input\":${input}}",
                parser,
                new RateLimitSettings(10_000)
            ),
            new CustomTaskSettings(Map.of("key", "test_value")),
            new CustomSecretSettings(Map.of("test_key", new SecureString("test_value".toCharArray())))
        );
    }

    private static CustomModel createInternalEmbeddingModel(
        @Nullable SimilarityMeasure similarityMeasure,
        TextEmbeddingResponseParser parser,
        String url,
        @Nullable ChunkingSettings chunkingSettings,
        @Nullable Integer batchSize
    ) {
        var inferenceId = "inference_id";

        return new CustomModel(
            inferenceId,
            TaskType.TEXT_EMBEDDING,
            CustomService.NAME,
            new CustomServiceSettings(
                new CustomServiceSettings.TextEmbeddingSettings(similarityMeasure, 123, 456, DenseVectorFieldMapper.ElementType.FLOAT),
                url,
                Map.of("key", "value"),
                QueryParameters.EMPTY,
                "{\"input\":${input}}",
                parser,
                new RateLimitSettings(10_000),
                batchSize
            ),
            new CustomTaskSettings(Map.of("key", "test_value")),
            new CustomSecretSettings(Map.of("test_key", new SecureString("test_value".toCharArray()))),
            chunkingSettings
        );
    }

    private static CustomModel createCustomModel(TaskType taskType, CustomResponseParser customResponseParser, String url) {
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
            new CustomSecretSettings(Map.of("test_key", new SecureString("test_value".toCharArray())))
        );
    }

    private static CustomServiceSettings.TextEmbeddingSettings getDefaultTextEmbeddingSettings(TaskType taskType) {
        return taskType == TaskType.TEXT_EMBEDDING
            ? CustomServiceSettings.TextEmbeddingSettings.DEFAULT_FLOAT
            : CustomServiceSettings.TextEmbeddingSettings.NON_TEXT_EMBEDDING_TASK_TYPE_SETTINGS;
    }

    public void testInfer_ReturnsAnError_WithoutParsingTheResponseBody() throws IOException {
        try (var service = createService(threadPool, clientManager)) {
            String responseJson = "error";

            webServer.enqueue(new MockResponse().setResponseCode(400).setBody(responseJson));

            var model = createInternalEmbeddingModel(new TextEmbeddingResponseParser("$.data[*].embedding"), getUrl(webServer));
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                model,
                null,
                null,
                null,
                List.of("test input"),
                false,
                new HashMap<>(),
                InputType.INTERNAL_SEARCH,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));

            assertThat(
                exception.getMessage(),
                is(
                    Strings.format(
                        "Received an unsuccessful status code for request "
                            + "from inference entity id [inference_id] status [400]. Error message: [%s]",
                        responseJson
                    )
                )
            );
        }
    }

    public void testInfer_HandlesTextEmbeddingRequest_OpenAI_Format() throws IOException {
        try (var service = createService(threadPool, clientManager)) {
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
                  "model": "text-embedding-ada-002-v2",
                  "usage": {
                      "prompt_tokens": 8,
                      "total_tokens": 8
                  }
                }
                """;

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = createInternalEmbeddingModel(new TextEmbeddingResponseParser("$.data[*].embedding"), getUrl(webServer));
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                model,
                null,
                null,
                null,
                List.of("test input"),
                false,
                new HashMap<>(),
                InputType.INTERNAL_SEARCH,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            InferenceServiceResults results = listener.actionGet(TIMEOUT);
            assertThat(results, instanceOf(TextEmbeddingFloatResults.class));

            var embeddingResults = (TextEmbeddingFloatResults) results;
            assertThat(
                embeddingResults.embeddings(),
                is(List.of(new TextEmbeddingFloatResults.Embedding(new float[] { 0.0123F, -0.0123F })))
            );
        }
    }

    public void testInfer_HandlesRerankRequest_Cohere_Format() throws IOException {
        try (var service = createService(threadPool, clientManager)) {
            String responseJson = """
                {
                    "index": "44873262-1315-4c06-8433-fdc90c9790d0",
                    "results": [
                        {
                            "document": {
                                "text": "Washington, D.C.."
                            },
                            "index": 2,
                            "relevance_score": 0.98005307
                        },
                        {
                            "document": {
                                "text": "Capital punishment has existed in the United States since beforethe United States was a country."
                            },
                            "index": 3,
                            "relevance_score": 0.27904198
                        },
                        {
                            "document": {
                                "text": "Carson City is the capital city of the American state of Nevada."
                            },
                            "index": 0,
                            "relevance_score": 0.10194652
                        }
                    ],
                    "meta": {
                        "api_version": {
                            "version": "1"
                        },
                        "billed_units": {
                            "search_units": 1
                        }
                    }
                }
                """;

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = createCustomModel(
                TaskType.RERANK,
                new RerankResponseParser("$.results[*].relevance_score", "$.results[*].index", "$.results[*].document.text"),
                getUrl(webServer)
            );

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                model,
                "query",
                null,
                null,
                List.of("test input"),
                false,
                new HashMap<>(),
                InputType.INTERNAL_SEARCH,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            InferenceServiceResults results = listener.actionGet(TIMEOUT);
            assertThat(results, instanceOf(RankedDocsResults.class));

            var rerankResults = (RankedDocsResults) results;
            assertThat(
                rerankResults.getRankedDocs(),
                is(
                    List.of(
                        new RankedDocsResults.RankedDoc(2, 0.98005307f, "Washington, D.C.."),
                        new RankedDocsResults.RankedDoc(
                            3,
                            0.27904198f,
                            "Capital punishment has existed in the United States since beforethe United States was a country."
                        ),
                        new RankedDocsResults.RankedDoc(0, 0.10194652f, "Carson City is the capital city of the American state of Nevada.")
                    )
                )
            );
        }
    }

    public void testInfer_HandlesCompletionRequest_OpenAI_Format() throws IOException {
        try (var service = createService(threadPool, clientManager)) {
            String responseJson = """
                {
                  "id": "chatcmpl-123",
                  "object": "chat.completion",
                  "created": 1677652288,
                  "model": "gpt-3.5-turbo-0613",
                  "system_fingerprint": "fp_44709d6fcb",
                  "choices": [
                      {
                          "index": 0,
                          "message": {
                              "role": "assistant",
                              "content": "Hello there, how may I assist you today?"
                             },
                          "logprobs": null,
                          "finish_reason": "stop"
                      }
                  ],
                  "usage": {
                    "prompt_tokens": 9,
                    "completion_tokens": 12,
                    "total_tokens": 21
                  }
                }
                """;

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = createCustomModel(
                TaskType.COMPLETION,
                new CompletionResponseParser("$.choices[*].message.content"),
                getUrl(webServer)
            );

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                model,
                null,
                null,
                null,
                List.of("test input"),
                false,
                new HashMap<>(),
                InputType.INTERNAL_SEARCH,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            InferenceServiceResults results = listener.actionGet(TIMEOUT);
            assertThat(results, instanceOf(ChatCompletionResults.class));

            var completionResults = (ChatCompletionResults) results;
            assertThat(
                completionResults.getResults(),
                is(List.of(new ChatCompletionResults.Result("Hello there, how may I assist you today?")))
            );
        }
    }

    public void testInfer_HandlesSparseEmbeddingRequest_Alibaba_Format() throws IOException {
        try (var service = createService(threadPool, clientManager)) {
            String responseJson = """
                {
                    "request_id": "75C50B5B-E79E-4930-****-F48DBB392231",
                    "latency": 22,
                    "usage": {
                        "token_count": 11
                    },
                    "result": {
                        "sparse_embeddings": [
                            {
                                "index": 0,
                                "embedding": [
                                    {
                                        "tokenId": 6,
                                        "weight": 0.101
                                    },
                                    {
                                        "tokenId": 163040,
                                        "weight": 0.28417
                                    }
                                ]
                            }
                        ]
                    }
                }
                """;

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = createCustomModel(
                TaskType.SPARSE_EMBEDDING,
                new SparseEmbeddingResponseParser(
                    "$.result.sparse_embeddings[*].embedding[*].tokenId",
                    "$.result.sparse_embeddings[*].embedding[*].weight"
                ),
                getUrl(webServer)
            );

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                model,
                null,
                null,
                null,
                List.of("test input"),
                false,
                new HashMap<>(),
                InputType.INTERNAL_SEARCH,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            InferenceServiceResults results = listener.actionGet(TIMEOUT);
            assertThat(results, instanceOf(SparseEmbeddingResults.class));

            var sparseEmbeddingResults = (SparseEmbeddingResults) results;
            assertThat(
                sparseEmbeddingResults.embeddings(),
                is(
                    List.of(
                        new SparseEmbeddingResults.Embedding(
                            List.of(new WeightedToken("6", 0.101f), new WeightedToken("163040", 0.28417f)),
                            false
                        )
                    )
                )
            );
        }
    }

    public void testChunkedInfer_ChunkingSettingsSet() throws IOException {
        var model = createInternalEmbeddingModel(
            SimilarityMeasure.DOT_PRODUCT,
            new TextEmbeddingResponseParser("$.data[*].embedding"),
            getUrl(webServer),
            ChunkingSettingsTests.createRandomChunkingSettings(),
            2
        );

        String responseJson = """
            {
              "object": "list",
              "data": [
                  {
                      "object": "embedding",
                      "index": 0,
                      "embedding": [
                          0.123,
                          -0.123
                      ]
                  },
                  {
                      "object": "embedding",
                      "index": 1,
                      "embedding": [
                          0.223,
                          -0.223
                      ]
                  }
              ],
              "model": "text-embedding-ada-002-v2",
              "usage": {
                  "prompt_tokens": 8,
                  "total_tokens": 8
              }
            }
            """;

        try (var service = createService(threadPool, clientManager)) {
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            PlainActionFuture<List<ChunkedInference>> listener = new PlainActionFuture<>();
            service.chunkedInfer(
                model,
                null,
                List.of(new ChunkInferenceInput("a"), new ChunkInferenceInput("bb")),
                new HashMap<>(),
                InputType.INTERNAL_INGEST,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var results = listener.actionGet(TIMEOUT);
            assertThat(results, hasSize(2));
            {
                assertThat(results.get(0), CoreMatchers.instanceOf(ChunkedInferenceEmbedding.class));
                var floatResult = (ChunkedInferenceEmbedding) results.get(0);
                assertThat(floatResult.chunks(), hasSize(1));
                assertEquals(new ChunkedInference.TextOffset(0, 1), floatResult.chunks().get(0).offset());
                assertThat(floatResult.chunks().get(0).embedding(), Matchers.instanceOf(TextEmbeddingFloatResults.Embedding.class));
                assertArrayEquals(
                    new float[] { 0.123f, -0.123f },
                    ((TextEmbeddingFloatResults.Embedding) floatResult.chunks().get(0).embedding()).values(),
                    0.0f
                );
            }
            {
                assertThat(results.get(1), CoreMatchers.instanceOf(ChunkedInferenceEmbedding.class));
                var floatResult = (ChunkedInferenceEmbedding) results.get(1);
                assertThat(floatResult.chunks(), hasSize(1));
                assertEquals(new ChunkedInference.TextOffset(0, 2), floatResult.chunks().get(0).offset());
                assertThat(floatResult.chunks().get(0).embedding(), Matchers.instanceOf(TextEmbeddingFloatResults.Embedding.class));
                assertArrayEquals(
                    new float[] { 0.223f, -0.223f },
                    ((TextEmbeddingFloatResults.Embedding) floatResult.chunks().get(0).embedding()).values(),
                    0.0f
                );
            }

            assertThat(webServer.requests(), hasSize(1));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.size(), is(1));
            assertThat(requestMap.get("input"), is(List.of("a", "bb")));
        }
    }

    public void testChunkedInfer_ChunkingSettingsNotSet() throws IOException {
        var model = createInternalEmbeddingModel(new TextEmbeddingResponseParser("$.data[*].embedding"), getUrl(webServer));
        String responseJson = """
            {
              "object": "list",
              "data": [
                  {
                      "object": "embedding",
                      "index": 0,
                      "embedding": [
                          0.123,
                          -0.123
                      ]
                  }
              ],
              "model": "text-embedding-ada-002-v2",
              "usage": {
                  "prompt_tokens": 8,
                  "total_tokens": 8
              }
            }
            """;

        try (var service = createService(threadPool, clientManager)) {
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            PlainActionFuture<List<ChunkedInference>> listener = new PlainActionFuture<>();
            service.chunkedInfer(
                model,
                null,
                List.of(new ChunkInferenceInput("a")),
                new HashMap<>(),
                InputType.INTERNAL_INGEST,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var results = listener.actionGet(TIMEOUT);
            assertThat(results, hasSize(1));
            {
                assertThat(results.get(0), CoreMatchers.instanceOf(ChunkedInferenceEmbedding.class));
                var floatResult = (ChunkedInferenceEmbedding) results.get(0);
                assertThat(floatResult.chunks(), hasSize(1));
                assertEquals(new ChunkedInference.TextOffset(0, 1), floatResult.chunks().get(0).offset());
                assertThat(floatResult.chunks().get(0).embedding(), Matchers.instanceOf(TextEmbeddingFloatResults.Embedding.class));
                assertArrayEquals(
                    new float[] { 0.123f, -0.123f },
                    ((TextEmbeddingFloatResults.Embedding) floatResult.chunks().get(0).embedding()).values(),
                    0.0f
                );
            }

            assertThat(webServer.requests(), hasSize(1));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.size(), is(1));
            assertThat(requestMap.get("input"), is(List.of("a")));
        }
    }
}
