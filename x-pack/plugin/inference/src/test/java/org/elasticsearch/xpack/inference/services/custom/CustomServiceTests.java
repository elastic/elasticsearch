/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.RerankRequest;
import org.elasticsearch.inference.RerankingInferenceService;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.WeightedToken;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsTests;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceEmbedding;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.InferenceServiceTestCase;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.custom.response.CompletionResponseParser;
import org.elasticsearch.xpack.inference.services.custom.response.DenseEmbeddingResponseParser;
import org.elasticsearch.xpack.inference.services.custom.response.RerankResponseParser;
import org.elasticsearch.xpack.inference.services.custom.response.SparseEmbeddingResponseParser;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.inference.InferenceStringTests.createRandomUsingDataTypes;
import static org.elasticsearch.xpack.inference.Utils.TIMEOUT;
import static org.elasticsearch.xpack.inference.Utils.getRequestConfigMap;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.custom.CustomServiceParameterizedTestConfiguration.URL_VALUE;
import static org.elasticsearch.xpack.inference.services.custom.CustomServiceParameterizedTestConfiguration.createCustomModel;
import static org.elasticsearch.xpack.inference.services.custom.CustomServiceParameterizedTestConfiguration.createInternalEmbeddingModel;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CustomServiceTests extends InferenceServiceTestCase {

    public void testInfer_ReturnsAnError_WithoutParsingTheResponseBody() throws IOException {
        try (var service = createInferenceService()) {
            String responseJson = "error";

            webServer.enqueue(new MockResponse().setResponseCode(400).setBody(responseJson));

            var model = createInternalEmbeddingModel(
                new DenseEmbeddingResponseParser("$.data[*].embedding", CustomServiceEmbeddingType.FLOAT),
                getUrl(webServer)
            );
            TestPlainActionFuture<InferenceServiceResults> listener = new TestPlainActionFuture<>();
            service.infer(
                model,
                null,
                null,
                null,
                List.of("test input"),
                false,
                new HashMap<>(),
                InputType.INTERNAL_SEARCH,
                null,
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
        try (var service = createInferenceService()) {
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

            var model = createInternalEmbeddingModel(
                new DenseEmbeddingResponseParser("$.data[*].embedding", CustomServiceEmbeddingType.FLOAT),
                getUrl(webServer)
            );
            TestPlainActionFuture<InferenceServiceResults> listener = new TestPlainActionFuture<>();
            service.infer(
                model,
                null,
                null,
                null,
                List.of("test input"),
                false,
                new HashMap<>(),
                InputType.INTERNAL_SEARCH,
                null,
                listener
            );

            InferenceServiceResults results = listener.actionGet(TIMEOUT);
            assertThat(results, instanceOf(DenseEmbeddingFloatResults.class));

            var embeddingResults = (DenseEmbeddingFloatResults) results;
            assertThat(
                embeddingResults.embeddings(),
                is(List.of(new DenseEmbeddingFloatResults.Embedding(new float[] { 0.0123F, -0.0123F })))
            );
        }
    }

    public void testRerankInfer_ThrowsError_WithNonTextQuery() throws IOException {
        var textInputs = randomList(1, 5, () -> createRandomUsingDataTypes(EnumSet.of(DataType.TEXT)));
        var nonTextQuery = createRandomUsingDataTypes(EnumSet.complementOf(EnumSet.of(DataType.TEXT)));
        testRerankInfer_ThrowsError_WithNonTextInputOrQuery(textInputs, nonTextQuery);
    }

    public void testRerankInfer_ThrowsError_WithNonTextInputs() throws IOException {
        var nonTextInputs = randomList(1, 5, () -> createRandomUsingDataTypes(EnumSet.complementOf(EnumSet.of(DataType.TEXT))));
        var textQuery = createRandomUsingDataTypes(EnumSet.of(DataType.TEXT));
        testRerankInfer_ThrowsError_WithNonTextInputOrQuery(nonTextInputs, textQuery);
    }

    public void testRerankInfer_ThrowsError_WithNonTextInputsAndQuery() throws IOException {
        var nonTextInputs = randomList(1, 5, () -> createRandomUsingDataTypes(EnumSet.complementOf(EnumSet.of(DataType.TEXT))));
        var nonTextQuery = createRandomUsingDataTypes(EnumSet.complementOf(EnumSet.of(DataType.TEXT)));
        testRerankInfer_ThrowsError_WithNonTextInputOrQuery(nonTextInputs, nonTextQuery);
    }

    private void testRerankInfer_ThrowsError_WithNonTextInputOrQuery(List<InferenceString> inputs, InferenceString query)
        throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        var model = mock(CustomModel.class);
        when(model.getTaskType()).thenReturn(TaskType.RERANK);

        try (var service = new CustomService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            TestPlainActionFuture<InferenceServiceResults> listener = new TestPlainActionFuture<>();

            service.rerankInfer(model, new RerankRequest(inputs, query, null, null, new HashMap<>()), null, listener);

            var thrownException = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(thrownException.status(), CoreMatchers.is(RestStatus.BAD_REQUEST));
            assertThat(
                thrownException.getMessage(),
                CoreMatchers.is("The custom service does not support rerank with non-text inputs or queries")
            );
        }
    }

    public void testRerankInfer_Cohere_Format() throws IOException {
        try (var service = createInferenceService()) {
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
                getUrl(webServer),
                null
            );

            TestPlainActionFuture<InferenceServiceResults> listener = new TestPlainActionFuture<>();

            var inputOne = randomAlphanumericOfLength(8);
            var inputTwo = randomAlphanumericOfLength(8);
            var query = randomAlphanumericOfLength(8);
            var request = new RerankRequest(
                List.of(new InferenceString(DataType.TEXT, inputOne), new InferenceString(DataType.TEXT, inputTwo)),
                new InferenceString(DataType.TEXT, query),
                null,
                null,
                null
            );
            service.rerankInfer(model, request, null, listener);

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
        try (var service = createInferenceService()) {
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
                getUrl(webServer),
                null
            );

            TestPlainActionFuture<InferenceServiceResults> listener = new TestPlainActionFuture<>();
            service.infer(
                model,
                null,
                null,
                null,
                List.of("test input"),
                false,
                new HashMap<>(),
                InputType.INTERNAL_SEARCH,
                null,
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
        try (var service = createInferenceService()) {
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
                getUrl(webServer),
                null
            );

            TestPlainActionFuture<InferenceServiceResults> listener = new TestPlainActionFuture<>();
            service.infer(
                model,
                null,
                null,
                null,
                List.of("test input"),
                false,
                new HashMap<>(),
                InputType.INTERNAL_SEARCH,
                null,
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

    public void testParseRequestConfig_ThrowsAValidationError_WhenReplacementDoesNotFillTemplate() throws Exception {
        try (var service = createInferenceService()) {

            var settingsMap = new HashMap<String, Object>(
                Map.of(
                    ServiceFields.URL,
                    "http://www.abc.com",
                    CustomServiceSettings.REQUEST,
                    "request body ${some_template}",
                    CustomServiceSettings.RESPONSE,
                    new HashMap<>(
                        Map.of(
                            CustomServiceSettings.JSON_PARSER,
                            new HashMap<>(Map.of(CompletionResponseParser.COMPLETION_PARSER_RESULT, "$.result.text"))
                        )
                    )
                )
            );

            var config = getRequestConfigMap(settingsMap, Map.of(), Map.of());

            var listener = new TestPlainActionFuture<Model>();
            service.parseRequestConfig("id", TaskType.COMPLETION, config, listener);

            var exception = expectThrows(ValidationException.class, () -> listener.actionGet(TIMEOUT));

            assertThat(
                exception.getMessage(),
                is(
                    "Validation Failed: 1: Failed to validate model configuration: Found placeholder "
                        + "[${some_template}] in field [request] after replacement call, please check that all "
                        + "templates have a corresponding field definition.;"
                )
            );
        }
    }

    public void testChunkedInfer_DenseEmbeddings_ChunkingSettingsSet() throws IOException {
        var model = createInternalEmbeddingModel(
            SimilarityMeasure.DOT_PRODUCT,
            new DenseEmbeddingResponseParser("$.data[*].embedding", CustomServiceEmbeddingType.FLOAT),
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

        try (var service = createInferenceService()) {
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            TestPlainActionFuture<List<ChunkedInference>> listener = new TestPlainActionFuture<>();
            service.chunkedInfer(
                model,
                null,
                List.of(new ChunkInferenceInput("a"), new ChunkInferenceInput("bb")),
                new HashMap<>(),
                InputType.INTERNAL_INGEST,
                null,
                listener
            );

            var results = listener.actionGet(TIMEOUT);
            assertThat(results, hasSize(2));
            {
                assertThat(results.get(0), CoreMatchers.instanceOf(ChunkedInferenceEmbedding.class));
                var floatResult = (ChunkedInferenceEmbedding) results.get(0);
                assertThat(floatResult.chunks(), hasSize(1));
                assertEquals(new ChunkedInference.TextOffset(0, 1), floatResult.chunks().get(0).offset());
                assertThat(floatResult.chunks().get(0).embedding(), Matchers.instanceOf(DenseEmbeddingFloatResults.Embedding.class));
                assertArrayEquals(
                    new float[] { 0.123f, -0.123f },
                    ((DenseEmbeddingFloatResults.Embedding) floatResult.chunks().get(0).embedding()).values(),
                    0.0f
                );
            }
            {
                assertThat(results.get(1), CoreMatchers.instanceOf(ChunkedInferenceEmbedding.class));
                var floatResult = (ChunkedInferenceEmbedding) results.get(1);
                assertThat(floatResult.chunks(), hasSize(1));
                assertEquals(new ChunkedInference.TextOffset(0, 2), floatResult.chunks().get(0).offset());
                assertThat(floatResult.chunks().get(0).embedding(), Matchers.instanceOf(DenseEmbeddingFloatResults.Embedding.class));
                assertArrayEquals(
                    new float[] { 0.223f, -0.223f },
                    ((DenseEmbeddingFloatResults.Embedding) floatResult.chunks().get(0).embedding()).values(),
                    0.0f
                );
            }

            assertThat(webServer.requests(), hasSize(1));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.size(), is(1));
            assertThat(requestMap.get("input"), is(List.of("a", "bb")));
        }
    }

    public void testChunkedInfer_DenseEmbeddings_ChunkingSettingsNotSet() throws IOException {
        var model = createInternalEmbeddingModel(
            new DenseEmbeddingResponseParser("$.data[*].embedding", CustomServiceEmbeddingType.FLOAT),
            getUrl(webServer)
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
                  }
              ],
              "model": "text-embedding-ada-002-v2",
              "usage": {
                  "prompt_tokens": 8,
                  "total_tokens": 8
              }
            }
            """;

        try (var service = createInferenceService()) {
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            TestPlainActionFuture<List<ChunkedInference>> listener = new TestPlainActionFuture<>();
            service.chunkedInfer(
                model,
                null,
                List.of(new ChunkInferenceInput("a")),
                new HashMap<>(),
                InputType.INTERNAL_INGEST,
                null,
                listener
            );

            var results = listener.actionGet(TIMEOUT);
            assertThat(results, hasSize(1));
            {
                assertThat(results.get(0), CoreMatchers.instanceOf(ChunkedInferenceEmbedding.class));
                var floatResult = (ChunkedInferenceEmbedding) results.get(0);
                assertThat(floatResult.chunks(), hasSize(1));
                assertEquals(new ChunkedInference.TextOffset(0, 1), floatResult.chunks().get(0).offset());
                assertThat(floatResult.chunks().get(0).embedding(), Matchers.instanceOf(DenseEmbeddingFloatResults.Embedding.class));
                assertArrayEquals(
                    new float[] { 0.123f, -0.123f },
                    ((DenseEmbeddingFloatResults.Embedding) floatResult.chunks().get(0).embedding()).values(),
                    0.0f
                );
            }

            assertThat(webServer.requests(), hasSize(1));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.size(), is(1));
            assertThat(requestMap.get("input"), is(List.of("a")));
        }
    }

    public void testChunkedInfer_SparseEmbeddings_ChunkingSettingsSet() throws IOException {
        var model = createCustomModel(
            TaskType.SPARSE_EMBEDDING,
            new SparseEmbeddingResponseParser(
                "$.result.sparse_embeddings[*].embedding[*].tokenId",
                "$.result.sparse_embeddings[*].embedding[*].weight"
            ),
            getUrl(webServer),
            ChunkingSettingsTests.createRandomChunkingSettings()
        );

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
                            },
                            {
                                "index": 1,
                                "embedding": [
                                    {
                                        "tokenId": 4,
                                        "weight": 0.201
                                    },
                                    {
                                        "tokenId": 153040,
                                        "weight": 0.24417
                                    }
                                ]
                            }
                        ]
                    }
                }
            """;

        try (var service = createInferenceService()) {
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            TestPlainActionFuture<List<ChunkedInference>> listener = new TestPlainActionFuture<>();
            service.chunkedInfer(
                model,
                null,
                List.of(new ChunkInferenceInput("a"), new ChunkInferenceInput("bb")),
                new HashMap<>(),
                InputType.INTERNAL_INGEST,
                null,
                listener
            );

            var results = listener.actionGet(TIMEOUT);
            assertThat(results, hasSize(2));

            // Check first embedding
            {
                assertThat(results.get(0), CoreMatchers.instanceOf(ChunkedInferenceEmbedding.class));
                var sparseEmbeddingResult = (ChunkedInferenceEmbedding) results.get(0);
                assertThat(sparseEmbeddingResult.chunks(), hasSize(1));
                assertEquals(new ChunkedInference.TextOffset(0, 1), sparseEmbeddingResult.chunks().get(0).offset());
                assertThat(sparseEmbeddingResult.chunks().get(0).embedding(), Matchers.instanceOf(SparseEmbeddingResults.Embedding.class));
                assertThat(
                    ((SparseEmbeddingResults.Embedding) sparseEmbeddingResult.chunks().get(0).embedding()),
                    equalTo(
                        new SparseEmbeddingResults.Embedding(
                            List.of(new WeightedToken("6", 0.101f), new WeightedToken("163040", 0.28417f)),
                            false
                        )
                    )
                );
            }

            // Check second embedding
            {
                assertThat(results.get(1), CoreMatchers.instanceOf(ChunkedInferenceEmbedding.class));
                var sparseEmbeddingResult = (ChunkedInferenceEmbedding) results.get(1);
                assertThat(sparseEmbeddingResult.chunks(), hasSize(1));
                assertEquals(new ChunkedInference.TextOffset(0, 2), sparseEmbeddingResult.chunks().get(0).offset());
                assertThat(sparseEmbeddingResult.chunks().get(0).embedding(), Matchers.instanceOf(SparseEmbeddingResults.Embedding.class));
                assertThat(
                    ((SparseEmbeddingResults.Embedding) sparseEmbeddingResult.chunks().get(0).embedding()),
                    equalTo(
                        new SparseEmbeddingResults.Embedding(
                            List.of(new WeightedToken("4", 0.201f), new WeightedToken("153040", 0.24417f)),
                            false
                        )
                    )
                );
            }

            assertThat(webServer.requests(), hasSize(1));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.size(), is(1));
            assertThat(requestMap.get("input"), is(List.of("a", "bb")));
        }
    }

    public void testChunkedInfer_SparseEmbeddings_ChunkingSettingsNotSet() throws IOException {
        var model = createCustomModel(
            TaskType.SPARSE_EMBEDDING,
            new SparseEmbeddingResponseParser(
                "$.result.sparse_embeddings[*].embedding[*].tokenId",
                "$.result.sparse_embeddings[*].embedding[*].weight"
            ),
            getUrl(webServer),
            null // chunking not explicitly set
        );

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
                            },
                            {
                                "index": 1,
                                "embedding": [
                                    {
                                        "tokenId": 4,
                                        "weight": 0.201
                                    },
                                    {
                                        "tokenId": 153040,
                                        "weight": 0.24417
                                    }
                                ]
                            }
                        ]
                    }
                }
            """;

        try (var service = createInferenceService()) {
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            TestPlainActionFuture<List<ChunkedInference>> listener = new TestPlainActionFuture<>();
            service.chunkedInfer(
                model,
                null,
                List.of(new ChunkInferenceInput("a"), new ChunkInferenceInput("bb")),
                new HashMap<>(),
                InputType.INTERNAL_INGEST,
                null,
                listener
            );

            var results = listener.actionGet(TIMEOUT);
            assertThat(results, hasSize(2));

            // Check first embedding
            {
                assertThat(results.get(0), CoreMatchers.instanceOf(ChunkedInferenceEmbedding.class));
                var sparseEmbeddingResult = (ChunkedInferenceEmbedding) results.get(0);
                assertThat(sparseEmbeddingResult.chunks(), hasSize(1));
                assertEquals(new ChunkedInference.TextOffset(0, 1), sparseEmbeddingResult.chunks().get(0).offset());
                assertThat(sparseEmbeddingResult.chunks().get(0).embedding(), Matchers.instanceOf(SparseEmbeddingResults.Embedding.class));
                assertThat(
                    ((SparseEmbeddingResults.Embedding) sparseEmbeddingResult.chunks().get(0).embedding()),
                    equalTo(
                        new SparseEmbeddingResults.Embedding(
                            List.of(new WeightedToken("6", 0.101f), new WeightedToken("163040", 0.28417f)),
                            false
                        )
                    )
                );
            }

            // Check second embedding
            {
                assertThat(results.get(1), CoreMatchers.instanceOf(ChunkedInferenceEmbedding.class));
                var sparseEmbeddingResult = (ChunkedInferenceEmbedding) results.get(1);
                assertThat(sparseEmbeddingResult.chunks(), hasSize(1));
                assertEquals(new ChunkedInference.TextOffset(0, 2), sparseEmbeddingResult.chunks().get(0).offset());
                assertThat(sparseEmbeddingResult.chunks().get(0).embedding(), Matchers.instanceOf(SparseEmbeddingResults.Embedding.class));
                assertThat(
                    ((SparseEmbeddingResults.Embedding) sparseEmbeddingResult.chunks().get(0).embedding()),
                    equalTo(
                        new SparseEmbeddingResults.Embedding(
                            List.of(new WeightedToken("4", 0.201f), new WeightedToken("153040", 0.24417f)),
                            false
                        )
                    )
                );
            }

            assertThat(webServer.requests(), hasSize(1));

            // Check request
            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.size(), is(1));
            assertThat(requestMap.get("input"), is(List.of("a", "bb")));
        }
    }

    public void testChunkedInfer_noInputs() throws IOException {
        var model = createInternalEmbeddingModel(
            new DenseEmbeddingResponseParser("$.data[*].embedding", CustomServiceEmbeddingType.FLOAT),
            getUrl(webServer)
        );

        try (var service = createInferenceService()) {

            TestPlainActionFuture<List<ChunkedInference>> listener = new TestPlainActionFuture<>();
            service.chunkedInfer(model, null, List.of(), new HashMap<>(), InputType.INTERNAL_INGEST, null, listener);

            var results = listener.actionGet(TIMEOUT);
            assertThat(results, empty());
            assertThat(webServer.requests(), empty());
        }
    }

    @Override
    public InferenceService createInferenceService() {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        return new CustomService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty());
    }

    @Override
    public Model createEmbeddingModel(SimilarityMeasure similarity) {
        HashMap<String, Object> settingsMap = new HashMap<>(
            Map.of(
                ServiceFields.URL,
                URL_VALUE,
                CustomServiceSettings.REQUEST,
                randomAlphaOfLength(8),
                CustomServiceSettings.RESPONSE,
                new HashMap<>(
                    Map.of(
                        CustomServiceSettings.JSON_PARSER,
                        new HashMap<>(
                            Map.of(DenseEmbeddingResponseParser.TEXT_EMBEDDING_PARSER_EMBEDDINGS, "$.result.embeddings[*].embedding")
                        )
                    )
                )
            )
        );
        if (similarity != null) {
            settingsMap.put(ServiceFields.SIMILARITY, similarity.toString());
        }
        var settings = CustomServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST, TaskType.TEXT_EMBEDDING);
        return CustomModelTests.createModel(
            randomAlphaOfLength(8),
            TaskType.TEXT_EMBEDDING,
            settings,
            CustomTaskSettings.EMPTY_SETTINGS,
            null
        );
    }

    @Override
    public EnumSet<TaskType> expectedStreamingTasks() {
        return EnumSet.noneOf(TaskType.class);
    }

    @Override
    protected void assertRerankerWindowSize(RerankingInferenceService rerankingInferenceService) {
        assertThat(rerankingInferenceService.rerankerWindowSize("any model"), CoreMatchers.is(300));
    }
}
