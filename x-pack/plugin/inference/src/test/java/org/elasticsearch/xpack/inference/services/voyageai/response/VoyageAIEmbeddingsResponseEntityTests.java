/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.response;

import org.apache.http.HttpResponse;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InferenceStringGroup;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingByteResults;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.InputTypeTests;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.services.voyageai.request.VoyageAIEmbeddingsRequest;
import org.elasticsearch.xpack.inference.services.voyageai.request.VoyageAIMultimodalEmbeddingsRequest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.elasticsearch.xpack.inference.services.voyageai.embeddings.text.VoyageAIEmbeddingsModelTests.createModel;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class VoyageAIEmbeddingsResponseEntityTests extends ESTestCase {

    /**
     * Helper method to create InferenceStringGroups from text strings.
     */
    private static List<InferenceStringGroup> toInferenceStringGroups(String... texts) {
        return java.util.Arrays.stream(texts).map(InferenceStringGroup::new).toList();
    }

    public void testFromResponse_CreatesResultsForASingleItem() throws IOException {
        String responseJson = """
            {
              "object": "list",
              "data": [
                  {
                      "object": "embedding",
                      "index": 0,
                      "embedding": [
                          0.014539449,
                          -0.015288644
                      ]
                  }
              ],
              "model": "voyage-3-large",
              "usage": {
                  "total_tokens": 8
              }
            }
            """;

        VoyageAIEmbeddingsRequest request = new VoyageAIEmbeddingsRequest(
            List.of("abc", "def"),
            InputTypeTests.randomSearchAndIngestWithNull(),
            createModel("url", "api_key", null, "voyage-3-large")
        );

        InferenceServiceResults parsedResults = VoyageAIEmbeddingsResponseEntity.fromResponse(
            request,
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(
            ((DenseEmbeddingFloatResults) parsedResults).embeddings(),
            is(List.of(new DenseEmbeddingFloatResults.Embedding(new float[] { 0.014539449F, -0.015288644F })))
        );
    }

    public void testFromResponse_CreatesResultsForMultipleItems() throws IOException {
        String responseJson = """
            {
              "object": "list",
              "data": [
                  {
                      "object": "embedding",
                      "index": 0,
                      "embedding": [
                          0.014539449,
                          -0.015288644
                      ]
                  },
                  {
                      "object": "embedding",
                      "index": 1,
                      "embedding": [
                          0.0123,
                          -0.0123
                      ]
                  }
              ],
              "model": "voyage-3-large",
              "usage": {
                  "total_tokens": 8
              }
            }
            """;

        VoyageAIEmbeddingsRequest request = new VoyageAIEmbeddingsRequest(
            List.of("abc", "def"),
            InputTypeTests.randomSearchAndIngestWithNull(),
            createModel("url", "api_key", null, "voyage-3-large")
        );

        InferenceServiceResults parsedResults = VoyageAIEmbeddingsResponseEntity.fromResponse(
            request,
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(
            ((DenseEmbeddingFloatResults) parsedResults).embeddings(),
            is(
                List.of(
                    new DenseEmbeddingFloatResults.Embedding(new float[] { 0.014539449F, -0.015288644F }),
                    new DenseEmbeddingFloatResults.Embedding(new float[] { 0.0123F, -0.0123F })
                )
            )
        );
    }

    public void testFromResponse_FailsWhenDataFieldIsNotPresent() {
        String responseJson = """
            {
              "object": "list",
              "not_data": [
                  {
                      "object": "embedding",
                      "index": 0,
                      "embedding": [
                          0.014539449,
                          -0.015288644
                      ]
                  }
              ],
              "model": "voyage-3-large",
              "usage": {
                  "total_tokens": 8
              }
            }
            """;

        VoyageAIEmbeddingsRequest request = new VoyageAIEmbeddingsRequest(
            List.of("abc", "def"),
            InputTypeTests.randomSearchAndIngestWithNull(),
            createModel("url", "api_key", null, "voyage-3-large")
        );

        var thrownException = expectThrows(
            java.lang.IllegalArgumentException.class,
            () -> VoyageAIEmbeddingsResponseEntity.fromResponse(
                request,
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(thrownException.getMessage(), is("Required [data]"));
    }

    public void testFromResponse_FailsWhenDataFieldNotAnArray() {
        String responseJson = """
            {
              "object": "list",
              "data": {
                  "test": {
                      "object": "embedding",
                      "index": 0,
                      "embedding": [
                          0.014539449,
                          -0.015288644
                      ]
                  }
              },
              "model": "voyage-3-large",
              "usage": {
                  "total_tokens": 8
              }
            }
            """;

        VoyageAIEmbeddingsRequest request = new VoyageAIEmbeddingsRequest(
            List.of("abc", "def"),
            InputTypeTests.randomSearchAndIngestWithNull(),
            createModel("url", "api_key", null, "voyage-3-large")
        );

        var thrownException = expectThrows(
            XContentParseException.class,
            () -> VoyageAIEmbeddingsResponseEntity.fromResponse(
                request,
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(thrownException.getMessage(), containsString("[EmbeddingFloatResult] failed to parse field [data]"));
    }

    public void testFromResponse_FailsWhenEmbeddingsDoesNotExist() {
        String responseJson = """
            {
              "object": "list",
              "data": [
                  {
                      "object": "embedding",
                      "index": 0,
                      "embeddingzzz": [
                          0.014539449,
                          -0.015288644
                      ]
                  }
              ],
              "model": "voyage-3-large",
              "usage": {
                  "total_tokens": 8
              }
            }
            """;

        VoyageAIEmbeddingsRequest request = new VoyageAIEmbeddingsRequest(
            List.of("abc", "def"),
            InputTypeTests.randomSearchAndIngestWithNull(),
            createModel("url", "api_key", null, "voyage-3-large")
        );

        var thrownException = expectThrows(
            XContentParseException.class,
            () -> VoyageAIEmbeddingsResponseEntity.fromResponse(
                request,
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(thrownException.getMessage(), containsString("[EmbeddingFloatResult] failed to parse field [data]"));
    }

    public void testFromResponse_FailsWhenEmbeddingValueIsAString() {
        String responseJson = """
            {
              "object": "list",
              "data": [
                  {
                      "object": "embedding",
                      "index": 0,
                      "embedding": [
                          "abc"
                      ]
                  }
              ],
              "model": "voyage-3-large",
              "usage": {
                  "total_tokens": 8
              }
            }
            """;

        VoyageAIEmbeddingsRequest request = new VoyageAIEmbeddingsRequest(
            List.of("abc", "def"),
            InputTypeTests.randomSearchAndIngestWithNull(),
            createModel("url", "api_key", null, "voyage-3-large")
        );

        var thrownException = expectThrows(
            XContentParseException.class,
            () -> VoyageAIEmbeddingsResponseEntity.fromResponse(
                request,
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(thrownException.getMessage(), is("[8:15] [EmbeddingFloatResult] failed to parse field [data]"));
    }

    public void testFromResponse_SucceedsWhenEmbeddingValueIsInt() throws IOException {
        String responseJson = """
            {
              "object": "list",
              "data": [
                  {
                      "object": "embedding",
                      "index": 0,
                      "embedding": [
                          1
                      ]
                  }
              ],
              "model": "voyage-3-large",
              "usage": {
                  "total_tokens": 8
              }
            }
            """;

        VoyageAIEmbeddingsRequest request = new VoyageAIEmbeddingsRequest(
            List.of("abc", "def"),
            InputTypeTests.randomSearchAndIngestWithNull(),
            createModel("url", "api_key", null, "voyage-3-large")
        );

        InferenceServiceResults parsedResults = VoyageAIEmbeddingsResponseEntity.fromResponse(
            request,
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(
            ((DenseEmbeddingFloatResults) parsedResults).embeddings(),
            is(List.of(new DenseEmbeddingFloatResults.Embedding(new float[] { 1.0F })))
        );
    }

    public void testFromResponse_SucceedsWhenEmbeddingValueIsLong() throws IOException {
        String responseJson = """
            {
              "object": "list",
              "data": [
                  {
                      "object": "embedding",
                      "index": 0,
                      "embedding": [
                          40294967295
                      ]
                  }
              ],
              "model": "voyage-3-large",
              "usage": {
                  "total_tokens": 8
              }
            }
            """;

        VoyageAIEmbeddingsRequest request = new VoyageAIEmbeddingsRequest(
            List.of("abc", "def"),
            InputTypeTests.randomSearchAndIngestWithNull(),
            createModel("url", "api_key", null, "voyage-3-large")
        );

        InferenceServiceResults parsedResults = VoyageAIEmbeddingsResponseEntity.fromResponse(
            request,
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(
            ((DenseEmbeddingFloatResults) parsedResults).embeddings(),
            is(List.of(new DenseEmbeddingFloatResults.Embedding(new float[] { 4.0294965E10F })))
        );
    }

    public void testFromResponse_FailsWhenEmbeddingValueIsAnObject() {
        String responseJson = """
            {
              "object": "list",
              "data": [
                  {
                      "object": "embedding",
                      "index": 0,
                      "embedding": [
                          {}
                      ]
                  }
              ],
              "model": "voyage-3-large",
              "usage": {
                  "total_tokens": 8
              }
            }
            """;

        VoyageAIEmbeddingsRequest request = new VoyageAIEmbeddingsRequest(
            List.of("abc", "def"),
            InputTypeTests.randomSearchAndIngestWithNull(),
            createModel("url", "api_key", null, "voyage-3-large")
        );

        var thrownException = expectThrows(
            XContentParseException.class,
            () -> VoyageAIEmbeddingsResponseEntity.fromResponse(
                request,
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(thrownException.getMessage(), is("[8:15] [EmbeddingFloatResult] failed to parse field [data]"));
    }

    public void testFieldsInDifferentOrderServer() throws IOException {
        // The fields of the objects in the data array are reordered
        String response = """
            {
                "object": "list",
                "model": "voyage-3-large",
                "data": [
                    {
                        "embedding": [
                            -0.9,
                            0.5,
                            0.3
                        ],
                        "index": 0,
                        "object": "embedding"
                    },
                    {
                        "index": 0,
                        "embedding": [
                            0.1,
                            0.5
                        ],
                        "object": "embedding"
                    },
                    {
                        "object": "embedding",
                        "index": 0,
                        "embedding": [
                            0.5,
                            0.5
                        ]
                    }
                ],
                "usage": {
                    "total_tokens": 0
                }
            }""";

        VoyageAIEmbeddingsRequest request = new VoyageAIEmbeddingsRequest(
            List.of("abc", "def"),
            InputTypeTests.randomSearchAndIngestWithNull(),
            createModel("url", "api_key", null, "voyage-3-large")
        );

        InferenceServiceResults parsedResults = VoyageAIEmbeddingsResponseEntity.fromResponse(
            request,
            new HttpResult(mock(HttpResponse.class), response.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(parsedResults, instanceOf(DenseEmbeddingFloatResults.class));

        assertThat(
            ((DenseEmbeddingFloatResults) parsedResults).embeddings(),
            is(
                List.of(
                    new DenseEmbeddingFloatResults.Embedding(new float[] { -0.9F, 0.5F, 0.3F }),
                    new DenseEmbeddingFloatResults.Embedding(new float[] { 0.1F, 0.5F }),
                    new DenseEmbeddingFloatResults.Embedding(new float[] { 0.5F, 0.5F })
                )
            )
        );
    }

    public void testFromResponse_HandlesMultimodalRequest_WithFloatEmbeddings() throws IOException {
        String responseJson = """
            {
              "object": "list",
              "data": [
                  {
                      "object": "embedding",
                      "index": 0,
                      "embedding": [
                          0.014539449,
                          -0.015288644
                      ]
                  }
              ],
              "model": "voyage-multimodal-3",
              "usage": {
                  "total_tokens": 8
              }
            }
            """;

        VoyageAIMultimodalEmbeddingsRequest request = new VoyageAIMultimodalEmbeddingsRequest(
            toInferenceStringGroups("abc", "def"),
            InputTypeTests.randomSearchAndIngestWithNull(),
            org.elasticsearch.xpack.inference.services.voyageai.embeddings.multimodal
                .VoyageAIMultimodalEmbeddingsModelTests.createModel(
                    "url",
                    "api_key",
                    null,
                    "voyage-multimodal-3"
                )
        );

        InferenceServiceResults parsedResults = VoyageAIEmbeddingsResponseEntity.fromResponse(
            request,
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(parsedResults, instanceOf(DenseEmbeddingFloatResults.class));
        assertThat(
            ((DenseEmbeddingFloatResults) parsedResults).embeddings(),
            is(List.of(new DenseEmbeddingFloatResults.Embedding(new float[] { 0.014539449F, -0.015288644F })))
        );
    }

    public void testFromResponse_HandlesMultimodalRequest_WithInt8Embeddings() throws IOException {
        String responseJson = """
            {
              "object": "list",
              "data": [
                  {
                      "object": "embedding",
                      "index": 0,
                      "embedding": [
                          100,
                          -50
                      ]
                  }
              ],
              "model": "voyage-multimodal-3",
              "usage": {
                  "total_tokens": 8
              }
            }
            """;

        var multimodalModel = org.elasticsearch.xpack.inference.services.voyageai.embeddings.multimodal
            .VoyageAIMultimodalEmbeddingsModelTests.createModel("url", "api_key", null, "voyage-multimodal-3");
        // Create a model with INT8 embedding type
        var modelWithInt8 = new org.elasticsearch.xpack.inference.services.voyageai.embeddings.multimodal
            .VoyageAIMultimodalEmbeddingsModel(
                multimodalModel,
                new org.elasticsearch.xpack.inference.services.voyageai.embeddings.multimodal
                    .VoyageAIMultimodalEmbeddingsServiceSettings(
                        multimodalModel.getServiceSettings().getCommonSettings(),
                        org.elasticsearch.xpack.inference.services.voyageai.embeddings.multimodal
                            .VoyageAIMultimodalEmbeddingType.INT8,
                        multimodalModel.getServiceSettings().similarity(),
                        multimodalModel.getServiceSettings().dimensions(),
                        multimodalModel.getServiceSettings().maxInputTokens(),
                        false
                    )
            );

        VoyageAIMultimodalEmbeddingsRequest request = new VoyageAIMultimodalEmbeddingsRequest(
            toInferenceStringGroups("abc"),
            InputTypeTests.randomSearchAndIngestWithNull(),
            modelWithInt8
        );

        InferenceServiceResults parsedResults = VoyageAIEmbeddingsResponseEntity.fromResponse(
            request,
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(parsedResults, instanceOf(DenseEmbeddingByteResults.class));
        assertThat(
            ((DenseEmbeddingByteResults) parsedResults).embeddings(),
            is(List.of(DenseEmbeddingByteResults.Embedding.of(List.of((byte) 100, (byte) -50))))
        );
    }

    public void testFromResponse_ThrowsException_ForUnsupportedRequestType() {
        String responseJson = """
            {
              "object": "list",
              "data": [],
              "model": "voyage-3-large",
              "usage": {
                  "total_tokens": 0
              }
            }
            """;

        // Create a mock request that's not VoyageAIEmbeddingsRequest or VoyageAIMultimodalEmbeddingsRequest
        org.elasticsearch.xpack.inference.external.request.Request unsupportedRequest = mock(
            org.elasticsearch.xpack.inference.external.request.Request.class
        );

        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> VoyageAIEmbeddingsResponseEntity.fromResponse(
                unsupportedRequest,
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(
            thrownException.getMessage(),
            containsString("Unsupported request type")
        );
    }
}
