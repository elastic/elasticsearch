/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.response;

import org.apache.http.HttpResponse;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.InputTypeTests;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.services.voyageai.request.VoyageAIEmbeddingsRequest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingsModelTests.createModel;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class VoyageAIEmbeddingsResponseEntityTests extends ESTestCase {
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
            ((TextEmbeddingFloatResults) parsedResults).embeddings(),
            is(List.of(new TextEmbeddingFloatResults.Embedding(new float[] { 0.014539449F, -0.015288644F })))
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
            ((TextEmbeddingFloatResults) parsedResults).embeddings(),
            is(
                List.of(
                    new TextEmbeddingFloatResults.Embedding(new float[] { 0.014539449F, -0.015288644F }),
                    new TextEmbeddingFloatResults.Embedding(new float[] { 0.0123F, -0.0123F })
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
            ((TextEmbeddingFloatResults) parsedResults).embeddings(),
            is(List.of(new TextEmbeddingFloatResults.Embedding(new float[] { 1.0F })))
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
            ((TextEmbeddingFloatResults) parsedResults).embeddings(),
            is(List.of(new TextEmbeddingFloatResults.Embedding(new float[] { 4.0294965E10F })))
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

        assertThat(parsedResults, instanceOf(TextEmbeddingFloatResults.class));

        assertThat(
            ((TextEmbeddingFloatResults) parsedResults).embeddings(),
            is(
                List.of(
                    new TextEmbeddingFloatResults.Embedding(new float[] { -0.9F, 0.5F, 0.3F }),
                    new TextEmbeddingFloatResults.Embedding(new float[] { 0.1F, 0.5F }),
                    new TextEmbeddingFloatResults.Embedding(new float[] { 0.5F, 0.5F })
                )
            )
        );
    }
}
