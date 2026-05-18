/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.response;

import org.apache.http.HttpResponse;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.EmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.GenericDenseEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OpenAiEmbeddingsResponseEntityTests extends ESTestCase {
    public void testFromResponse_CreatesResultsForASingleItem_TextEmbedding() throws IOException {
        testFromResponse_CreatesResultsForASingleItem(TaskType.TEXT_EMBEDDING);
    }

    public void testFromResponse_CreatesResultsForASingleItem_Embedding() throws IOException {
        testFromResponse_CreatesResultsForASingleItem(TaskType.EMBEDDING);
    }

    private static void testFromResponse_CreatesResultsForASingleItem(TaskType taskType) throws IOException {
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
              "model": "text-embedding-ada-002-v2",
              "usage": {
                  "prompt_tokens": 8,
                  "total_tokens": 8
              }
            }
            """;

        EmbeddingFloatResults parsedResults = getEmbeddingFloatResultsAndAssertType(taskType, responseJson);

        assertThat(
            parsedResults.embeddings(),
            is(List.of(new EmbeddingFloatResults.Embedding(new float[] { 0.014539449F, -0.015288644F })))
        );
    }

    public void testFromResponse_CreatesResultsForMultipleItems_TextEmbedding() throws IOException {
        testFromResponse_CreatesResultsForMultipleItems(TaskType.TEXT_EMBEDDING);
    }

    public void testFromResponse_CreatesResultsForMultipleItems_Embedding() throws IOException {
        testFromResponse_CreatesResultsForMultipleItems(TaskType.EMBEDDING);
    }

    private static void testFromResponse_CreatesResultsForMultipleItems(TaskType taskType) throws IOException {
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
              "model": "text-embedding-ada-002-v2",
              "usage": {
                  "prompt_tokens": 8,
                  "total_tokens": 8
              }
            }
            """;

        EmbeddingFloatResults parsedResults = getEmbeddingFloatResultsAndAssertType(taskType, responseJson);

        assertThat(
            parsedResults.embeddings(),
            is(
                List.of(
                    new EmbeddingFloatResults.Embedding(new float[] { 0.014539449F, -0.015288644F }),
                    new EmbeddingFloatResults.Embedding(new float[] { 0.0123F, -0.0123F })
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
              "model": "text-embedding-ada-002-v2",
              "usage": {
                  "prompt_tokens": 8,
                  "total_tokens": 8
              }
            }
            """;

        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> OpenAiEmbeddingsResponseEntity.fromResponse(
                mock(OutboundRequest.class),
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
              "model": "text-embedding-ada-002-v2",
              "usage": {
                  "prompt_tokens": 8,
                  "total_tokens": 8
              }
            }
            """;

        var thrownException = expectThrows(
            XContentParseException.class,
            () -> OpenAiEmbeddingsResponseEntity.fromResponse(
                mock(OutboundRequest.class),
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
              "model": "text-embedding-ada-002-v2",
              "usage": {
                  "prompt_tokens": 8,
                  "total_tokens": 8
              }
            }
            """;

        var thrownException = expectThrows(
            XContentParseException.class,
            () -> OpenAiEmbeddingsResponseEntity.fromResponse(
                mock(OutboundRequest.class),
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
              "model": "text-embedding-ada-002-v2",
              "usage": {
                  "prompt_tokens": 8,
                  "total_tokens": 8
              }
            }
            """;

        var thrownException = expectThrows(
            XContentParseException.class,
            () -> OpenAiEmbeddingsResponseEntity.fromResponse(
                mock(OutboundRequest.class),
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(thrownException.getMessage(), containsString("[EmbeddingFloatResult] failed to parse field [data]"));
    }

    public void testFromResponse_SucceedsWhenEmbeddingValueIsInt_TextEmbedding() throws IOException {
        testFromResponse_SucceedsWhenEmbeddingValueIsInt(TaskType.TEXT_EMBEDDING);
    }

    public void testFromResponse_SucceedsWhenEmbeddingValueIsInt_Embedding() throws IOException {
        testFromResponse_SucceedsWhenEmbeddingValueIsInt(TaskType.EMBEDDING);
    }

    private static void testFromResponse_SucceedsWhenEmbeddingValueIsInt(TaskType taskType) throws IOException {
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
              "model": "text-embedding-ada-002-v2",
              "usage": {
                  "prompt_tokens": 8,
                  "total_tokens": 8
              }
            }
            """;

        EmbeddingFloatResults parsedResults = getEmbeddingFloatResultsAndAssertType(taskType, responseJson);

        assertThat(parsedResults.embeddings(), is(List.of(new EmbeddingFloatResults.Embedding(new float[] { 1.0F }))));
    }

    public void testFromResponse_SucceedsWhenEmbeddingValueIsLong_TextEmbedding() throws IOException {
        testFromResponse_SucceedsWhenEmbeddingValueIsLong(TaskType.TEXT_EMBEDDING);
    }

    public void testFromResponse_SucceedsWhenEmbeddingValueIsLong_Embedding() throws IOException {
        testFromResponse_SucceedsWhenEmbeddingValueIsLong(TaskType.EMBEDDING);
    }

    private static void testFromResponse_SucceedsWhenEmbeddingValueIsLong(TaskType taskType) throws IOException {
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
              "model": "text-embedding-ada-002-v2",
              "usage": {
                  "prompt_tokens": 8,
                  "total_tokens": 8
              }
            }
            """;

        EmbeddingFloatResults parsedResults = getEmbeddingFloatResultsAndAssertType(taskType, responseJson);

        assertThat(parsedResults.embeddings(), is(List.of(new EmbeddingFloatResults.Embedding(new float[] { 4.0294965E10F }))));
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
              "model": "text-embedding-ada-002-v2",
              "usage": {
                  "prompt_tokens": 8,
                  "total_tokens": 8
              }
            }
            """;

        var thrownException = expectThrows(
            XContentParseException.class,
            () -> OpenAiEmbeddingsResponseEntity.fromResponse(
                mock(OutboundRequest.class),
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(thrownException.getMessage(), containsString("[EmbeddingFloatResult] failed to parse field [data]"));
    }

    public void testFieldsInDifferentOrderServer_TextEmbedding() throws IOException {
        testFieldsInDifferentOrderServer(TaskType.TEXT_EMBEDDING);
    }

    public void testFieldsInDifferentOrderServer_Embedding() throws IOException {
        testFieldsInDifferentOrderServer(TaskType.EMBEDDING);
    }

    private static void testFieldsInDifferentOrderServer(TaskType taskType) throws IOException {
        // The fields of the objects in the data array are reordered
        String response = """
            {
                "created": 1711530064,
                "object": "list",
                "id": "6667830b-716b-4796-9a61-33b67b5cc81d",
                "model": "mxbai-embed-large-v1",
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
                    "prompt_tokens": 0,
                    "completion_tokens": 0,
                    "total_tokens": 0
                }
            }""";

        EmbeddingFloatResults parsedResults = getEmbeddingFloatResultsAndAssertType(taskType, response);

        assertThat(
            parsedResults.embeddings(),
            is(
                List.of(
                    new EmbeddingFloatResults.Embedding(new float[] { -0.9F, 0.5F, 0.3F }),
                    new EmbeddingFloatResults.Embedding(new float[] { 0.1F, 0.5F }),
                    new EmbeddingFloatResults.Embedding(new float[] { 0.5F, 0.5F })
                )
            )
        );
    }

    private static EmbeddingFloatResults getEmbeddingFloatResultsAndAssertType(TaskType taskType, String response) throws IOException {
        OutboundRequest outboundRequestMock = mock(OutboundRequest.class);
        when(outboundRequestMock.getTaskType()).thenReturn(taskType);
        EmbeddingFloatResults parsedResults = OpenAiEmbeddingsResponseEntity.fromResponse(
            outboundRequestMock,
            new HttpResult(mock(HttpResponse.class), response.getBytes(StandardCharsets.UTF_8))
        );

        if (taskType.equals(TaskType.TEXT_EMBEDDING)) {
            assertThat(parsedResults, instanceOf(DenseEmbeddingFloatResults.class));
        } else {
            assertThat(parsedResults, instanceOf(GenericDenseEmbeddingFloatResults.class));
        }
        return parsedResults;
    }
}
