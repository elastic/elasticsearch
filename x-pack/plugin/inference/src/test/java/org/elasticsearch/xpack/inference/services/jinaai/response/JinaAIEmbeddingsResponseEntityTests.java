/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.response;

import org.apache.http.HttpResponse;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingBitResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingByteResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.InputTypeTests;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingType;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsTaskSettings;
import org.elasticsearch.xpack.inference.services.jinaai.request.JinaAIEmbeddingsRequestTests;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class JinaAIEmbeddingsResponseEntityTests extends ESTestCase {
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
              "model": "jina-embeddings-v3",
              "usage": {
                  "prompt_tokens": 8,
                  "total_tokens": 8
              }
            }
            """;

        InferenceServiceResults parsedResults = JinaAIEmbeddingsResponseEntity.fromResponse(
            JinaAIEmbeddingsRequestTests.createRequest(
                List.of("abc"),
                InputTypeTests.randomWithNull(),
                JinaAIEmbeddingsModelTests.createModel(
                    "url",
                    "secret",
                    JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                    null,
                    null,
                    "model",
                    JinaAIEmbeddingType.FLOAT
                )
            ),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(parsedResults, instanceOf(TextEmbeddingFloatResults.class));
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
              "model": "jina-embeddings-v3",
              "usage": {
                  "prompt_tokens": 8,
                  "total_tokens": 8
              }
            }
            """;

        InferenceServiceResults parsedResults = JinaAIEmbeddingsResponseEntity.fromResponse(
            JinaAIEmbeddingsRequestTests.createRequest(
                List.of("abc"),
                InputTypeTests.randomWithNull(),
                JinaAIEmbeddingsModelTests.createModel(
                    "url",
                    "secret",
                    JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                    null,
                    null,
                    "model",
                    JinaAIEmbeddingType.FLOAT
                )
            ),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(parsedResults, instanceOf(TextEmbeddingFloatResults.class));
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
              "model": "jina-embeddings-v3",
              "usage": {
                  "prompt_tokens": 8,
                  "total_tokens": 8
              }
            }
            """;

        var thrownException = expectThrows(
            IllegalStateException.class,
            () -> JinaAIEmbeddingsResponseEntity.fromResponse(
                JinaAIEmbeddingsRequestTests.createRequest(
                    List.of("abc"),
                    InputTypeTests.randomWithNull(),
                    JinaAIEmbeddingsModelTests.createModel(
                        "url",
                        "secret",
                        JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                        null,
                        null,
                        "model",
                        JinaAIEmbeddingType.FLOAT
                    )
                ),
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(thrownException.getMessage(), is("Failed to find required field [data] in JinaAI embeddings response"));
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
              "model": "jina-embeddings-v3",
              "usage": {
                  "prompt_tokens": 8,
                  "total_tokens": 8
              }
            }
            """;

        var thrownException = expectThrows(
            ParsingException.class,
            () -> JinaAIEmbeddingsResponseEntity.fromResponse(
                JinaAIEmbeddingsRequestTests.createRequest(
                    List.of("abc"),
                    InputTypeTests.randomWithNull(),
                    JinaAIEmbeddingsModelTests.createModel(
                        "url",
                        "secret",
                        JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                        null,
                        null,
                        "model",
                        JinaAIEmbeddingType.FLOAT
                    )
                ),
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(
            thrownException.getMessage(),
            is("Failed to parse object: expecting token of type [START_ARRAY] but found [START_OBJECT]")
        );
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
              "model": "jina-embeddings-v3",
              "usage": {
                  "prompt_tokens": 8,
                  "total_tokens": 8
              }
            }
            """;

        var thrownException = expectThrows(
            IllegalStateException.class,
            () -> JinaAIEmbeddingsResponseEntity.fromResponse(
                JinaAIEmbeddingsRequestTests.createRequest(
                    List.of("abc"),
                    InputTypeTests.randomWithNull(),
                    JinaAIEmbeddingsModelTests.createModel(
                        "url",
                        "secret",
                        JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                        null,
                        null,
                        "model",
                        JinaAIEmbeddingType.FLOAT
                    )
                ),
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(thrownException.getMessage(), is("Failed to find required field [embedding] in JinaAI embeddings response"));
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
              "model": "jina-embeddings-v3",
              "usage": {
                  "prompt_tokens": 8,
                  "total_tokens": 8
              }
            }
            """;

        var thrownException = expectThrows(
            ParsingException.class,
            () -> JinaAIEmbeddingsResponseEntity.fromResponse(
                JinaAIEmbeddingsRequestTests.createRequest(
                    List.of("abc"),
                    InputTypeTests.randomWithNull(),
                    JinaAIEmbeddingsModelTests.createModel(
                        "url",
                        "secret",
                        JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                        null,
                        null,
                        "model",
                        JinaAIEmbeddingType.FLOAT
                    )
                ),
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(
            thrownException.getMessage(),
            is("Failed to parse object: expecting token of type [VALUE_NUMBER] but found [VALUE_STRING]")
        );
    }

    public void testFromResponse_SucceedsWhenEmbeddingType_IsBinary() throws IOException {
        String responseJson = """
            {
              "object": "list",
              "data": [
                  {
                      "object": "embedding",
                      "index": 0,
                      "embedding": [
                           -55,
                            74,
                            101,
                            67,
                            83
                      ]
                  }
              ],
              "model": "jina-embeddings-v3",
              "usage": {
                  "prompt_tokens": 8,
                  "total_tokens": 8
              }
            }
            """;

        InferenceServiceResults parsedResults = JinaAIEmbeddingsResponseEntity.fromResponse(
            JinaAIEmbeddingsRequestTests.createRequest(
                List.of("abc"),
                InputTypeTests.randomWithNull(),
                JinaAIEmbeddingsModelTests.createModel(
                    "url",
                    "secret",
                    JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                    null,
                    null,
                    "model",
                    JinaAIEmbeddingType.BINARY
                )
            ),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(
            ((TextEmbeddingBitResults) parsedResults).embeddings(),
            is(List.of(new TextEmbeddingByteResults.Embedding(new byte[] { (byte) -55, (byte) 74, (byte) 101, (byte) 67, (byte) 83 })))
        );
    }

    public void testFromResponse_SucceedsWhenEmbeddingType_IsBit() throws IOException {
        String responseJson = """
            {
              "object": "list",
              "data": [
                  {
                      "object": "embedding",
                      "index": 0,
                      "embedding": [
                           -55,
                            74,
                            101,
                            67,
                            83
                      ]
                  }
              ],
              "model": "jina-embeddings-v3",
              "usage": {
                  "prompt_tokens": 8,
                  "total_tokens": 8
              }
            }
            """;

        InferenceServiceResults parsedResults = JinaAIEmbeddingsResponseEntity.fromResponse(
            JinaAIEmbeddingsRequestTests.createRequest(
                List.of("abc"),
                InputTypeTests.randomWithNull(),
                JinaAIEmbeddingsModelTests.createModel(
                    "url",
                    "secret",
                    JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                    null,
                    null,
                    "model",
                    JinaAIEmbeddingType.BIT
                )
            ),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(
            ((TextEmbeddingBitResults) parsedResults).embeddings(),
            is(List.of(new TextEmbeddingByteResults.Embedding(new byte[] { (byte) -55, (byte) 74, (byte) 101, (byte) 67, (byte) 83 })))
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
              "model": "jina-embeddings-v3",
              "usage": {
                  "prompt_tokens": 8,
                  "total_tokens": 8
              }
            }
            """;

        var thrownException = expectThrows(
            ParsingException.class,
            () -> JinaAIEmbeddingsResponseEntity.fromResponse(
                JinaAIEmbeddingsRequestTests.createRequest(
                    List.of("abc"),
                    InputTypeTests.randomWithNull(),
                    JinaAIEmbeddingsModelTests.createModel(
                        "url",
                        "secret",
                        JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                        null,
                        null,
                        "model",
                        JinaAIEmbeddingType.BINARY
                    )
                ),
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(
            thrownException.getMessage(),
            is("Failed to parse object: expecting token of type [VALUE_NUMBER] but found [START_OBJECT]")
        );
    }

    public void testFieldsInDifferentOrderServer() throws IOException {
        // The fields of the objects in the data array are reordered
        String response = """
            {
                "object": "list",
                "id": "6667830b-716b-4796-9a61-33b67b5cc81d",
                "model": "jina-embeddings-v3",
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

        TextEmbeddingFloatResults parsedResults = (TextEmbeddingFloatResults) JinaAIEmbeddingsResponseEntity.fromResponse(
            JinaAIEmbeddingsRequestTests.createRequest(
                List.of("abc"),
                InputTypeTests.randomWithNull(),
                JinaAIEmbeddingsModelTests.createModel(
                    "url",
                    "secret",
                    JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                    null,
                    null,
                    "model",
                    JinaAIEmbeddingType.FLOAT
                )
            ),
            new HttpResult(mock(HttpResponse.class), response.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(
            parsedResults.embeddings(),
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
