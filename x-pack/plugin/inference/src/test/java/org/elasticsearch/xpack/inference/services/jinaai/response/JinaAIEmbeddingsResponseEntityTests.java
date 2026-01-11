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
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingBitResults;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.EmbeddingBitResults;
import org.elasticsearch.xpack.core.inference.results.EmbeddingByteResults;
import org.elasticsearch.xpack.core.inference.results.EmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.GenericDenseEmbeddingBitResults;
import org.elasticsearch.xpack.core.inference.results.GenericDenseEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.InputTypeTests;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingType;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsTaskSettings;
import org.elasticsearch.xpack.inference.services.jinaai.request.JinaAIEmbeddingsRequestTests;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.elasticsearch.xpack.inference.TaskTypeTests.randomEmbeddingTaskType;
import static org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingType.BINARY;
import static org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingType.BIT;
import static org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingType.FLOAT;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;
import static org.mockito.Mockito.mock;

public class JinaAIEmbeddingsResponseEntityTests extends ESTestCase {
    public void testFromResponse_CreatesResultsForASingleItem_textEmbeddingTask() throws IOException {
        testFromResponse_singleItem(TaskType.TEXT_EMBEDDING);
    }

    public void testFromResponse_CreatesResultsForASingleItem_embeddingTask() throws IOException {
        testFromResponse_singleItem(TaskType.EMBEDDING);
    }

    private static void testFromResponse_singleItem(TaskType taskType) throws IOException {
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
            JinaAIEmbeddingsRequestTests.createTextOnlyRequest(
                List.of("abc"),
                InputTypeTests.randomWithNull(),
                JinaAIEmbeddingsModelTests.createModel(null, "modelName", JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS, "secret", taskType)
            ),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertResultsType(taskType, JinaAIEmbeddingType.FLOAT, parsedResults);
        assertThat(
            ((EmbeddingFloatResults) parsedResults).embeddings(),
            Matchers.is(List.of(new EmbeddingFloatResults.Embedding(new float[] { 0.014539449F, -0.015288644F })))
        );
    }

    public void testFromResponse_CreatesResultsForMultipleItems_textEmbeddingTask() throws IOException {
        testFromResponse_multipleItems(TaskType.TEXT_EMBEDDING);
    }

    public void testFromResponse_CreatesResultsForMultipleItems_embeddingTask() throws IOException {
        testFromResponse_multipleItems(TaskType.EMBEDDING);
    }

    private static void testFromResponse_multipleItems(TaskType taskType) throws IOException {
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
            JinaAIEmbeddingsRequestTests.createTextOnlyRequest(
                List.of("abc"),
                InputTypeTests.randomWithNull(),
                JinaAIEmbeddingsModelTests.createModel(null, "modelName", JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS, "secret", taskType)
            ),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertResultsType(taskType, JinaAIEmbeddingType.FLOAT, parsedResults);
        assertThat(
            ((EmbeddingFloatResults) parsedResults).embeddings(),
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
              "model": "jina-embeddings-v3",
              "usage": {
                  "prompt_tokens": 8,
                  "total_tokens": 8
              }
            }
            """;

        var thrownException = expectThrows(
            IllegalStateException.class,
            () -> callFromResponse(responseJson, randomFrom(JinaAIEmbeddingType.values()))
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
            () -> callFromResponse(responseJson, randomFrom(JinaAIEmbeddingType.values()))
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
            () -> callFromResponse(responseJson, randomFrom(JinaAIEmbeddingType.values()))
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
            () -> callFromResponse(responseJson, randomFrom(JinaAIEmbeddingType.values()))
        );

        assertThat(
            thrownException.getMessage(),
            is("Failed to parse object: expecting token of type [VALUE_NUMBER] but found [VALUE_STRING]")
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
            () -> callFromResponse(responseJson, randomFrom(JinaAIEmbeddingType.values()))
        );

        assertThat(
            thrownException.getMessage(),
            is("Failed to parse object: expecting token of type [VALUE_NUMBER] but found [START_OBJECT]")
        );
    }

    public void testFromResponse_withBitEmbeddingType_FailsWhenEmbeddingValueIsLargerThanByte() {
        String responseJson = """
            {
              "object": "list",
              "data": [
                  {
                      "object": "embedding",
                      "index": 0,
                      "embedding": [
                           -1024
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

        var thrownException = expectThrows(IllegalArgumentException.class, () -> callFromResponse(responseJson, randomFrom(BIT, BINARY)));

        assertThat(thrownException.getMessage(), is("Value [-1024] is out of range for a byte"));
    }

    private static void callFromResponse(String responseJson, JinaAIEmbeddingType embeddingType) throws IOException {
        JinaAIEmbeddingsResponseEntity.fromResponse(
            JinaAIEmbeddingsRequestTests.createTextOnlyRequest(
                List.of("abc"),
                InputTypeTests.randomWithNull(),
                JinaAIEmbeddingsModelTests.createModel(
                    null,
                    "modelName",
                    embeddingType,
                    JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                    "secret",
                    null,
                    randomEmbeddingTaskType(),
                    false
                )
            ),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );
    }

    public void testFromResponse_SucceedsWhenEmbeddingType_IsBinary() throws IOException {
        fromResponse_withNonFloatEmbeddingType(BINARY);
    }

    public void testFromResponse_SucceedsWhenEmbeddingType_IsBit() throws IOException {
        fromResponse_withNonFloatEmbeddingType(BIT);
    }

    private static void fromResponse_withNonFloatEmbeddingType(JinaAIEmbeddingType embeddingType) throws IOException {
        assertThat(embeddingType, oneOf(BIT, BINARY));
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

        var taskType = randomEmbeddingTaskType();
        InferenceServiceResults parsedResults = JinaAIEmbeddingsResponseEntity.fromResponse(
            JinaAIEmbeddingsRequestTests.createTextOnlyRequest(
                List.of("abc"),
                InputTypeTests.randomWithNull(),
                JinaAIEmbeddingsModelTests.createModel(
                    null,
                    "modelName",
                    embeddingType,
                    JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                    "secret",
                    null,
                    taskType,
                    false
                )
            ),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertResultsType(taskType, embeddingType, parsedResults);
        assertThat(
            ((EmbeddingBitResults) parsedResults).embeddings(),
            is(List.of(new EmbeddingByteResults.Embedding(new byte[] { (byte) -55, (byte) 74, (byte) 101, (byte) 67, (byte) 83 })))
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

        TaskType taskType = randomEmbeddingTaskType();
        InferenceServiceResults parsedResults = JinaAIEmbeddingsResponseEntity.fromResponse(
            JinaAIEmbeddingsRequestTests.createTextOnlyRequest(
                List.of("abc"),
                InputTypeTests.randomWithNull(),
                JinaAIEmbeddingsModelTests.createModel(null, "modelName", JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS, "secret", taskType)
            ),
            new HttpResult(mock(HttpResponse.class), response.getBytes(StandardCharsets.UTF_8))
        );

        assertResultsType(taskType, FLOAT, parsedResults);
        assertThat(
            ((EmbeddingFloatResults) parsedResults).embeddings(),
            is(
                List.of(
                    new EmbeddingFloatResults.Embedding(new float[] { -0.9F, 0.5F, 0.3F }),
                    new EmbeddingFloatResults.Embedding(new float[] { 0.1F, 0.5F }),
                    new EmbeddingFloatResults.Embedding(new float[] { 0.5F, 0.5F })
                )
            )
        );
    }

    private static void assertResultsType(TaskType taskType, JinaAIEmbeddingType embeddingType, InferenceServiceResults parsedResults) {
        if (taskType.equals(TaskType.TEXT_EMBEDDING)) {
            switch (embeddingType) {
                case FLOAT -> assertThat(parsedResults, instanceOf(DenseEmbeddingFloatResults.class));
                case BIT, BINARY -> assertThat(parsedResults, instanceOf(DenseEmbeddingBitResults.class));
            }
        } else if (taskType.equals(TaskType.EMBEDDING)) {
            switch (embeddingType) {
                case FLOAT -> assertThat(parsedResults, instanceOf(GenericDenseEmbeddingFloatResults.class));
                case BIT, BINARY -> assertThat(parsedResults, instanceOf(GenericDenseEmbeddingBitResults.class));
            }
        } else {
            throw new IllegalArgumentException("Invalid taskType: " + taskType);
        }
    }
}
