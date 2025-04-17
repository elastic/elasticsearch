/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.response;

import org.apache.http.HttpResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class GoogleVertexAiEmbeddingsResponseEntityTests extends ESTestCase {

    public void testFromResponse_CreatesResultsForASingleItem() throws IOException {
        String responseJson = """
            {
                   "predictions": [
                      {
                        "embeddings": {
                          "statistics": {
                            "truncated": false,
                            "token_count": 6
                          },
                          "values": [
                            -0.123,
                            0.123
                          ]
                        }
                      }
                   ]
                 }
            """;

        TextEmbeddingFloatResults parsedResults = GoogleVertexAiEmbeddingsResponseEntity.fromResponse(
            mock(Request.class),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(parsedResults.embeddings(), is(List.of(TextEmbeddingFloatResults.Embedding.of(List.of(-0.123F, 0.123F)))));
    }

    public void testFromResponse_CreatesResultsForMultipleItems() throws IOException {
        String responseJson = """
            {
                   "predictions": [
                      {
                        "embeddings": {
                          "statistics": {
                            "truncated": false,
                            "token_count": 6
                          },
                          "values": [
                            -0.123,
                            0.123
                          ]
                        }
                      },
                      {
                        "embeddings": {
                          "statistics": {
                            "truncated": false,
                            "token_count": 6
                          },
                          "values": [
                            -0.456,
                            0.456
                          ]
                        }
                      }
                   ]
                 }
            """;

        TextEmbeddingFloatResults parsedResults = GoogleVertexAiEmbeddingsResponseEntity.fromResponse(
            mock(Request.class),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(
            parsedResults.embeddings(),
            is(
                List.of(
                    TextEmbeddingFloatResults.Embedding.of(List.of(-0.123F, 0.123F)),
                    TextEmbeddingFloatResults.Embedding.of(List.of(-0.456F, 0.456F))
                )
            )
        );
    }

    public void testFromResponse_FailsWhenPredictionsFieldIsNotPresent() {
        String responseJson = """
            {
                   "not_predictions": [
                      {
                        "embeddings": {
                          "statistics": {
                            "truncated": false,
                            "token_count": 6
                          },
                          "values": [
                            -0.123,
                            0.123
                          ]
                        }
                      },
                      {
                        "embeddings": {
                          "statistics": {
                            "truncated": false,
                            "token_count": 6
                          },
                          "values": [
                            -0.456,
                            0.456
                          ]
                        }
                      }
                   ]
                 }
            """;

        var thrownException = expectThrows(
            IllegalStateException.class,
            () -> GoogleVertexAiEmbeddingsResponseEntity.fromResponse(
                mock(Request.class),
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(thrownException.getMessage(), is("Failed to find required field [predictions] in Google Vertex AI embeddings response"));
    }

    public void testFromResponse_FailsWhenEmbeddingsFieldIsNotPresent() {
        String responseJson = """
            {
                   "predictions": [
                      {
                        "not_embeddings": {
                          "statistics": {
                            "truncated": false,
                            "token_count": 6
                          },
                          "values": [
                            -0.123,
                            0.123
                          ]
                        }
                      }
                   ]
                 }
            """;

        var thrownException = expectThrows(
            IllegalStateException.class,
            () -> GoogleVertexAiEmbeddingsResponseEntity.fromResponse(
                mock(Request.class),
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(thrownException.getMessage(), is("Failed to find required field [embeddings] in Google Vertex AI embeddings response"));
    }

    public void testFromResponse_FailsWhenValuesFieldIsNotPresent() {
        String responseJson = """
            {
                   "predictions": [
                      {
                        "embeddings": {
                          "statistics": {
                            "truncated": false,
                            "token_count": 6
                          },
                          "not_values": [
                            -0.123,
                            0.123
                          ]
                        }
                      }
                   ]
                 }
            """;

        var thrownException = expectThrows(
            IllegalStateException.class,
            () -> GoogleVertexAiEmbeddingsResponseEntity.fromResponse(
                mock(Request.class),
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(thrownException.getMessage(), is("Failed to find required field [values] in Google Vertex AI embeddings response"));
    }
}
