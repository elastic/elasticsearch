/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.elastic;

import org.apache.http.HttpResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.core.ml.search.WeightedToken;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ElasticInferenceServiceSparseEmbeddingsResponseEntityTests extends ESTestCase {

    public void testSparseEmbeddingsResponse_SingleEmbeddingInData_NoMeta_NoTruncation() throws Exception {
        String responseJson = """
            {
                "data": [
                     {
                       "a": 1.23,
                       "is": 4.56,
                       "it": 7.89
                    }
                ]
            }
            """;

        SparseEmbeddingResults parsedResults = ElasticInferenceServiceSparseEmbeddingsResponseEntity.fromResponse(
            mock(Request.class),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(
            parsedResults.embeddings(),
            is(
                List.of(
                    SparseEmbeddingResults.Embedding.create(
                        List.of(new WeightedToken("a", 1.23F), new WeightedToken("is", 4.56F), new WeightedToken("it", 7.89F)),
                        false
                    )
                )
            )
        );
    }

    public void testSparseEmbeddingsResponse_MultipleEmbeddingsInData_NoMeta_NoTruncation() throws Exception {
        String responseJson = """
            {
                "data": [
                     {
                       "a": 1.23,
                       "is": 4.56,
                       "it": 7.89
                    },
                    {
                       "b": 1.23,
                       "it": 4.56,
                       "is": 7.89
                    }
                ]
            }
            """;

        SparseEmbeddingResults parsedResults = ElasticInferenceServiceSparseEmbeddingsResponseEntity.fromResponse(
            mock(Request.class),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(
            parsedResults.embeddings(),
            is(
                List.of(
                    SparseEmbeddingResults.Embedding.create(
                        List.of(new WeightedToken("a", 1.23F), new WeightedToken("is", 4.56F), new WeightedToken("it", 7.89F)),
                        false
                    ),
                    SparseEmbeddingResults.Embedding.create(
                        List.of(new WeightedToken("b", 1.23F), new WeightedToken("it", 4.56F), new WeightedToken("is", 7.89F)),
                        false
                    )
                )
            )
        );
    }

    public void testSparseEmbeddingsResponse_SingleEmbeddingInData_NoMeta_Truncated() throws Exception {
        String responseJson = """
            {
                "data": [
                     {
                       "a": 1.23,
                       "is": 4.56,
                       "it": 7.89
                    }
                ]
            }
            """;

        var request = mock(Request.class);
        when(request.getTruncationInfo()).thenReturn(new boolean[] { true });

        SparseEmbeddingResults parsedResults = ElasticInferenceServiceSparseEmbeddingsResponseEntity.fromResponse(
            request,
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(
            parsedResults.embeddings(),
            is(
                List.of(
                    SparseEmbeddingResults.Embedding.create(
                        List.of(new WeightedToken("a", 1.23F), new WeightedToken("is", 4.56F), new WeightedToken("it", 7.89F)),
                        true
                    )
                )
            )
        );
    }

    public void testSparseEmbeddingsResponse_MultipleEmbeddingsInData_NoMeta_Truncated() throws Exception {
        String responseJson = """
            {
                "data": [
                     {
                       "a": 1.23,
                       "is": 4.56,
                       "it": 7.89
                    },
                    {
                       "b": 1.23,
                       "it": 4.56,
                       "is": 7.89
                    }
                ]
            }
            """;

        var request = mock(Request.class);
        when(request.getTruncationInfo()).thenReturn(new boolean[] { true, false });

        SparseEmbeddingResults parsedResults = ElasticInferenceServiceSparseEmbeddingsResponseEntity.fromResponse(
            request,
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(
            parsedResults.embeddings(),
            is(
                List.of(
                    SparseEmbeddingResults.Embedding.create(
                        List.of(new WeightedToken("a", 1.23F), new WeightedToken("is", 4.56F), new WeightedToken("it", 7.89F)),
                        true
                    ),
                    SparseEmbeddingResults.Embedding.create(
                        List.of(new WeightedToken("b", 1.23F), new WeightedToken("it", 4.56F), new WeightedToken("is", 7.89F)),
                        false
                    )
                )
            )
        );
    }

    public void testSparseEmbeddingsResponse_SingleEmbeddingInData_IgnoresMetaBeforeData_NoTruncation() throws Exception {
        String responseJson = """
            {
                "meta": {
                    "processing_latency": 1.23
                },
                "data": [
                     {
                       "a": 1.23,
                       "is": 4.56,
                       "it": 7.89
                    }
                ]
            }
            """;

        SparseEmbeddingResults parsedResults = ElasticInferenceServiceSparseEmbeddingsResponseEntity.fromResponse(
            mock(Request.class),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(
            parsedResults.embeddings(),
            is(
                List.of(
                    SparseEmbeddingResults.Embedding.create(
                        List.of(new WeightedToken("a", 1.23F), new WeightedToken("is", 4.56F), new WeightedToken("it", 7.89F)),
                        false
                    )
                )
            )
        );
    }

    public void testSparseEmbeddingsResponse_SingleEmbeddingInData_IgnoresMetaAfterData_NoTruncation() throws Exception {
        String responseJson = """
            {
                "data": [
                     {
                       "a": 1.23,
                       "is": 4.56,
                       "it": 7.89
                    }
                ],
                "meta": {
                    "processing_latency": 1.23
                }
            }
            """;

        SparseEmbeddingResults parsedResults = ElasticInferenceServiceSparseEmbeddingsResponseEntity.fromResponse(
            mock(Request.class),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(
            parsedResults.embeddings(),
            is(
                List.of(
                    SparseEmbeddingResults.Embedding.create(
                        List.of(new WeightedToken("a", 1.23F), new WeightedToken("is", 4.56F), new WeightedToken("it", 7.89F)),
                        false
                    )
                )
            )
        );
    }
}
