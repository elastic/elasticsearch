/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.response;

import org.apache.http.HttpResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.external.response.elastic.ElasticInferenceServiceDenseTextEmbeddingsResponseEntity;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;

public class ElasticInferenceServiceDenseTextEmbeddingsResponseEntityTests extends ESTestCase {

    public void testDenseTextEmbeddingsResponse_SingleEmbeddingInData_NoMeta() throws Exception {
        String responseJson = """
            {
                "data": [
                    [
                        1.23,
                        4.56,
                        7.89
                    ]
                ]
            }
            """;

        TextEmbeddingFloatResults parsedResults = ElasticInferenceServiceDenseTextEmbeddingsResponseEntity.fromResponse(
            mock(Request.class),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(parsedResults.embeddings(), hasSize(1));

        var embedding = parsedResults.embeddings().get(0);
        assertThat(embedding.values(), is(new float[] { 1.23f, 4.56f, 7.89f }));
    }

    public void testDenseTextEmbeddingsResponse_MultipleEmbeddingsInData_NoMeta() throws Exception {
        String responseJson = """
            {
                "data": [
                    [
                        1.23,
                        4.56,
                        7.89
                    ],
                    [
                        0.12,
                        0.34,
                        0.56
                    ]
                ]
            }
            """;

        TextEmbeddingFloatResults parsedResults = ElasticInferenceServiceDenseTextEmbeddingsResponseEntity.fromResponse(
            mock(Request.class),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(parsedResults.embeddings(), hasSize(2));

        var firstEmbedding = parsedResults.embeddings().get(0);
        assertThat(firstEmbedding.values(), is(new float[] { 1.23f, 4.56f, 7.89f }));

        var secondEmbedding = parsedResults.embeddings().get(1);
        assertThat(secondEmbedding.values(), is(new float[] { 0.12f, 0.34f, 0.56f }));
    }

    public void testDenseTextEmbeddingsResponse_EmptyData() throws Exception {
        String responseJson = """
            {
                "data": []
            }
            """;

        TextEmbeddingFloatResults parsedResults = ElasticInferenceServiceDenseTextEmbeddingsResponseEntity.fromResponse(
            mock(Request.class),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(parsedResults.embeddings(), hasSize(0));
    }

    public void testDenseTextEmbeddingsResponse_SingleEmbeddingInData_IgnoresMeta() throws Exception {
        String responseJson = """
            {
                "data": [
                    [
                        -1.0,
                        0.0,
                        1.0
                    ]
                ],
                "meta": {
                    "usage": {
                        "total_tokens": 5
                    }
                }
            }
            """;

        TextEmbeddingFloatResults parsedResults = ElasticInferenceServiceDenseTextEmbeddingsResponseEntity.fromResponse(
            mock(Request.class),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(parsedResults.embeddings(), hasSize(1));

        var embedding = parsedResults.embeddings().get(0);
        assertThat(embedding.values(), is(new float[] { -1.0f, 0.0f, 1.0f }));
    }
}
