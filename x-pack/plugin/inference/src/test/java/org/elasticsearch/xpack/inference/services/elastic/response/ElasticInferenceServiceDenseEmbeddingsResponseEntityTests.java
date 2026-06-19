/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.response;

import org.apache.http.HttpResponse;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.EmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.GenericDenseEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.services.elastic.request.ElasticInferenceServiceDenseEmbeddingsRequest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.elasticsearch.test.ESTestCase.assertThat;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ElasticInferenceServiceDenseEmbeddingsResponseEntityTests extends ESTestCase {

    public void testDenseEmbeddingsResponse_SingleEmbeddingInData_NoMeta_TextEmbedding() throws Exception {
        testDenseEmbeddingsResponse_SingleEmbeddingInData_NoMeta(TaskType.TEXT_EMBEDDING);
    }

    public void testDenseEmbeddingsResponse_SingleEmbeddingInData_NoMeta_Embedding() throws Exception {
        testDenseEmbeddingsResponse_SingleEmbeddingInData_NoMeta(TaskType.EMBEDDING);
    }

    private static void testDenseEmbeddingsResponse_SingleEmbeddingInData_NoMeta(TaskType taskType) throws IOException {
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

        var parsedResults = getResults(responseJson, taskType);

        assertResultsClass(taskType, parsedResults);
        assertThat(parsedResults.embeddings(), hasSize(1));

        var embedding = parsedResults.embeddings().get(0);
        assertThat(embedding.values(), is(new float[] { 1.23f, 4.56f, 7.89f }));
    }

    public void testDenseEmbeddingsResponse_MultipleEmbeddingsInData_NoMeta_TextEmbedding() throws Exception {
        testDenseEmbeddingsResponse_MultipleEmbeddingsInData_NoMeta(TaskType.TEXT_EMBEDDING);
    }

    public void testDenseEmbeddingsResponse_MultipleEmbeddingsInData_NoMeta_Embedding() throws Exception {
        testDenseEmbeddingsResponse_MultipleEmbeddingsInData_NoMeta(TaskType.EMBEDDING);
    }

    private static void testDenseEmbeddingsResponse_MultipleEmbeddingsInData_NoMeta(TaskType taskType) throws IOException {
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

        var parsedResults = getResults(responseJson, taskType);

        assertResultsClass(taskType, parsedResults);
        assertThat(parsedResults.embeddings(), hasSize(2));

        var firstEmbedding = parsedResults.embeddings().get(0);
        assertThat(firstEmbedding.values(), is(new float[] { 1.23f, 4.56f, 7.89f }));

        var secondEmbedding = parsedResults.embeddings().get(1);
        assertThat(secondEmbedding.values(), is(new float[] { 0.12f, 0.34f, 0.56f }));
    }

    public void testDenseEmbeddingsResponse_EmptyData_TextEmbedding() throws Exception {
        testDenseEmbeddingsResponse_EmptyData(TaskType.TEXT_EMBEDDING);
    }

    public void testDenseEmbeddingsResponse_EmptyData_Embedding() throws Exception {
        testDenseEmbeddingsResponse_EmptyData(TaskType.EMBEDDING);
    }

    private static void testDenseEmbeddingsResponse_EmptyData(TaskType taskType) throws IOException {
        String responseJson = """
            {
                "data": []
            }
            """;

        var parsedResults = getResults(responseJson, taskType);

        assertResultsClass(taskType, parsedResults);
        assertThat(parsedResults.embeddings(), hasSize(0));
    }

    public void testDenseEmbeddingsResponse_SingleEmbeddingInData_IgnoresMeta_TextEmbedding() throws Exception {
        testDenseEmbeddingsResponse_SingleEmbeddingInData_IgnoresMeta(TaskType.TEXT_EMBEDDING);
    }

    public void testDenseEmbeddingsResponse_SingleEmbeddingInData_IgnoresMeta_Embedding() throws Exception {
        testDenseEmbeddingsResponse_SingleEmbeddingInData_IgnoresMeta(TaskType.EMBEDDING);
    }

    private static void testDenseEmbeddingsResponse_SingleEmbeddingInData_IgnoresMeta(TaskType taskType) throws IOException {
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

        var parsedResults = getResults(responseJson, taskType);

        assertResultsClass(taskType, parsedResults);
        assertThat(parsedResults.embeddings(), hasSize(1));

        var embedding = parsedResults.embeddings().get(0);
        assertThat(embedding.values(), is(new float[] { -1.0f, 0.0f, 1.0f }));
    }

    private static EmbeddingFloatResults getResults(String responseJson, TaskType taskType) throws IOException {
        var request = mock(ElasticInferenceServiceDenseEmbeddingsRequest.class);
        when(request.getTaskType()).thenReturn(taskType);
        return ElasticInferenceServiceDenseEmbeddingsResponseEntity.fromResponse(
            request,
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );
    }

    private static void assertResultsClass(TaskType taskType, EmbeddingFloatResults parsedResults) {
        if (taskType == TaskType.TEXT_EMBEDDING) {
            assertThat(parsedResults, instanceOf(DenseEmbeddingFloatResults.class));
        } else {
            assertThat(parsedResults, instanceOf(GenericDenseEmbeddingFloatResults.class));
        }
    }
}
