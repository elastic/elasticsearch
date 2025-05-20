/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom.response;

import org.apache.http.HttpResponse;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;
import org.elasticsearch.xpack.core.ml.search.WeightedToken;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.services.custom.CustomModelTests;
import org.elasticsearch.xpack.inference.services.custom.request.CustomRequest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class CustomResponseEntityTests extends ESTestCase {

    public void testFromTextEmbeddingResponse() throws IOException {
        String responseJson = """
            {
                "request_id": "B4AB89C8-B135-xxxx-A6F8-2BAB801A2CE4",
                "latency": 38,
                "usage": {
                    "token_count": 3072
                },
                "result": {
                    "embeddings": [
                        {
                            "index": 0,
                            "embedding": [
                                -0.02868066355586052,
                                0.022033605724573135
                            ]
                        }
                    ]
                }
            }
            """;

        var request = new CustomRequest(
            null,
            List.of("abc"),
            CustomModelTests.getTestModel(TaskType.TEXT_EMBEDDING, new TextEmbeddingResponseParser("$.result.embeddings[*].embedding"))
        );
        InferenceServiceResults results = CustomResponseEntity.fromResponse(
            request,
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(results, instanceOf(TextEmbeddingFloatResults.class));
        assertThat(
            ((TextEmbeddingFloatResults) results).embeddings(),
            is(List.of(new TextEmbeddingFloatResults.Embedding(new float[] { -0.02868066355586052f, 0.022033605724573135f })))
        );
    }

    public void testFromSparseEmbeddingResponse() throws IOException {
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
                                    "weight": 0.10137939453125
                                },
                                {
                                    "tokenId": 163040,
                                    "weight": 0.2841796875
                                }
                            ]
                        }
                    ]
                }
            }
            """;

        var request = new CustomRequest(
            null,
            List.of("abc"),
            CustomModelTests.getTestModel(
                TaskType.SPARSE_EMBEDDING,
                new SparseEmbeddingResponseParser(
                    "$.result.sparse_embeddings[*].embedding[*].tokenId",
                    "$.result.sparse_embeddings[*].embedding[*].weight"
                )
            )
        );

        InferenceServiceResults results = CustomResponseEntity.fromResponse(
            request,
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );
        assertThat(results, instanceOf(SparseEmbeddingResults.class));

        SparseEmbeddingResults sparseEmbeddingResults = (SparseEmbeddingResults) results;

        List<SparseEmbeddingResults.Embedding> embeddingList = new ArrayList<>();
        List<WeightedToken> weightedTokens = new ArrayList<>();
        weightedTokens.add(new WeightedToken("6", 0.10137939453125f));
        weightedTokens.add(new WeightedToken("163040", 0.2841796875f));
        embeddingList.add(new SparseEmbeddingResults.Embedding(weightedTokens, false));

        for (int i = 0; i < embeddingList.size(); i++) {
            assertThat(sparseEmbeddingResults.embeddings().get(i), is(embeddingList.get(i)));
        }
    }

    public void testFromRerankResponse() throws IOException {
        String responseJson = """
            {
              "request_id": "450fcb80-f796-46c1-8d69-e1e86d29aa9f",
              "latency": 564.903929,
              "usage": {
                "doc_count": 2
              },
              "result": {
               "scores":[
                 {
                   "index":1,
                   "score": 1.37
                 },
                 {
                   "index":0,
                   "score": -0.3
                 }
               ]
              }
            }
            """;

        var request = new CustomRequest(
            null,
            List.of("abc"),
            CustomModelTests.getTestModel(
                TaskType.RERANK,
                new RerankResponseParser("$.result.scores[*].score", "$.result.scores[*].index", null)
            )
        );

        InferenceServiceResults results = CustomResponseEntity.fromResponse(
            request,
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(results, instanceOf(RankedDocsResults.class));
        var expected = new ArrayList<RankedDocsResults.RankedDoc>();
        expected.add(new RankedDocsResults.RankedDoc(1, 1.37F, null));
        expected.add(new RankedDocsResults.RankedDoc(0, -0.3F, null));

        for (int i = 0; i < ((RankedDocsResults) results).getRankedDocs().size(); i++) {
            assertThat(((RankedDocsResults) results).getRankedDocs().get(i).index(), is(expected.get(i).index()));
        }
    }

    public void testFromCompletionResponse() throws IOException {
        String responseJson = """
            {
              "request_id": "450fcb80-f796-****-8d69-e1e86d29aa9f",
              "latency": 564.903929,
              "result": {
                "text":"completion results"
              },
              "usage": {
                  "output_tokens": 6320,
                  "input_tokens": 35,
                  "total_tokens": 6355
              }
            }
            """;

        var request = new CustomRequest(
            null,
            List.of("abc"),
            CustomModelTests.getTestModel(TaskType.COMPLETION, new CompletionResponseParser("$.result.text"))
        );

        InferenceServiceResults results = CustomResponseEntity.fromResponse(
            request,
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(results, instanceOf(ChatCompletionResults.class));
        ChatCompletionResults chatCompletionResults = (ChatCompletionResults) results;
        assertThat(chatCompletionResults.getResults().size(), is(1));
        assertThat(chatCompletionResults.getResults().get(0).content(), is("completion results"));
    }
}
