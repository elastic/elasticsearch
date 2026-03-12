/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker.schema.elastic;

import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.WeightedToken;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;

import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

public class ElasticSparseEmbeddingPayloadTests extends ElasticPayloadTestCase<ElasticSparseEmbeddingPayload> {
    @Override
    protected ElasticSparseEmbeddingPayload payload() {
        return new ElasticSparseEmbeddingPayload();
    }

    @Override
    protected Set<TaskType> expectedSupportedTaskTypes() {
        return Set.of(TaskType.SPARSE_EMBEDDING);
    }

    public void testParseResponse() throws Exception {
        var responseJson = """
            {
                 "sparse_embedding" : [
                     {
                         "is_truncated" : false,
                         "embedding" : {
                             "token" : 0.1
                         }
                     },
                     {
                         "is_truncated" : false,
                         "embedding" : {
                             "token2" : 0.2,
                             "token3" : 0.3
                         }
                     }
                 ]
            }
            """;

        var results = payload.responseBody(mockModel(), invokeEndpointResponse(responseJson));

        assertThat(results.embeddings().size(), is(2));

        assertThat(
            results.embeddings(),
            containsInAnyOrder(
                new SparseEmbeddingResults.Embedding(List.of(new WeightedToken("token", 0.1F)), false),
                new SparseEmbeddingResults.Embedding(List.of(new WeightedToken("token2", 0.2F), new WeightedToken("token3", 0.3F)), false)
            )
        );
    }
}
