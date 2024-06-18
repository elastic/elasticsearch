/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.results;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.inference.results.InferenceChunkedSparseEmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.ChunkedNlpInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.MlChunkedTextExpansionResults;
import org.elasticsearch.xpack.core.ml.search.WeightedToken;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class InferenceChunkedSparseEmbeddingResultsTests extends AbstractWireSerializingTestCase<InferenceChunkedSparseEmbeddingResults> {

    public static InferenceChunkedSparseEmbeddingResults createRandomResults() {
        var chunks = new ArrayList<MlChunkedTextExpansionResults.ChunkedResult>();
        int numChunks = randomIntBetween(1, 5);

        for (int i = 0; i < numChunks; i++) {
            var tokenWeights = new ArrayList<WeightedToken>();
            int numTokens = randomIntBetween(1, 8);
            for (int j = 0; j < numTokens; j++) {
                tokenWeights.add(new WeightedToken(Integer.toString(j), (float) randomDoubleBetween(0.0, 5.0, false)));
            }
            chunks.add(new MlChunkedTextExpansionResults.ChunkedResult(randomAlphaOfLength(6), tokenWeights));
        }

        return new InferenceChunkedSparseEmbeddingResults(chunks);
    }

    public void testToXContent_CreatesTheRightJsonForASingleChunk() {
        var entity = new InferenceChunkedSparseEmbeddingResults(
            List.of(new MlChunkedTextExpansionResults.ChunkedResult("text", List.of(new WeightedToken("token", 0.1f))))
        );

        assertThat(
            entity.asMap(),
            is(
                Map.of(
                    InferenceChunkedSparseEmbeddingResults.FIELD_NAME,
                    List.of(Map.of(ChunkedNlpInferenceResults.TEXT, "text", ChunkedNlpInferenceResults.INFERENCE, Map.of("token", 0.1f)))
                )
            )
        );

        String xContentResult = Strings.toString(entity, true, true);
        assertThat(xContentResult, is("""
            {
              "sparse_embedding_chunk" : [
                {
                  "text" : "text",
                  "inference" : {
                    "token" : 0.1
                  }
                }
              ]
            }"""));
    }

    public void testToXContent_CreatesTheRightJsonForASingleChunk_FromSparseEmbeddingResults() {
        var entity = InferenceChunkedSparseEmbeddingResults.listOf(
            List.of("text"),
            new SparseEmbeddingResults(List.of(new SparseEmbeddingResults.Embedding(List.of(new WeightedToken("token", 0.1f)), false)))
        );

        assertThat(entity.size(), is(1));

        var firstEntry = entity.get(0);

        assertThat(
            firstEntry.asMap(),
            is(
                Map.of(
                    InferenceChunkedSparseEmbeddingResults.FIELD_NAME,
                    List.of(Map.of(ChunkedNlpInferenceResults.TEXT, "text", ChunkedNlpInferenceResults.INFERENCE, Map.of("token", 0.1f)))
                )
            )
        );

        String xContentResult = Strings.toString(firstEntry, true, true);
        assertThat(xContentResult, is("""
            {
              "sparse_embedding_chunk" : [
                {
                  "text" : "text",
                  "inference" : {
                    "token" : 0.1
                  }
                }
              ]
            }"""));
    }

    public void testToXContent_ThrowsWhenInputSizeIsDifferentThanEmbeddings() {
        var exception = expectThrows(
            IllegalArgumentException.class,
            () -> InferenceChunkedSparseEmbeddingResults.listOf(
                List.of("text", "text2"),
                new SparseEmbeddingResults(List.of(new SparseEmbeddingResults.Embedding(List.of(new WeightedToken("token", 0.1f)), false)))
            )
        );

        assertThat(exception.getMessage(), is("The number of inputs [2] does not match the embeddings [1]"));
    }

    @Override
    protected Writeable.Reader<InferenceChunkedSparseEmbeddingResults> instanceReader() {
        return InferenceChunkedSparseEmbeddingResults::new;
    }

    @Override
    protected InferenceChunkedSparseEmbeddingResults createTestInstance() {
        return createRandomResults();
    }

    @Override
    protected InferenceChunkedSparseEmbeddingResults mutateInstance(InferenceChunkedSparseEmbeddingResults instance) throws IOException {
        return randomValueOtherThan(instance, InferenceChunkedSparseEmbeddingResultsTests::createRandomResults);
    }
}
