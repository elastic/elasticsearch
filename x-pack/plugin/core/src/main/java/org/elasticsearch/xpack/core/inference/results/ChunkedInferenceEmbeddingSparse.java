/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.search.WeightedToken;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.core.inference.results.TextEmbeddingUtils.validateInputSizeAgainstEmbeddings;

public record ChunkedInferenceEmbeddingSparse(List<SparseEmbeddingChunk> chunks) {

    public static List<ChunkedInference> listOf(List<String> inputs, SparseEmbeddingResults sparseEmbeddingResults) {
        validateInputSizeAgainstEmbeddings(inputs, sparseEmbeddingResults.embeddings().size());

        var results = new ArrayList<ChunkedInference>(inputs.size());
        for (int i = 0; i < inputs.size(); i++) {
            results.add(
                new ChunkedInferenceEmbedding(
                    List.of(
                        new SparseEmbeddingChunk(
                            sparseEmbeddingResults.embeddings().get(i).tokens(),
                            inputs.get(i),
                            new ChunkedInference.TextOffset(0, inputs.get(i).length())
                        )
                    )
                )
            );
        }

        return results;
    }

    public record SparseEmbeddingChunk(List<WeightedToken> weightedTokens, String matchedText, ChunkedInference.TextOffset offset)
        implements
            EmbeddingResults.EmbeddingChunk {

        public ChunkedInference.Chunk toChunk(XContent xcontent) throws IOException {
            return new ChunkedInference.Chunk(matchedText, offset, toBytesReference(xcontent, weightedTokens));
        }

        private static BytesReference toBytesReference(XContent xContent, List<WeightedToken> tokens) throws IOException {
            XContentBuilder b = XContentBuilder.builder(xContent);
            b.startObject();
            for (var weightedToken : tokens) {
                weightedToken.toXContent(b, ToXContent.EMPTY_PARAMS);
            }
            b.endObject();
            return BytesReference.bytes(b);
        }
    }
}
