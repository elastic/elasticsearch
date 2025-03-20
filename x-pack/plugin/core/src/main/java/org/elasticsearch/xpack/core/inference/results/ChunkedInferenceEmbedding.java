/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.xcontent.XContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.elasticsearch.xpack.core.inference.results.TextEmbeddingUtils.validateInputSizeAgainstEmbeddings;

public record ChunkedInferenceEmbedding(List<EmbeddingResults.Chunk> chunks) implements ChunkedInference {

    public static List<ChunkedInference> listOf(List<String> inputs, SparseEmbeddingResults sparseEmbeddingResults) {
        validateInputSizeAgainstEmbeddings(inputs, sparseEmbeddingResults.embeddings().size());

        var results = new ArrayList<ChunkedInference>(inputs.size());
        for (int i = 0; i < inputs.size(); i++) {
            results.add(
                new ChunkedInferenceEmbedding(
                    List.of(
                        new EmbeddingResults.Chunk(sparseEmbeddingResults.embeddings().get(i), new TextOffset(0, inputs.get(i).length()))
                    )
                )
            );
        }

        return results;
    }

    @Override
    public Iterator<Chunk> chunksAsByteReference(XContent xcontent) throws IOException {
        List<Chunk> chunkedInferenceChunks = new ArrayList<>();
        for (EmbeddingResults.Chunk embeddingResultsChunk : chunks()) {
            chunkedInferenceChunks.add(new Chunk(embeddingResultsChunk.offset(), embeddingResultsChunk.embedding().toBytesRef(xcontent)));
        }
        return chunkedInferenceChunks.iterator();
    }
}
