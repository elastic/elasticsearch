/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xcontent.XContent;

import java.io.IOException;
import java.util.List;

public interface EmbeddingResults<C extends EmbeddingResults.EmbeddingChunk, E extends EmbeddingResults.EmbeddingResult<C>>
    extends
        InferenceServiceResults {

    interface EmbeddingChunk {
        ChunkedInference.Chunk toChunk(XContent xcontent) throws IOException;

        String matchedText();

        ChunkedInference.TextOffset offset();
    }

    interface EmbeddingResult<C extends EmbeddingResults.EmbeddingChunk> {
        C toEmbeddingChunk(String text, ChunkedInference.TextOffset offset);
    }

    List<E> embeddings();
}
