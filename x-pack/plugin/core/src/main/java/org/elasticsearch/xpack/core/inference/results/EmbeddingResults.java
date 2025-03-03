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

/**
 * The results of a call to the inference service that contains embeddings (sparse or dense).
 * A call to the inference service may contain multiple input texts, so this results may
 * contain multiple results.
 */
public interface EmbeddingResults<C extends EmbeddingResults.Chunk, E extends EmbeddingResults.Embedding<C>>
    extends
        InferenceServiceResults {

    /**
     * A resulting embedding together with its input text.
     */
    interface Chunk {
        ChunkedInference.Chunk toChunk(XContent xcontent) throws IOException;

        String matchedText();

        ChunkedInference.TextOffset offset();
    }

    /**
     * A resulting embedding for one of the input texts to the inference service.
     */
    interface Embedding<C extends Chunk> {
        /**
         * Combines the resulting embedding with the input into a chunk.
         */
        C toChunk(String text, ChunkedInference.TextOffset offset);
    }

    /**
     * The resulting list of embeddings for the input texts to the inference service.
     */
    List<E> embeddings();
}
