/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.common.bytes.BytesReference;
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
public interface EmbeddingResults<E extends EmbeddingResults.Embedding<E>> extends InferenceServiceResults {

    /**
     * A resulting embedding for one of the input texts to the inference service.
     */
    interface Embedding<E extends Embedding<E>> {
        /**
         * Merges the existing embedding and provided embedding into a new embedding.
         */
        E merge(E embedding);

        /**
         * Serializes the embedding to bytes.
         */
        BytesReference toBytesRef(XContent xContent) throws IOException;
    }

    /**
     * The resulting list of embeddings for the input texts to the inference service.
     */
    List<E> embeddings();

    /**
     * A resulting embedding together with the offset into the input text.
     */
    record Chunk(Embedding<?> embedding, ChunkedInference.TextOffset offset) {}
}
