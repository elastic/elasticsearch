/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.inference.InferenceResults;

import java.io.IOException;
import java.util.List;

/**
 * Writes a dense embedding result in the following json format
 * <pre>
 * {
 *     "text_embedding": [
 *         {
 *             "embedding": [
 *                 0.1
 *             ]
 *         },
 *         {
 *             "embedding": [
 *                 0.2
 *             ]
 *         }
 *     ]
 * }
 * </pre>
 */
public final class DenseEmbeddingFloatResults extends EmbeddingFloatResults {
    // This name is a holdover from before this class was renamed
    public static final String NAME = "text_embedding_service_results";
    public static final String TEXT_EMBEDDING = "text_embedding";

    public DenseEmbeddingFloatResults(List<EmbeddingFloatResults.Embedding> embeddings) {
        super(embeddings, TEXT_EMBEDDING);
    }

    public DenseEmbeddingFloatResults(StreamInput in) throws IOException {
        super(in, TEXT_EMBEDDING);
    }

    public static DenseEmbeddingFloatResults of(List<? extends InferenceResults> results) {
        return new DenseEmbeddingFloatResults(getEmbeddingsFromResults(results));
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
