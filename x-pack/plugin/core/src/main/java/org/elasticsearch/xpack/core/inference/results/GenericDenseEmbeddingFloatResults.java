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
 *     "embeddings": [
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
public final class GenericDenseEmbeddingFloatResults extends AbstractDenseEmbeddingFloatResults {
    public static final String NAME = "embedding_float_results";
    public static final String EMBEDDINGS = "embeddings";

    public GenericDenseEmbeddingFloatResults(List<Embedding> embeddings) {
        super(embeddings);
    }

    public GenericDenseEmbeddingFloatResults(StreamInput in) throws IOException {
        super(in);
    }

    public static GenericDenseEmbeddingFloatResults of(List<? extends InferenceResults> results) {
        return new GenericDenseEmbeddingFloatResults(getEmbeddingsFromResults(results));
    }

    @Override
    public String getArrayName() {
        return EMBEDDINGS;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
