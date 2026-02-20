/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.List;

/**
 * Writes a dense embedding result in the following json format.
 * <pre>
 * {
 *     "embeddings_bits": [
 *         {
 *             "embedding": [
 *                 23
 *             ]
 *         },
 *         {
 *             "embedding": [
 *                 -23
 *             ]
 *         }
 *     ]
 * }
 * </pre>
 */
public final class GenericDenseEmbeddingBitResults extends EmbeddingBitResults {
    public static final String NAME = "embedding_bit_results";
    public static final String EMBEDDINGS_BITS = "embeddings_bits";

    public GenericDenseEmbeddingBitResults(List<EmbeddingByteResults.Embedding> embeddings) {
        super(embeddings, EMBEDDINGS_BITS);
    }

    public GenericDenseEmbeddingBitResults(StreamInput in) throws IOException {
        super(in, EMBEDDINGS_BITS);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
