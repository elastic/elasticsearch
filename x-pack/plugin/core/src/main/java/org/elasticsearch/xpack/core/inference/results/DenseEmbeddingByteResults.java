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
 * Writes a dense embedding result in the following json format
 * <pre>
 * {
 *     "text_embedding_bytes": [
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
public final class DenseEmbeddingByteResults extends EmbeddingByteResults {
    // This name is a holdover from before this class was renamed
    public static final String NAME = "text_embedding_service_byte_results";
    public static final String TEXT_EMBEDDING_BYTES = "text_embedding_bytes";

    public DenseEmbeddingByteResults(List<EmbeddingByteResults.Embedding> embeddings) {
        super(embeddings, TEXT_EMBEDDING_BYTES);
    }

    public DenseEmbeddingByteResults(StreamInput in) throws IOException {
        super(in, TEXT_EMBEDDING_BYTES);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
