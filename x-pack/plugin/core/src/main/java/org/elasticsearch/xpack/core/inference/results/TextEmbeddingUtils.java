/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import java.util.List;

public class TextEmbeddingUtils {

    /**
     * Returns the first text embedding entry's array size.
     * @param embeddings the list of embeddings
     * @return the size of the text embedding
     * @throws IllegalStateException if the list of embeddings is empty
     */
    public static int getFirstEmbeddingSize(List<EmbeddingInt> embeddings) throws IllegalStateException {
        if (embeddings.isEmpty()) {
            throw new IllegalStateException("Embeddings list is empty");
        }

        return embeddings.get(0).getSize();
    }

    private TextEmbeddingUtils() {}

}
