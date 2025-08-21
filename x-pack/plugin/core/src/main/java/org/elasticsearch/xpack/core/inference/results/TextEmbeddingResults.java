/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

public interface TextEmbeddingResults<E extends EmbeddingResults.Embedding<E>> extends EmbeddingResults<E> {

    /**
     * Returns the first text embedding entry in the result list's array size.
     * @return the size of the text embedding
     * @throws IllegalStateException if the list of embeddings is empty
     */
    int getFirstEmbeddingSize() throws IllegalStateException;
}
