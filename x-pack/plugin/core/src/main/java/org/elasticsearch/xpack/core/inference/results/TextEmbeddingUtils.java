/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.common.Strings;

public class TextEmbeddingUtils {

    /**
     * Throws an exception if the number of elements in the input text list is different than the results in text embedding
     * response.
     */
    public static void validateInputSizeAgainstEmbeddings(int inputsSize, int embeddingSize) {
        if (inputsSize != embeddingSize) {
            throw new IllegalArgumentException(
                Strings.format("The number of inputs [%s] does not match the embeddings [%s]", inputsSize, embeddingSize)
            );
        }
    }

    private TextEmbeddingUtils() {}

}
