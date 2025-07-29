/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.common.Strings;

import java.util.List;

public class TextEmbeddingUtils {

    /**
     * Throws an exception if the number of elements in the input text list is different than the results in text embedding
     * response.
     */
    public static void validateInputSizeAgainstEmbeddings(List<String> inputs, int embeddingSize) {
        if (inputs.size() != embeddingSize) {
            throw new IllegalArgumentException(
                Strings.format("The number of inputs [%s] does not match the embeddings [%s]", inputs.size(), embeddingSize)
            );
        }
    }

    private TextEmbeddingUtils() {}

}
