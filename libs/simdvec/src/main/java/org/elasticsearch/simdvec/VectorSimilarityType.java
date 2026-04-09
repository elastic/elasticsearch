/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import org.apache.lucene.index.VectorSimilarityFunction;

/** Vector similarity type. */
public enum VectorSimilarityType {

    COSINE(VectorSimilarityFunction.COSINE),

    DOT_PRODUCT(VectorSimilarityFunction.DOT_PRODUCT),

    EUCLIDEAN(VectorSimilarityFunction.EUCLIDEAN),

    MAXIMUM_INNER_PRODUCT(VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT);

    private final VectorSimilarityFunction function;

    VectorSimilarityType(VectorSimilarityFunction function) {
        this.function = function;
    }

    public VectorSimilarityFunction function() {
        return function;
    }

    /** Converts from the given vector similarity type to this similarity type. */
    public static VectorSimilarityType of(VectorSimilarityFunction func) {
        return switch (func) {
            case EUCLIDEAN -> VectorSimilarityType.EUCLIDEAN;
            case COSINE -> VectorSimilarityType.COSINE;
            case DOT_PRODUCT -> VectorSimilarityType.DOT_PRODUCT;
            case MAXIMUM_INNER_PRODUCT -> VectorSimilarityType.MAXIMUM_INNER_PRODUCT;
        };
    }
}
