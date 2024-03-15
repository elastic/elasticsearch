/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.vectors;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.elasticsearch.nativeaccess.VectorSimilarityType;

public final class VectorSimilarityTypeConverter {

    public static VectorSimilarityType of(VectorSimilarityFunction func) {
        return switch (func) {
            case EUCLIDEAN -> VectorSimilarityType.EUCLIDEAN;
            case COSINE -> VectorSimilarityType.COSINE;
            case DOT_PRODUCT -> VectorSimilarityType.DOT_PRODUCT;
            case MAXIMUM_INNER_PRODUCT -> VectorSimilarityType.MAXIMUM_INNER_PRODUCT;
        };
    }
}
