/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.elasticsearch.index.mapper.vectors.VectorSimilarityFloatValueSource;

/**
 * Subclass of VectorSimilarityFloatValueSource offering access to its members for other classes
 * in the same package.
 */
class AccessibleVectorSimilarityFloatValueSource extends VectorSimilarityFloatValueSource {

    String field;
    float[] target;
    VectorSimilarityFunction vectorSimilarityFunction;

    AccessibleVectorSimilarityFloatValueSource(String field, float[] target, VectorSimilarityFunction vectorSimilarityFunction) {
        super(field, target, vectorSimilarityFunction);
        this.field = field;
        this.target = target;
        this.vectorSimilarityFunction = vectorSimilarityFunction;
    }

    public String field() {
        return field;
    }

    public float[] target() {
        return target;
    }

    public VectorSimilarityFunction similarityFunction() {
        return vectorSimilarityFunction;
    }
}
