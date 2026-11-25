/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.search.VectorScorer;
import org.elasticsearch.search.internal.FilterFloatVectorValues;

import java.io.IOException;

/**
 * Provides appropriate values for iterating and providing HasSlice with the raw vector file
 * But still provides VectorScorer logic with the quantizedValues
 */
public final class QuantizedAndRawFloatVectorValues extends FilterFloatVectorValues {
    private final FloatVectorValues quantizedValues;

    public QuantizedAndRawFloatVectorValues(FloatVectorValues quantizedValues, FloatVectorValues rawValues) {
        // It's critical to pass rawValues up stream to satisfy the HasSlice implementation and that slice be
        // the raw vector slices
        super(rawValues);
        this.quantizedValues = quantizedValues;
    }

    @Override
    public VectorScorer scorer(float[] target) throws IOException {
        return quantizedValues.scorer(target);
    }

    public FloatVectorValues copy() throws IOException {
        return new QuantizedAndRawFloatVectorValues(quantizedValues.copy(), in.copy());
    }
}
