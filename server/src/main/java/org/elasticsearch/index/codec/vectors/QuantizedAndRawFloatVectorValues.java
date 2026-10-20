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
import org.apache.lucene.util.IOSupplier;
import org.elasticsearch.search.internal.FilterFloatVectorValues;

import java.io.IOException;

/**
 * Provides appropriate values for iterating and providing HasSlice with the raw vector file
 * But still provides VectorScorer logic with the quantizedValues
 */
public final class QuantizedAndRawFloatVectorValues extends FilterFloatVectorValues implements SequentialVectorValues {
    private final FloatVectorValues quantizedValues;
    private final IOSupplier<FloatVectorValues> sequentialRawValues;

    public QuantizedAndRawFloatVectorValues(FloatVectorValues quantizedValues, FloatVectorValues rawValues) {
        this(quantizedValues, rawValues, null);
    }

    /** @param sequentialRawValues the raw vectors read sequentially, see {@link SequentialRawVectors}; may be or return null */
    public QuantizedAndRawFloatVectorValues(
        FloatVectorValues quantizedValues,
        FloatVectorValues rawValues,
        IOSupplier<FloatVectorValues> sequentialRawValues
    ) {
        // It's critical to pass rawValues up stream to satisfy the HasSlice implementation and that slice be
        // the raw vector slices
        super(rawValues);
        this.quantizedValues = quantizedValues;
        this.sequentialRawValues = sequentialRawValues;
    }

    @Override
    public VectorScorer scorer(float[] target) throws IOException {
        return quantizedValues.scorer(target);
    }

    public FloatVectorValues copy() throws IOException {
        return new QuantizedAndRawFloatVectorValues(quantizedValues.copy(), in.copy(), sequentialRawValues);
    }

    @Override
    public FloatVectorValues sequential() throws IOException {
        FloatVectorValues sequential = sequentialRawValues == null ? null : sequentialRawValues.get();
        return sequential == null ? this : new QuantizedAndRawFloatVectorValues(quantizedValues, sequential);
    }
}
