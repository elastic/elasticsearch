/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

/**
 * Vector that stores long values.
 * This class is generated. Do not edit it.
 */
public sealed interface LongVector extends Vector permits ConstantLongVector,FilterLongVector,LongArrayVector {

    long getLong(int position);

    @Override
    LongBlock asBlock();

    @Override
    LongVector filter(int... positions);

    static Builder newVectorBuilder(int estimatedSize) {
        return new LongVectorBuilder(estimatedSize);
    }

    sealed interface Builder extends Vector.Builder permits LongVectorBuilder {
        /**
         * Appends a long to the current entry.
         */
        Builder appendLong(long value);

        @Override
        LongVector build();
    }
}
