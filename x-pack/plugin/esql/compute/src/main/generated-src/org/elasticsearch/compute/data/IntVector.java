/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

/**
 * Vector that stores int values.
 * This class is generated. Do not edit it.
 */
public sealed interface IntVector extends Vector permits ConstantIntVector,FilterIntVector,IntArrayVector {

    int getInt(int position);

    @Override
    IntBlock asBlock();

    @Override
    IntVector filter(int... positions);

    static Builder newVectorBuilder(int estimatedSize) {
        return new IntVectorBuilder(estimatedSize);
    }

    sealed interface Builder extends Vector.Builder permits IntVectorBuilder {
        /**
         * Appends a int to the current entry.
         */
        Builder appendInt(int value);

        @Override
        IntVector build();
    }
}
