/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

/**
 * Block that stores double values.
 */
public sealed interface DoubleBlock extends Block permits DoubleArrayBlock,DoubleVectorBlock,FilterDoubleBlock {

    /**
     * Retrieves the double value stored at the given value index.
     *
     * <p> Values for a given position are between getFirstValueIndex(position) (inclusive) and
     * getFirstValueIndex(position) + getValueCount(position) (exclusive).
     *
     * @param valueIndex the value index
     * @return the data value (as a double)
     */
    double getDouble(int valueIndex);

    @Override
    DoubleVector asVector();

    @Override
    DoubleBlock getRow(int position);

    @Override
    DoubleBlock filter(int... positions);

    static Builder newBlockBuilder(int estimatedSize) {
        return new DoubleBlockBuilder(estimatedSize);
    }

    static DoubleBlock newConstantBlockWith(double value, int positions) {
        return new ConstantDoubleVector(value, positions).asBlock();
    }

    sealed interface Builder extends Block.Builder permits DoubleBlockBuilder {

        /**
         * Appends a double to the current entry.
         */
        Builder appendDouble(double value);

        @Override
        Builder appendNull();

        @Override
        Builder beginPositionEntry();

        @Override
        Builder endPositionEntry();

        @Override
        DoubleBlock build();
    }
}
