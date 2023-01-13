/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

/**
 * Block that stores long values.
 */
public sealed interface LongBlock extends Block permits FilterLongBlock,LongArrayBlock,LongVectorBlock {

    /**
     * Retrieves the long value stored at the given value index.
     *
     * <p> Values for a given position are between getFirstValueIndex(position) (inclusive) and
     * getFirstValueIndex(position) + getValueCount(position) (exclusive).
     *
     * @param valueIndex the value index
     * @return the data value (as a long)
     */
    long getLong(int valueIndex);

    @Override
    LongVector asVector();

    @Override
    LongBlock getRow(int position);

    @Override
    LongBlock filter(int... positions);

    static Builder newBlockBuilder(int estimatedSize) {
        return new LongBlockBuilder(estimatedSize);
    }

    static LongBlock newConstantBlockWith(long value, int positions) {
        return new ConstantLongVector(value, positions).asBlock();
    }

    sealed interface Builder extends Block.Builder permits LongBlockBuilder {

        /**
         * Appends a long to the current entry.
         */
        Builder appendLong(long value);

        @Override
        Builder appendNull();

        @Override
        Builder beginPositionEntry();

        @Override
        Builder endPositionEntry();

        @Override
        LongBlock build();
    }
}
