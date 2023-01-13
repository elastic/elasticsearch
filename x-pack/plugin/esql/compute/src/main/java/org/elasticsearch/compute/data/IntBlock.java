/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

/**
 * Block that stores int values.
 */
public sealed interface IntBlock extends Block permits FilterIntBlock,IntArrayBlock,IntVectorBlock {

    /**
     * Retrieves the integer value stored at the given value index.
     *
     * <p> Values for a given position are between getFirstValueIndex(position) (inclusive) and
     * getFirstValueIndex(position) + getValueCount(position) (exclusive).
     *
     * @param valueIndex the value index
     * @return the data value (as an int)
     */
    int getInt(int valueIndex);

    @Override
    IntVector asVector();

    @Override
    IntBlock getRow(int position);

    @Override
    IntBlock filter(int... positions);

    LongBlock asLongBlock();

    static Builder newBlockBuilder(int estimatedSize) {
        return new IntBlockBuilder(estimatedSize);
    }

    static IntBlock newConstantBlockWith(int value, int positions) {
        return new ConstantIntVector(value, positions).asBlock();
    }

    sealed interface Builder extends Block.Builder permits IntBlockBuilder {

        /**
         * Appends an int to the current entry.
         */
        Builder appendInt(int value);

        @Override
        Builder appendNull();

        @Override
        Builder beginPositionEntry();

        @Override
        Builder endPositionEntry();

        @Override
        IntBlock build();
    }
}
