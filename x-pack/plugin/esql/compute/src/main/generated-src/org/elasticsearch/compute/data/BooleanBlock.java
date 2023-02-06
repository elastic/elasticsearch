/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

/**
 * Block that stores boolean values.
 * This class is generated. Do not edit it.
 */
public sealed interface BooleanBlock extends Block permits FilterBooleanBlock,BooleanArrayBlock,BooleanVectorBlock {

    /**
     * Retrieves the boolean value stored at the given value index.
     *
     * <p> Values for a given position are between getFirstValueIndex(position) (inclusive) and
     * getFirstValueIndex(position) + getValueCount(position) (exclusive).
     *
     * @param valueIndex the value index
     * @return the data value (as a boolean)
     */
    boolean getBoolean(int valueIndex);

    @Override
    BooleanVector asVector();

    @Override
    BooleanBlock getRow(int position);

    @Override
    BooleanBlock filter(int... positions);

    /**
     * Compares the given object with this block for equality. Returns {@code true} if and only if the
     * given object is a BooleanBlock, and both blocks are {@link #equals(BooleanBlock, BooleanBlock) equal}.
     */
    @Override
    boolean equals(Object obj);

    /** Returns the hash code of this block, as defined by {@link #hash(BooleanBlock)}. */
    @Override
    int hashCode();

    /**
     * Returns {@code true} if the given blocks are equal to each other, otherwise {@code false}.
     * Two blocks are considered equal if they have the same position count, and contain the same
     * values (including absent null values) in the same order. This definition ensures that the
     * equals method works properly across different implementations of the BooleanBlock interface.
     */
    static boolean equals(BooleanBlock block1, BooleanBlock block2) {
        final int positions = block1.getPositionCount();
        if (positions != block2.getPositionCount()) {
            return false;
        }
        for (int pos = 0; pos < positions; pos++) {
            if (block1.isNull(pos) || block2.isNull(pos)) {
                if (block1.isNull(pos) != block2.isNull(pos)) {
                    return false;
                }
            } else {
                final int valueCount = block1.getValueCount(pos);
                if (valueCount != block2.getValueCount(pos)) {
                    return false;
                }
                final int b1ValueIdx = block1.getFirstValueIndex(pos);
                final int b2ValueIdx = block2.getFirstValueIndex(pos);
                for (int valueIndex = 0; valueIndex < valueCount; valueIndex++) {
                    if (block1.getBoolean(b1ValueIdx + valueIndex) != block2.getBoolean(b2ValueIdx + valueIndex)) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    /**
     * Generates the hash code for the given block. The hash code is computed from the block's values.
     * This ensures that {@code block1.equals(block2)} implies that {@code block1.hashCode()==block2.hashCode()}
     * for any two blocks, {@code block1} and {@code block2}, as required by the general contract of
     * {@link Object#hashCode}.
     */
    static int hash(BooleanBlock block) {
        final int positions = block.getPositionCount();
        int result = 1;
        for (int pos = 0; pos < positions; pos++) {
            if (block.isNull(pos)) {
                result = 31 * result - 1;
            } else {
                final int valueCount = block.getValueCount(pos);
                result = 31 * result + valueCount;
                final int firstValueIdx = block.getFirstValueIndex(pos);
                for (int valueIndex = 0; valueIndex < valueCount; valueIndex++) {
                    result = 31 * result + Boolean.hashCode(block.getBoolean(firstValueIdx + valueIndex));
                }
            }
        }
        return result;
    }

    static Builder newBlockBuilder(int estimatedSize) {
        return new BooleanBlockBuilder(estimatedSize);
    }

    static BooleanBlock newConstantBlockWith(boolean value, int positions) {
        return new ConstantBooleanVector(value, positions).asBlock();
    }

    sealed interface Builder extends Block.Builder permits BooleanBlockBuilder {

        /**
         * Appends a boolean to the current entry.
         */
        Builder appendBoolean(boolean value);

        @Override
        Builder appendNull();

        @Override
        Builder beginPositionEntry();

        @Override
        Builder endPositionEntry();

        @Override
        BooleanBlock build();
    }
}
