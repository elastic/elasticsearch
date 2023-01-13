/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;

/**
 * Block that stores BytesRef values.
 */
public sealed interface BytesRefBlock extends Block permits BytesRefArrayBlock,BytesRefVectorBlock,FilterBytesRefBlock {

    /**
     * Retrieves the ByteRef value stored at the given value index.
     *
     * <p> Values for a given position are between getFirstValueIndex(position) (inclusive) and
     * getFirstValueIndex(position) + getValueCount(position) (exclusive).
     *
     * @param valueIndex the value index
     * @param dest the destination
     * @return the data value (as a long)
     */
    BytesRef getBytesRef(int valueIndex, BytesRef dest);

    @Override
    BytesRefVector asVector();

    @Override
    BytesRefBlock getRow(int position);

    @Override
    BytesRefBlock filter(int... positions);

    static Builder newBytesRefBlockBuilder(int estimatedSize) {
        return new BytesRefBlockBuilder(estimatedSize);
    }

    static BytesRefBlock newConstantBytesRefBlockWith(BytesRef value, int positions) {
        return new ConstantBytesRefVector(value, positions).asBlock();
    }

    sealed interface Builder extends Block.Builder permits BytesRefBlockBuilder {

        /**
         * Appends a T to the current entry.
         */
        Builder appendBytesRef(BytesRef value);

        @Override
        Builder appendNull();

        @Override
        Builder beginPositionEntry();

        @Override
        Builder endPositionEntry();

        @Override
        BytesRefBlock build();
    }
}
