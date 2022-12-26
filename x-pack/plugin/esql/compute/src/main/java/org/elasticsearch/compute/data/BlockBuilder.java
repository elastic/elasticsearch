/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;

public interface BlockBuilder {

    /**
     * Appends an int to the current entry.
     */
    BlockBuilder appendInt(int value);

    /**
     * Appends a long to the current entry.
     */
    BlockBuilder appendLong(long value);

    /**
     * Appends a double to the current entry;
     */
    BlockBuilder appendDouble(double value);

    /**
     * Appends a null value to the block.
     */
    BlockBuilder appendNull();

    /**
     * Appends a BytesRef to the current entry;
     */
    BlockBuilder appendBytesRef(BytesRef value);

    /**
     * Begins a multi-value entry.
     */
    BlockBuilder beginPositionEntry();

    /**
     * Ends the current multi-value entry.
     */
    BlockBuilder endPositionEntry();

    /**
     * Builds the block. This method can be called multiple times.
     */
    Block build();

    static BlockBuilder newIntBlockBuilder(int estimatedSize) {
        return new IntBlockBuilder(estimatedSize);
    }

    static Block newConstantIntBlockWith(int value, int positions) {
        return new VectorBlock(new ConstantIntVector(value, positions));
    }

    static BlockBuilder newLongBlockBuilder(int estimatedSize) {
        return new LongBlockBuilder(estimatedSize);
    }

    static Block newConstantLongBlockWith(long value, int positions) {
        return new VectorBlock(new ConstantLongVector(value, positions));
    }

    static BlockBuilder newDoubleBlockBuilder(int estimatedSize) {
        return new DoubleBlockBuilder(estimatedSize);
    }

    static Block newConstantDoubleBlockWith(double value, int positions) {
        return new VectorBlock(new ConstantDoubleVector(value, positions));
    }

    static BlockBuilder newBytesRefBlockBuilder(int estimatedSize) {
        return new BytesRefBlockBuilder(estimatedSize);
    }

    static Block newConstantBytesRefBlockWith(BytesRef value, int positions) {
        return new VectorBlock(new ConstantBytesRefVector(value, positions));
    }

    static Block newConstantNullBlockWith(int positions) {
        return new ConstantNullBlock(positions);
    }

}
