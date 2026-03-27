/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.arrow;

// begin generated imports
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.ValueVector;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.core.Nullable;
// end generated imports

/**
 * Implementation of LongBlock backed by an Arrow buffer holding unsigned 32 bits integers.
 * <p>
 * This class is generated. Edit {@code X-ArrowBufBlock.java.st} instead.
 */
public final class UInt32ArrowBufBlock extends AbstractArrowBufBlock<LongVector, LongBlock> implements LongBlock {

    /**
     *  Create an ArrowBuf block based on the constituents of an Arrow <code>ValueVector</code>. The caller must retain the buffers if they
     *  are shared with other blocks or Arrow vectors.
     */
    public UInt32ArrowBufBlock(
        ArrowBuf valueBuffer,
        @Nullable ArrowBuf validityBuffer,
        @Nullable ArrowBuf offsetBuffer,
        int valueCount,
        int offsetCount,
        BlockFactory blockFactory
    ) {
        super(valueBuffer, validityBuffer, offsetBuffer, valueCount, offsetCount, blockFactory);
    }

    private UInt32ArrowBufBlock(ValueVector arrowVector, BlockFactory blockFactory) {
        super(arrowVector, blockFactory);
    }

    public static UInt32ArrowBufBlock of(ValueVector arrowVector, BlockFactory blockFactory) {
        return new UInt32ArrowBufBlock(arrowVector, blockFactory);
    }

    @Override
    protected int byteSize() {
        return Integer.BYTES;
    }

    @Override
    protected ArrowBufVectorConstructor<LongVector> vectorConstructor() {
        return UInt32ArrowBufVector::new;
    }

    @Override
    protected ArrowBufBlockConstructor<LongBlock> blockConstructor() {
        return UInt32ArrowBufBlock::new;
    }

    @Override
    public long getLong(int valueIndex) {
        return Integer.toUnsignedLong(valueBuffer.getInt((long) valueIndex * Integer.BYTES));
    }

    @Override
    public ElementType elementType() {
        return ElementType.LONG;
    }
}
