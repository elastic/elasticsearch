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
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.core.Nullable;
// end generated imports

/**
 * Implementation of IntBlock backed by an Arrow buffer holding unsigned 16 bits integers.
 * <p>
 * This class is generated. Edit {@code X-ArrowBufBlock.java.st} instead.
 */
public final class UInt16ArrowBufBlock extends AbstractArrowBufBlock<IntVector, IntBlock> implements IntBlock {

    /**
     *  Create an ArrowBuf block based on the constituents of an Arrow <code>ValueVector</code>. The caller must retain the buffers if they
     *  are shared with other blocks or Arrow vectors.
     */
    public UInt16ArrowBufBlock(
        ArrowBuf valueBuffer,
        @Nullable ArrowBuf validityBuffer,
        @Nullable ArrowBuf offsetBuffer,
        int valueCount,
        int offsetCount,
        BlockFactory blockFactory
    ) {
        super(valueBuffer, validityBuffer, offsetBuffer, valueCount, offsetCount, blockFactory);
    }

    private UInt16ArrowBufBlock(ValueVector arrowVector, BlockFactory blockFactory) {
        super(arrowVector, blockFactory);
    }

    public static UInt16ArrowBufBlock of(ValueVector arrowVector, BlockFactory blockFactory) {
        return new UInt16ArrowBufBlock(arrowVector, blockFactory);
    }

    @Override
    protected int byteSize() {
        return Short.BYTES;
    }

    @Override
    public int valueMaxByteSize() {
        return Integer.BYTES;
    }

    @Override
    protected ArrowBufVectorConstructor<IntVector> vectorConstructor() {
        return UInt16ArrowBufVector::new;
    }

    @Override
    protected ArrowBufBlockConstructor<IntBlock> blockConstructor() {
        return UInt16ArrowBufBlock::new;
    }

    @Override
    public int getInt(int valueIndex) {
        return Short.toUnsignedInt(valueBuffer.getShort((long) valueIndex * Short.BYTES));
    }

    @Override
    public ElementType elementType() {
        return ElementType.INT;
    }
}
