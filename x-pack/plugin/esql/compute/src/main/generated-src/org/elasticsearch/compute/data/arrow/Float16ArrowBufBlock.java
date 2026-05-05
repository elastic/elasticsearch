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
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.core.Nullable;
// end generated imports

/**
 * Implementation of DoubleBlock backed by an Arrow buffer holding 16 bits floats.
 * <p>
 * This class is generated. Edit {@code X-ArrowBufBlock.java.st} instead.
 */
public final class Float16ArrowBufBlock extends AbstractArrowBufBlock<DoubleVector, DoubleBlock> implements DoubleBlock {

    /**
     *  Create an ArrowBuf block based on the constituents of an Arrow <code>ValueVector</code>. The caller must retain the buffers if they
     *  are shared with other blocks or Arrow vectors.
     */
    public Float16ArrowBufBlock(
        ArrowBuf valueBuffer,
        @Nullable ArrowBuf validityBuffer,
        @Nullable ArrowBuf offsetBuffer,
        int valueCount,
        int offsetCount,
        BlockFactory blockFactory
    ) {
        super(valueBuffer, validityBuffer, offsetBuffer, valueCount, offsetCount, blockFactory);
    }

    private Float16ArrowBufBlock(ValueVector arrowVector, BlockFactory blockFactory) {
        super(arrowVector, blockFactory);
    }

    public static Float16ArrowBufBlock of(ValueVector arrowVector, BlockFactory blockFactory) {
        return new Float16ArrowBufBlock(arrowVector, blockFactory);
    }

    @Override
    protected int byteSize() {
        return Short.BYTES;
    }

    @Override
    public int valueMaxByteSize() {
        return Double.BYTES;
    }

    @Override
    protected ArrowBufVectorConstructor<DoubleVector> vectorConstructor() {
        return Float16ArrowBufVector::new;
    }

    @Override
    protected ArrowBufBlockConstructor<DoubleBlock> blockConstructor() {
        return Float16ArrowBufBlock::new;
    }

    @Override
    public double getDouble(int valueIndex) {
        return Float.float16ToFloat(valueBuffer.getShort((long) valueIndex * Short.BYTES));
    }

    @Override
    public ElementType elementType() {
        return ElementType.DOUBLE;
    }
}
