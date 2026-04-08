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
 * Implementation of DoubleBlock backed by an Arrow buffer holding 64 bits floats.
 * <p>
 * This class is generated. Edit {@code X-ArrowBufBlock.java.st} instead.
 */
public final class DoubleArrowBufBlock extends AbstractArrowBufBlock<DoubleVector, DoubleBlock> implements DoubleBlock {

    /**
     *  Create an ArrowBuf block based on the constituents of an Arrow <code>ValueVector</code>. The caller must retain the buffers if they
     *  are shared with other blocks or Arrow vectors.
     */
    public DoubleArrowBufBlock(
        ArrowBuf valueBuffer,
        @Nullable ArrowBuf validityBuffer,
        @Nullable ArrowBuf offsetBuffer,
        int valueCount,
        int offsetCount,
        BlockFactory blockFactory
    ) {
        super(valueBuffer, validityBuffer, offsetBuffer, valueCount, offsetCount, blockFactory);
    }

    private DoubleArrowBufBlock(ValueVector arrowVector, BlockFactory blockFactory) {
        super(arrowVector, blockFactory);
    }

    public static DoubleArrowBufBlock of(ValueVector arrowVector, BlockFactory blockFactory) {
        return new DoubleArrowBufBlock(arrowVector, blockFactory);
    }

    @Override
    protected int byteSize() {
        return Double.BYTES;
    }

    @Override
    protected ArrowBufVectorConstructor<DoubleVector> vectorConstructor() {
        return DoubleArrowBufVector::new;
    }

    @Override
    protected ArrowBufBlockConstructor<DoubleBlock> blockConstructor() {
        return DoubleArrowBufBlock::new;
    }

    @Override
    public double getDouble(int valueIndex) {
        return valueBuffer.getDouble((long) valueIndex * Double.BYTES);
    }

    @Override
    public ElementType elementType() {
        return ElementType.DOUBLE;
    }
}
