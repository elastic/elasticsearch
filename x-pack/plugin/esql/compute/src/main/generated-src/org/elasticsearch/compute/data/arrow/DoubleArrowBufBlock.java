/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.arrow;

// begin generated imports
import org.apache.arrow.memory.ArrowBuf;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.core.Nullable;
// end generated imports

/**
 * Arrow buffer backed DoubleBlock.
 * This class is generated. Edit {@code X-ArrowBufBlock.java.st} instead.
 */
public final class DoubleArrowBufBlock extends AbstractArrowBufBlock<DoubleVector, DoubleBlock> implements DoubleBlock {

    /**
     *  Create an ArrowBuf block based on the constituents of an Arrow ValueVector. It does not take ownership of buffers but rather
     *  increases their reference count. This means that callers must release the buffers (and decrease their reference counters)
     *  if they don't need them anymore.
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
