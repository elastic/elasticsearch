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
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.FloatVector;
import org.elasticsearch.core.Nullable;
// end generated imports

/**
 * Arrow buffer backed FloatBlock.
 * This class is generated. Edit {@code X-ArrowBufBlock.java.st} instead.
 */
public final class FloatArrowBufBlock extends AbstractArrowBufBlock<FloatVector, FloatBlock> implements FloatBlock {

    /**
     *  Create an ArrowBuf block based on the constituents of an Arrow ValueVector. It does not take ownership of buffers but rather
     *  increases their reference count. This means that callers must release the buffers (and decrease their reference counters)
     *  if they don't need them anymore.
     */
    public FloatArrowBufBlock(
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
        return Float.BYTES;
    }

    @Override
    protected ArrowBufVectorConstructor<FloatVector> vectorConstructor() {
        return FloatArrowBufVector::new;
    }

    @Override
    protected ArrowBufBlockConstructor<FloatBlock> blockConstructor() {
        return FloatArrowBufBlock::new;
    }

    @Override
    public float getFloat(int valueIndex) {
        return valueBuffer.getFloat((long) valueIndex * Float.BYTES);
    }

    @Override
    public ElementType elementType() {
        return ElementType.FLOAT;
    }
}
