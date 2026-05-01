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
 * Implementation of LongBlock backed by an Arrow buffer holding timestamps in seconds or microseconds, converted to millis or nanos.
 * <p>
 * This class is generated. Edit {@code X-ArrowBufBlock.java.st} instead.
 */
public final class LongMul1kArrowBufBlock extends AbstractArrowBufBlock<LongVector, LongBlock> implements LongBlock {

    /**
     *  Create an ArrowBuf block based on the constituents of an Arrow <code>ValueVector</code>. The caller must retain the buffers if they
     *  are shared with other blocks or Arrow vectors.
     */
    public LongMul1kArrowBufBlock(
        ArrowBuf valueBuffer,
        @Nullable ArrowBuf validityBuffer,
        @Nullable ArrowBuf offsetBuffer,
        int valueCount,
        int offsetCount,
        BlockFactory blockFactory
    ) {
        super(valueBuffer, validityBuffer, offsetBuffer, valueCount, offsetCount, blockFactory);
    }

    private LongMul1kArrowBufBlock(ValueVector arrowVector, BlockFactory blockFactory) {
        super(arrowVector, blockFactory);
    }

    public static LongMul1kArrowBufBlock of(ValueVector arrowVector, BlockFactory blockFactory) {
        return new LongMul1kArrowBufBlock(arrowVector, blockFactory);
    }

    @Override
    protected int byteSize() {
        return Long.BYTES;
    }

    @Override
    protected ArrowBufVectorConstructor<LongVector> vectorConstructor() {
        return LongMul1kArrowBufVector::new;
    }

    @Override
    protected ArrowBufBlockConstructor<LongBlock> blockConstructor() {
        return LongMul1kArrowBufBlock::new;
    }

    @Override
    public long getLong(int valueIndex) {
        return valueBuffer.getLong((long) valueIndex * Long.BYTES) * 1000;
    }

    @Override
    public ElementType elementType() {
        return ElementType.LONG;
    }
}
