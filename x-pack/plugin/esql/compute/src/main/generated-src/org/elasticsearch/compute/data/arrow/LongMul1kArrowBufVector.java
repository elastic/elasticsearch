/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.arrow;

// begin generated imports
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.FixedWidthVector;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongLookup;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.core.ReleasableIterator;

import java.io.IOException;
// end generated imports

/**
 * Implementation of LongVector backed by an Arrow buffer holding timestamps in seconds or microseconds, converted to millis or nanos.
 * <p>
 * This class is generated. Edit {@code X-ArrowBufVector.java.st} instead.
 */
public final class LongMul1kArrowBufVector extends AbstractArrowBufVector<LongVector, LongBlock> implements LongVector {

    /**
     *  Create an ArrowBuf vector based on the constituents of an Arrow <code>ValueVector</code>. The caller must retain the buffers if they
     *  are shared with other blocks or Arrow vectors.
     */
    public LongMul1kArrowBufVector(ArrowBuf valueBuffer, int positionCount, BlockFactory blockFactory) {
        super(valueBuffer, positionCount, blockFactory);
    }

    private LongMul1kArrowBufVector(FixedWidthVector arrowVector, BlockFactory blockFactory) {
        super(arrowVector, blockFactory);
    }

    public static LongMul1kArrowBufVector of(FixedWidthVector arrowVector, BlockFactory blockFactory) {
        return new LongMul1kArrowBufVector(arrowVector, blockFactory);
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
    protected int byteSize() {
        return Long.BYTES;
    }

    @Override
    public ElementType elementType() {
        return ElementType.LONG;
    }

    @Override
    public ReleasableIterator<LongBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        return new LongLookup(asBlock(), positions, targetBlockSize);
    }

    @Override
    public LongVector slice(int beginInclusive, int endExclusive) {
        if (beginInclusive == 0 && endExclusive == getPositionCount()) {
            incRef();
            return this;
        }
        try (LongVector.FixedBuilder builder = blockFactory().newLongVectorFixedBuilder(endExclusive - beginInclusive)) {
            for (int i = beginInclusive; i < endExclusive; i++) {
                builder.appendLong(getLong(i));
            }
            return builder.build();
        }
    }

}
