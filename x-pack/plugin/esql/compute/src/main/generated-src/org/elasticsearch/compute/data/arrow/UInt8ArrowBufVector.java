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
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntLookup;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.core.ReleasableIterator;

import java.io.IOException;
// end generated imports

/**
 * Implementation of IntVector backed by an Arrow buffer holding unsigned 8 bits integers.
 * <p>
 * This class is generated. Edit {@code X-ArrowBufVector.java.st} instead.
 */
public final class UInt8ArrowBufVector extends AbstractArrowBufVector<IntVector, IntBlock> implements IntVector {

    /**
     *  Create an ArrowBuf vector based on the constituents of an Arrow <code>ValueVector</code>. The caller must retain the buffers if they
     *  are shared with other blocks or Arrow vectors.
     */
    public UInt8ArrowBufVector(ArrowBuf valueBuffer, int positionCount, BlockFactory blockFactory) {
        super(valueBuffer, positionCount, blockFactory);
    }

    private UInt8ArrowBufVector(FixedWidthVector arrowVector, BlockFactory blockFactory) {
        super(arrowVector, blockFactory);
    }

    public static UInt8ArrowBufVector of(FixedWidthVector arrowVector, BlockFactory blockFactory) {
        return new UInt8ArrowBufVector(arrowVector, blockFactory);
    }

    @Override
    protected ArrowBufVectorConstructor<IntVector> vectorConstructor() {
        return UInt8ArrowBufVector::new;
    }

    @Override
    protected ArrowBufBlockConstructor<IntBlock> blockConstructor() {
        return UInt8ArrowBufBlock::new;
    }

    @Override
    public int getInt(int valueIndex) {
        return Byte.toUnsignedInt(valueBuffer.getByte(valueIndex));
    }

    @Override
    protected int byteSize() {
        return Byte.BYTES;
    }

    @Override
    public int valueMaxByteSize() {
        return Integer.BYTES;
    }

    @Override
    public ElementType elementType() {
        return ElementType.INT;
    }

    @Override
    public ReleasableIterator<IntBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        return new IntLookup(asBlock(), positions, targetBlockSize);
    }

    @Override
    public IntVector slice(int beginInclusive, int endExclusive) {
        if (beginInclusive == 0 && endExclusive == getPositionCount()) {
            incRef();
            return this;
        }
        try (IntVector.FixedBuilder builder = blockFactory().newIntVectorFixedBuilder(endExclusive - beginInclusive)) {
            for (int i = beginInclusive; i < endExclusive; i++) {
                builder.appendInt(getInt(i));
            }
            return builder.build();
        }
    }

    @Override
    public int min() {
        int v = Integer.MAX_VALUE;
        for (int i = 0; i < positionCount; i++) {
            v = Math.min(v, getInt(i));
        }
        return v;
    }

    @Override
    public int max() {
        int v = Integer.MIN_VALUE;
        for (int i = 0; i < positionCount; i++) {
            v = Math.max(v, getInt(i));
        }
        return v;
    }

}
