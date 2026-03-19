/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BitVector;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanLookup;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.core.ReleasableIterator;

import java.io.IOException;

/**
 * Arrow buffer backed {@link BooleanVector}. Booleans are bit-packed in Arrow (1 bit per value),
 * so this class overrides {@link #filter} with a bit-level implementation.
 */
public final class BooleanArrowBufVector extends AbstractArrowBufVector<BooleanVector, BooleanBlock> implements BooleanVector {

    public BooleanArrowBufVector(ArrowBuf valueBuffer, int positionCount, BlockFactory blockFactory) {
        super(valueBuffer, positionCount, blockFactory);
    }

    public static BooleanArrowBufVector of(BitVector bitVector, BlockFactory blockFactory) {
        if (bitVector.getNullCount() > 0) {
            throw new IllegalArgumentException("Expecting no nulls in " + bitVector.getClass().getSimpleName());
        }
        var result = new BooleanArrowBufVector(bitVector.getDataBuffer(), bitVector.getValueCount(), blockFactory);
        result.valueBuffer.getReferenceManager().retain();
        return result;
    }

    @Override
    protected ArrowBufVectorConstructor<BooleanVector> vectorConstructor() {
        return BooleanArrowBufVector::new;
    }

    @Override
    protected ArrowBufBlockConstructor<BooleanBlock> blockConstructor() {
        return BooleanArrowBufBlock::new;
    }

    @Override
    public boolean getBoolean(int position) {
        return (valueBuffer.getByte(position / 8) & (1 << (position % 8))) != 0;
    }

    @Override
    protected int byteSize() {
        throw new UnsupportedOperationException("booleans are bit-packed; byteSize() is not applicable");
    }

    @Override
    public ElementType elementType() {
        return ElementType.BOOLEAN;
    }

    @Override
    public boolean allTrue() {
        for (int i = 0; i < positionCount; i++) {
            if (getBoolean(i) == false) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean allFalse() {
        for (int i = 0; i < positionCount; i++) {
            if (getBoolean(i)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public ReleasableIterator<BooleanBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        return new BooleanLookup(asBlock(), positions, targetBlockSize);
    }

    @Override
    public BooleanVector filter(boolean mayContainDuplicates, int... positions) {
        var allocator = blockFactory.arrowAllocator();
        int bufLen = ((positions.length + 63) / 64) * Long.BYTES;
        var buffer = allocator.buffer(Math.max(1, bufLen));
        buffer.setZero(0, buffer.capacity());
        for (int i = 0; i < positions.length; i++) {
            if (getBoolean(positions[i])) {
                int byteIdx = i / 8;
                buffer.setByte(byteIdx, buffer.getByte(byteIdx) | (1 << (i % 8)));
            }
        }

        return vectorConstructor().create(buffer, positions.length, blockFactory);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        BooleanVector.super.writeTo(out);
    }
}
