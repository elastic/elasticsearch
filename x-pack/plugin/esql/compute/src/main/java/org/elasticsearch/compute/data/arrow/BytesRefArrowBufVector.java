/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.VarCharVector;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefLookup;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefVector;
import org.elasticsearch.core.ReleasableIterator;

import java.io.IOException;

/**
 * Arrow buffer backed {@link BytesRefVector}. Variable-length values use an additional offsets buffer
 * that maps each value index to a byte range in the data buffer.
 */
public final class BytesRefArrowBufVector extends AbstractArrowBufVector<BytesRefVector, BytesRefBlock> implements BytesRefVector {

    private final ArrowBuf valueOffsetsBuffer;

    public BytesRefArrowBufVector(ArrowBuf valueBuffer, ArrowBuf valueOffsetsBuffer, int positionCount, BlockFactory blockFactory) {
        super(valueBuffer, positionCount, blockFactory);
        this.valueOffsetsBuffer = valueOffsetsBuffer;
    }

    public static BytesRefArrowBufVector of(VarCharVector arrowVec, BlockFactory blockFactory) {

        if (arrowVec.getNullCount() > 0) {
            throw new IllegalArgumentException("Expecting no nulls in " + arrowVec.getClass().getSimpleName());
        }

        var result = new BytesRefArrowBufVector(
            arrowVec.getDataBuffer(),
            arrowVec.getOffsetBuffer(),
            arrowVec.getValueCount(),
            blockFactory
        );
        result.retainBuffers();
        return result;
    }

    @Override
    protected ArrowBufVectorConstructor<BytesRefVector> vectorConstructor() {
        throw new UnsupportedOperationException("use constructors directly");
    }

    @Override
    protected ArrowBufBlockConstructor<BytesRefBlock> blockConstructor() {
        throw new UnsupportedOperationException("use constructors directly");
    }

    @Override
    public void retainBuffers() {
        super.retainBuffers();
        this.valueOffsetsBuffer.getReferenceManager().retain();
    }

    @Override
    public void releaseBuffers() {
        super.releaseBuffers();
        this.valueOffsetsBuffer.getReferenceManager().release();
    }

    @Override
    public BytesRef getBytesRef(int position, BytesRef dest) {
        int start = valueOffsetsBuffer.getInt((long) position * Integer.BYTES);
        int end = valueOffsetsBuffer.getInt((long) (position + 1) * Integer.BYTES);
        int length = end - start;
        if (dest.bytes.length < length) {
            dest.bytes = new byte[length];
        }
        dest.offset = 0;
        dest.length = length;
        valueBuffer.getBytes(start, dest.bytes, 0, length);
        return dest;
    }

    @Override
    protected int byteSize() {
        throw new UnsupportedOperationException("BytesRef values are variable-length");
    }

    @Override
    public ElementType elementType() {
        return ElementType.BYTES_REF;
    }

    @Override
    public OrdinalBytesRefVector asOrdinals() {
        return null;
    }

    @Override
    public BytesRefBlock asBlock() {
        return new BytesRefArrowBufBlock(valueBuffer, valueOffsetsBuffer, null, null, positionCount, 0, blockFactory);
    }

    @Override
    public long ramBytesUsed() {
        return super.ramBytesUsed() + valueOffsetsBuffer.getActualMemoryConsumed();
    }

    @Override
    public BytesRefVector slice(int beginInclusive, int endExclusive) {
        if (beginInclusive == 0 && endExclusive == getPositionCount()) {
            incRef();
            return this;
        }
        BytesRef scratch = new BytesRef();
        try (BytesRefVector.Builder builder = blockFactory().newBytesRefVectorBuilder(endExclusive - beginInclusive)) {
            for (int i = beginInclusive; i < endExclusive; i++) {
                builder.appendBytesRef(getBytesRef(i, scratch));
            }
            return builder.build();
        }
    }

    @Override
    public ReleasableIterator<BytesRefBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        return new BytesRefLookup(asBlock(), positions, targetBlockSize);
    }

    @Override
    public BytesRefVector filter(boolean mayContainDuplicates, int... positions) {
        var allocator = blockFactory.arrowAllocator();

        // Precompute sizes
        int totalBytes = 0;
        for (int pos : positions) {
            int start = valueOffsetsBuffer.getInt((long) pos * Integer.BYTES);
            int end = valueOffsetsBuffer.getInt((long) (pos + 1) * Integer.BYTES);
            totalBytes += (end - start);
        }

        ArrowBuf newValues = null, newValueOffsets = null;
        boolean success = false;
        try {
            newValues = allocator.buffer(totalBytes);
            newValueOffsets = allocator.buffer((long) (positions.length + 1) * Integer.BYTES);
            success = true;
        } finally {
            if (success == false) {
                ArrowUtils.releaseBuffers(newValues, newValueOffsets);
            }
        }
        int byteIdx = 0;
        for (int i = 0; i < positions.length; i++) {
            int pos = positions[i];
            newValueOffsets.setInt((long) i * Integer.BYTES, byteIdx);
            int srcStart = valueOffsetsBuffer.getInt((long) pos * Integer.BYTES);
            int srcEnd = valueOffsetsBuffer.getInt((long) (pos + 1) * Integer.BYTES);
            int length = srcEnd - srcStart;
            if (length > 0) {
                newValues.setBytes(byteIdx, valueBuffer, srcStart, length);
                byteIdx += length;
            }
        }
        newValueOffsets.setInt((long) positions.length * Integer.BYTES, byteIdx);
        return new BytesRefArrowBufVector(newValues, newValueOffsets, positions.length, blockFactory);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        BytesRefVector.super.writeTo(out);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof BytesRefVector that) {
            return BytesRefVector.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return BytesRefVector.hash(this);
    }
}
