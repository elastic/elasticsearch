/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

/**
 * Arrow buffer backed {@link BytesRefBlock}. Variable-length values use an additional offsets buffer
 * that maps each value index to a byte range in the data buffer, following the Arrow variable-length
 * binary layout. This is separate from the position-to-value-index offsets used for multi-valued fields.
 */
public final class BytesRefArrowBufBlock extends AbstractArrowBufBlock<BytesRefVector, BytesRefBlock> implements BytesRefBlock {

    private final ArrowBuf valueOffsetsBuffer;

    /**
     * @param valueBuffer        raw byte data
     * @param valueOffsetsBuffer int32 offsets into valueBuffer for each value (Arrow variable-length layout)
     * @param validityBuffer     null bitmap (nullable)
     * @param offsetBuffer       position-to-value-index mapping for multi-valued fields (nullable)
     * @param valueCount         number of positions
     * @param offsetCount        number of position offset entries
     * @param blockFactory       block factory
     */
    public BytesRefArrowBufBlock(
        ArrowBuf valueBuffer,
        ArrowBuf valueOffsetsBuffer,
        @Nullable ArrowBuf validityBuffer,
        @Nullable ArrowBuf offsetBuffer,
        int valueCount,
        int offsetCount,
        BlockFactory blockFactory
    ) {
        super(valueBuffer, validityBuffer, offsetBuffer, valueCount, offsetCount, blockFactory);
        valueOffsetsBuffer.getReferenceManager().retain();
        this.valueOffsetsBuffer = valueOffsetsBuffer;
    }

    @Override
    protected int byteSize() {
        throw new UnsupportedOperationException("BytesRef values are variable-length");
    }

    @Override
    protected ArrowBufVectorConstructor<BytesRefVector> vectorConstructor() {
        throw new UnsupportedOperationException("use asVector() directly");
    }

    @Override
    protected ArrowBufBlockConstructor<BytesRefBlock> blockConstructor() {
        throw new UnsupportedOperationException("use constructors directly");
    }

    @Override
    public BytesRef getBytesRef(int valueIndex, BytesRef dest) {
        int start = valueOffsetsBuffer.getInt((long) valueIndex * Integer.BYTES);
        int end = valueOffsetsBuffer.getInt((long) (valueIndex + 1) * Integer.BYTES);
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
    public ElementType elementType() {
        return ElementType.BYTES_REF;
    }

    @Override
    public OrdinalBytesRefBlock asOrdinals() {
        return null;
    }

    @Override
    public BytesRefVector asVector() {
        if (validityBuffer == null && offsetBuffer == null) {
            return new BytesRefArrowBufVector(valueBuffer, valueOffsetsBuffer, valueCount, blockFactory);
        }
        return null;
    }

    @Override
    protected void closeInternal() {
        if (closed) {
            return;
        }
        super.closeInternal();
        this.valueOffsetsBuffer.getReferenceManager().release();
    }

    @Override
    public long ramBytesUsed() {
        return super.ramBytesUsed() + valueOffsetsBuffer.getActualMemoryConsumed();
    }

    private int valueByteLength(int valueIndex) {
        int start = valueOffsetsBuffer.getInt((long) valueIndex * Integer.BYTES);
        int end = valueOffsetsBuffer.getInt((long) (valueIndex + 1) * Integer.BYTES);
        return end - start;
    }

    @Override
    public BytesRefBlock filter(boolean mayContainDuplicates, int... positions) {
        var allocator = valueBuffer.getReferenceManager().getAllocator();

        if (offsetBuffer == null) {
            int totalBytes = 0;
            for (int pos : positions) {
                if (isNull(pos) == false) {
                    totalBytes += valueByteLength(pos);
                }
            }

            ArrowBuf newValues = allocator.buffer(Math.max(1, totalBytes));
            ArrowBuf newValueOffsets = allocator.buffer((long) (positions.length + 1) * Integer.BYTES);
            ArrowBuf newValidity = validityBuffer != null ? allocator.buffer(validityBufferLength(positions.length)) : null;
            if (newValidity != null) {
                newValidity.setZero(0, newValidity.capacity());
            }
            try {
                int byteIdx = 0;
                for (int i = 0; i < positions.length; i++) {
                    int pos = positions[i];
                    newValueOffsets.setInt((long) i * Integer.BYTES, byteIdx);
                    if (isNull(pos) == false) {
                        int srcStart = valueOffsetsBuffer.getInt((long) pos * Integer.BYTES);
                        int length = valueByteLength(pos);
                        if (length > 0) {
                            newValues.setBytes(byteIdx, valueBuffer, srcStart, length);
                            byteIdx += length;
                        }
                        if (newValidity != null) {
                            setValidityBit(newValidity, i);
                        }
                    }
                }
                newValueOffsets.setInt((long) positions.length * Integer.BYTES, byteIdx);
                return new BytesRefArrowBufBlock(newValues, newValueOffsets, newValidity, null, positions.length, 0, blockFactory);
            } finally {
                releaseBuffers(newValues, newValueOffsets, newValidity);
            }
        }

        // Multi-valued: compute total values and total bytes
        int totalValues = 0;
        int totalBytes = 0;
        for (int pos : positions) {
            int first = getFirstValueIndex(pos);
            int count = getValueCount(pos);
            for (int v = 0; v < count; v++) {
                totalBytes += valueByteLength(first + v);
            }
            totalValues += count;
        }

        ArrowBuf newValues = allocator.buffer(Math.max(1, totalBytes));
        ArrowBuf newValueOffsets = allocator.buffer((long) (totalValues + 1) * Integer.BYTES);
        ArrowBuf newValidity = validityBuffer != null ? allocator.buffer(validityBufferLength(positions.length)) : null;
        ArrowBuf newOffsets = allocator.buffer((long) (positions.length + 1) * Integer.BYTES);
        if (newValidity != null) {
            newValidity.setZero(0, newValidity.capacity());
        }
        try {
            int byteIdx = 0;
            int valueIdx = 0;
            for (int i = 0; i < positions.length; i++) {
                int pos = positions[i];
                newOffsets.setInt((long) i * Integer.BYTES, valueIdx);
                if (isNull(pos) == false) {
                    if (newValidity != null) {
                        setValidityBit(newValidity, i);
                    }
                    int first = getFirstValueIndex(pos);
                    int count = getValueCount(pos);
                    for (int v = 0; v < count; v++) {
                        int srcStart = valueOffsetsBuffer.getInt((long) (first + v) * Integer.BYTES);
                        int length = valueByteLength(first + v);
                        newValueOffsets.setInt((long) valueIdx * Integer.BYTES, byteIdx);
                        if (length > 0) {
                            newValues.setBytes(byteIdx, valueBuffer, srcStart, length);
                            byteIdx += length;
                        }
                        valueIdx++;
                    }
                }
            }
            newOffsets.setInt((long) positions.length * Integer.BYTES, valueIdx);
            newValueOffsets.setInt((long) valueIdx * Integer.BYTES, byteIdx);
            return new BytesRefArrowBufBlock(
                newValues,
                newValueOffsets,
                newValidity,
                newOffsets,
                positions.length,
                positions.length + 1,
                blockFactory
            );
        } finally {
            releaseBuffers(newValues, newValueOffsets, newValidity, newOffsets);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public BytesRefBlock keepMask(BooleanVector mask) {
        if (getPositionCount() == 0) {
            incRef();
            return this;
        }
        if (mask.isConstant()) {
            if (mask.getBoolean(0)) {
                incRef();
                return this;
            }
            return (BytesRefBlock) blockFactory().newConstantNullBlock(getPositionCount());
        }
        var allocator = valueBuffer.getReferenceManager().getAllocator();
        ArrowBuf newValidity = allocator.buffer(validityBufferLength(valueCount));
        newValidity.setZero(0, newValidity.capacity());
        for (int i = 0; i < valueCount; i++) {
            if (mask.getBoolean(i) && isNull(i) == false) {
                setValidityBit(newValidity, i);
            }
        }
        try {
            return new BytesRefArrowBufBlock(
                valueBuffer,
                valueOffsetsBuffer,
                newValidity,
                offsetBuffer,
                valueCount,
                offsetCount,
                blockFactory
            );
        } finally {
            releaseBuffers(newValidity);
        }
    }

    @Override
    public BytesRefBlock expand() {
        if (offsetBuffer == null) {
            incRef();
            return this;
        }
        int expandedPositionCount = getFirstValueIndex(valueCount);
        if (validityBuffer == null) {
            return new BytesRefArrowBufBlock(valueBuffer, valueOffsetsBuffer, null, null, expandedPositionCount, 0, blockFactory);
        }
        var allocator = valueBuffer.getReferenceManager().getAllocator();
        ArrowBuf newValidity = allocator.buffer(validityBufferLength(expandedPositionCount));
        for (int i = 0; i < newValidity.capacity(); i++) {
            newValidity.setByte(i, (byte) 0xFF);
        }
        for (int p = 0; p < valueCount; p++) {
            if (isNull(p)) {
                int expandedPos = getFirstValueIndex(p);
                int byteIndex = expandedPos / 8;
                newValidity.setByte(byteIndex, newValidity.getByte(byteIndex) & ~(1 << (expandedPos % 8)));
            }
        }
        try {
            return new BytesRefArrowBufBlock(valueBuffer, valueOffsetsBuffer, newValidity, null, expandedPositionCount, 0, blockFactory);
        } finally {
            releaseBuffers(newValidity);
        }
    }

    @Override
    public ReleasableIterator<? extends BytesRefBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        return new BytesRefArrowBufLookup(positions, targetBlockSize);
    }

    private class BytesRefArrowBufLookup implements ReleasableIterator<BytesRefBlock> {
        private final IntBlock positions;
        private final long targetByteSize;
        private int position;

        BytesRefArrowBufLookup(IntBlock positions, ByteSizeValue targetBlockSize) {
            BytesRefArrowBufBlock.this.incRef();
            positions.incRef();
            this.positions = positions;
            this.targetByteSize = targetBlockSize.getBytes();
        }

        @Override
        public boolean hasNext() {
            return position < positions.getPositionCount();
        }

        @Override
        public BytesRefBlock next() {
            var allocator = valueBuffer.getReferenceManager().getAllocator();
            BlockFactory factory = positions.blockFactory();

            int batchStart = position;
            int totalOutputValues = 0;
            long totalOutputBytes = 0;
            int count = 0;
            int scanPos = position;
            while (scanPos < positions.getPositionCount()) {
                int pStart = positions.getFirstValueIndex(scanPos);
                int pEnd = pStart + positions.getValueCount(scanPos);
                long valuesForPos = 0;
                for (int i = pStart; i < pEnd; i++) {
                    int vp = positions.getInt(i);
                    if (vp < getPositionCount()) {
                        int first = getFirstValueIndex(vp);
                        int vCount = getValueCount(vp);
                        for (int v = 0; v < vCount; v++) {
                            totalOutputBytes += valueByteLength(first + v);
                        }
                        valuesForPos += vCount;
                    }
                }
                if (valuesForPos > MAX_LOOKUP) {
                    throw new IllegalArgumentException("Found a single entry with " + valuesForPos + " entries");
                }
                totalOutputValues += (int) valuesForPos;
                count++;
                scanPos++;
                if (count > Operator.MIN_TARGET_PAGE_SIZE && totalOutputBytes >= targetByteSize) {
                    break;
                }
            }
            int batchEnd = scanPos;
            int batchSize = batchEnd - batchStart;

            ArrowBuf newValues = allocator.buffer(Math.max(1, totalOutputBytes));
            ArrowBuf newValueOffsets = allocator.buffer((long) (totalOutputValues + 1) * Integer.BYTES);
            ArrowBuf newOffsets = allocator.buffer((long) (batchSize + 1) * Integer.BYTES);
            ArrowBuf newValidity = allocator.buffer(validityBufferLength(batchSize));
            newValidity.setZero(0, newValidity.capacity());

            int byteIdx = 0;
            int valueIdx = 0;
            for (int p = batchStart; p < batchEnd; p++) {
                int outPos = p - batchStart;
                newOffsets.setInt((long) outPos * Integer.BYTES, valueIdx);
                int pStart = positions.getFirstValueIndex(p);
                int pEnd = pStart + positions.getValueCount(p);
                int valuesForPos = 0;
                for (int i = pStart; i < pEnd; i++) {
                    int vp = positions.getInt(i);
                    if (vp >= getPositionCount()) {
                        continue;
                    }
                    int vStart = getFirstValueIndex(vp);
                    int vCount = getValueCount(vp);
                    for (int v = 0; v < vCount; v++) {
                        int srcStart = valueOffsetsBuffer.getInt((long) (vStart + v) * Integer.BYTES);
                        int length = valueByteLength(vStart + v);
                        newValueOffsets.setInt((long) valueIdx * Integer.BYTES, byteIdx);
                        if (length > 0) {
                            newValues.setBytes(byteIdx, valueBuffer, srcStart, length);
                            byteIdx += length;
                        }
                        valueIdx++;
                        valuesForPos++;
                    }
                }
                if (valuesForPos > 0) {
                    setValidityBit(newValidity, outPos);
                }
            }
            newOffsets.setInt((long) batchSize * Integer.BYTES, valueIdx);
            newValueOffsets.setInt((long) valueIdx * Integer.BYTES, byteIdx);
            position = batchEnd;

            try {
                return new BytesRefArrowBufBlock(newValues, newValueOffsets, newValidity, newOffsets, batchSize, batchSize + 1, factory);
            } finally {
                releaseBuffers(newValues, newValueOffsets, newValidity, newOffsets);
            }
        }

        @Override
        public void close() {
            Releasables.close(BytesRefArrowBufBlock.this, positions);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof BytesRefBlock that) {
            return BytesRefBlock.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return BytesRefBlock.hash(this);
    }
}
