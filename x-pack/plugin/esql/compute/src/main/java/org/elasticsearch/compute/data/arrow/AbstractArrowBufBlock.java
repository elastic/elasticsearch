/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.AbstractNonThreadSafeRefCounted;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

public abstract class AbstractArrowBufBlock<V extends Vector, B extends Block> extends AbstractNonThreadSafeRefCounted implements Block {

    protected final int valueCount;
    protected final int offsetCount;
    protected final ArrowBuf valueBuffer;
    @Nullable
    protected final ArrowBuf validityBuffer;

    // Offsets are 32 bit values. Arrow IPC allows 32 and 64 bit offsets but arrow-java's ListVector uses 32 bits.
    // TODO: see how arrow-java behaves if presented with a List with 64 bit offsets.
    @Nullable
    protected final ArrowBuf offsetBuffer;
    protected final BlockFactory blockFactory;
    protected boolean closed = false;

    /**
     *  Create an ArrowBuf block based on the constituents of an Arrow ValueVector. It does not take ownership of buffers but rather
     *  increases their reference count. This means that callers must release the buffers (and decrease their reference counters)
     *  if they don't need them anymore.
     */
    public AbstractArrowBufBlock(
        ArrowBuf valueBuffer,
        @Nullable ArrowBuf validityBuffer,
        @Nullable ArrowBuf offsetBuffer,
        int valueCount,
        int offsetCount,
        BlockFactory blockFactory
    ) {
        valueBuffer.getReferenceManager().retain();
        if (validityBuffer != null) {
            validityBuffer.getReferenceManager().retain();
        }
        if (offsetBuffer != null) {
            offsetBuffer.getReferenceManager().retain();
        }

        this.valueBuffer = valueBuffer;
        this.validityBuffer = validityBuffer;
        this.offsetBuffer = offsetBuffer;
        this.valueCount = valueCount;
        this.offsetCount = offsetCount;
        this.blockFactory = blockFactory;
    }

    protected abstract int byteSize();

    protected abstract ArrowBufVectorConstructor<V> vectorConstructor();

    protected abstract ArrowBufBlockConstructor<B> blockConstructor();

    protected static int validityBufferLength(int positions) {
        return ((positions + 63) / 64) * Long.BYTES;
    }

    protected static void setValidityBit(ArrowBuf buf, int position) {
        int byteIndex = position / 8;
        buf.setByte(byteIndex, buf.getByte(byteIndex) | (1 << (position % 8)));
    }

    protected static void releaseBuffers(ArrowBuf... buffers) {
        for (ArrowBuf buf : buffers) {
            if (buf != null) {
                buf.getReferenceManager().release();
            }
        }
    }

    private int nullCount() {
        if (validityBuffer == null) {
            return 0;
        }
        int validCount = 0;
        int fullLongs = valueCount / 64;
        for (int i = 0; i < fullLongs; i++) {
            validCount += Long.bitCount(validityBuffer.getLong((long) i * Long.BYTES));
        }
        int remaining = valueCount % 64;
        if (remaining > 0) {
            long lastLong = validityBuffer.getLong((long) fullLongs * Long.BYTES);
            validCount += Long.bitCount(lastLong & ((1L << remaining) - 1));
        }
        return valueCount - validCount;
    }

    @Override
    protected void closeInternal() {
        if (closed) {
            return;
        }
        closed = true;
        this.valueBuffer.getReferenceManager().release();
        if (this.validityBuffer != null) {
            this.validityBuffer.getReferenceManager().release();
        }
        if (this.offsetBuffer != null) {
            this.offsetBuffer.getReferenceManager().release();
        }
    }

    @Override
    public V asVector() {
        if (validityBuffer == null && offsetBuffer == null) {
            return vectorConstructor().create(valueBuffer, valueCount, blockFactory);
        } else {
            return null;
        }
    }

    @Override
    public BlockFactory blockFactory() {
        return blockFactory;
    }

    @Override
    public void allowPassingToDifferentDriver() {
        // FIXME: Does this apply to Arrow buffers? Their allocator references the circuit breaker.
    }

    @Override
    public boolean isNull(int position) {
        if (validityBuffer == null) {
            return false;
        }
        // https://arrow.apache.org/docs/format/Columnar.html#validity-bitmaps
        return (validityBuffer.getByte(position / 8) & (1 << (position % 8))) == 0;
    }

    @Override
    public boolean mayHaveNulls() {
        return this.validityBuffer != null;
    }

    @Override
    public boolean areAllValuesNull() {
        if (validityBuffer == null) {
            return false;
        }

        // Buffers are padded to 8 bytes, and the validity bits in the padding are unset
        // https://arrow.apache.org/docs/format/Columnar.html#validity-bitmaps
        for (int i = 0; i < (valueCount + 63) / 64; i++) {
            if (validityBuffer.getLong(i * Long.BYTES) != 0) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean mayHaveMultivaluedFields() {
        return this.offsetBuffer != null;
    }

    @Override
    public boolean doesHaveMultivaluedFields() {
        return this.offsetBuffer != null;
    }

    @Override
    public MvOrdering mvOrdering() {
        return MvOrdering.UNORDERED;
    }

    @Override
    public long ramBytesUsed() {
        long sum = valueBuffer.getActualMemoryConsumed();
        if (this.validityBuffer != null) {
            sum += validityBuffer.getActualMemoryConsumed();
        }
        if (this.offsetBuffer != null) {
            sum += offsetBuffer.getActualMemoryConsumed();
        }
        return sum;
    }

    @Override
    public B filter(boolean mayContainDuplicates, int... positions) {
        var allocator = valueBuffer.getReferenceManager().getAllocator();
        int size = byteSize();

        if (offsetBuffer == null) {
            ArrowBuf newValues = allocator.buffer(Math.max(1, (long) positions.length * size));
            ArrowBuf newValidity = validityBuffer != null ? allocator.buffer(validityBufferLength(positions.length)) : null;
            if (newValidity != null) {
                newValidity.setZero(0, newValidity.capacity());
            }
            try {
                for (int i = 0; i < positions.length; i++) {
                    int pos = positions[i];
                    newValues.setBytes((long) i * size, valueBuffer, (long) pos * size, size);
                    if (newValidity != null && isNull(pos) == false) {
                        setValidityBit(newValidity, i);
                    }
                }
                return blockConstructor().create(newValues, newValidity, null, positions.length, 0, blockFactory);
            } finally {
                releaseBuffers(newValues, newValidity);
            }
        }

        int totalValues = 0;
        for (int pos : positions) {
            totalValues += getValueCount(pos);
        }

        ArrowBuf newValues = allocator.buffer(Math.max(1, (long) totalValues * size));
        ArrowBuf newValidity = validityBuffer != null ? allocator.buffer(validityBufferLength(positions.length)) : null;
        ArrowBuf newOffsets = allocator.buffer((long) (positions.length + 1) * Integer.BYTES);
        if (newValidity != null) {
            newValidity.setZero(0, newValidity.capacity());
        }
        try {
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
                    if (count > 0) {
                        newValues.setBytes((long) valueIdx * size, valueBuffer, (long) first * size, (long) count * size);
                        valueIdx += count;
                    }
                }
            }
            newOffsets.setInt((long) positions.length * Integer.BYTES, valueIdx);
            return blockConstructor().create(newValues, newValidity, newOffsets, positions.length, positions.length + 1, blockFactory);
        } finally {
            releaseBuffers(newValues, newValidity, newOffsets);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public B keepMask(BooleanVector mask) {
        if (getPositionCount() == 0) {
            incRef();
            return (B) this;
        }
        if (mask.isConstant()) {
            if (mask.getBoolean(0)) {
                incRef();
                return (B) this;
            }
            return (B) blockFactory().newConstantNullBlock(getPositionCount());
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
            return blockConstructor().create(valueBuffer, newValidity, offsetBuffer, valueCount, offsetCount, blockFactory);
        } finally {
            releaseBuffers(newValidity);
        }
    }

    @Override
    public ReleasableIterator<? extends B> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        return new ArrowBufLookup(positions, targetBlockSize);
    }

    private class ArrowBufLookup implements ReleasableIterator<B> {
        private final IntBlock positions;
        private final long targetByteSize;
        private int position;

        ArrowBufLookup(IntBlock positions, ByteSizeValue targetBlockSize) {
            AbstractArrowBufBlock.this.incRef();
            positions.incRef();
            this.positions = positions;
            this.targetByteSize = targetBlockSize.getBytes();
        }

        @Override
        public boolean hasNext() {
            return position < positions.getPositionCount();
        }

        @Override
        public B next() {
            int size = byteSize();
            var allocator = valueBuffer.getReferenceManager().getAllocator();
            BlockFactory factory = positions.blockFactory();

            // Phase 1: scan the batch to compute total output values
            int batchStart = position;
            int totalOutputValues = 0;
            int count = 0;
            int scanPos = position;
            while (scanPos < positions.getPositionCount()) {
                int pStart = positions.getFirstValueIndex(scanPos);
                int pEnd = pStart + positions.getValueCount(scanPos);
                long valuesForPos = 0;
                for (int i = pStart; i < pEnd; i++) {
                    int vp = positions.getInt(i);
                    if (vp < getPositionCount()) {
                        valuesForPos += getValueCount(vp);
                    }
                }
                if (valuesForPos > MAX_LOOKUP) {
                    throw new IllegalArgumentException("Found a single entry with " + valuesForPos + " entries");
                }
                totalOutputValues += (int) valuesForPos;
                count++;
                scanPos++;
                if (count > Operator.MIN_TARGET_PAGE_SIZE && (long) totalOutputValues * size >= targetByteSize) {
                    break;
                }
            }
            int batchEnd = scanPos;
            int batchSize = batchEnd - batchStart;

            // Phase 2: allocate buffers and populate
            ArrowBuf newValues = allocator.buffer(Math.max(1, (long) totalOutputValues * size));
            ArrowBuf newOffsets = allocator.buffer((long) (batchSize + 1) * Integer.BYTES);
            ArrowBuf newValidity = allocator.buffer(validityBufferLength(batchSize));
            newValidity.setZero(0, newValidity.capacity());

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
                    if (vCount > 0) {
                        newValues.setBytes((long) valueIdx * size, valueBuffer, (long) vStart * size, (long) vCount * size);
                        valueIdx += vCount;
                        valuesForPos += vCount;
                    }
                }
                if (valuesForPos > 0) {
                    setValidityBit(newValidity, outPos);
                }
            }
            newOffsets.setInt((long) batchSize * Integer.BYTES, valueIdx);
            position = batchEnd;

            try {
                return blockConstructor().create(newValues, newValidity, newOffsets, batchSize, batchSize + 1, factory);
            } finally {
                releaseBuffers(newValues, newValidity, newOffsets);
            }
        }

        @Override
        public void close() {
            Releasables.close(AbstractArrowBufBlock.this, positions);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public B expand() {
        if (offsetBuffer == null) {
            incRef();
            return (B) this;
        }
        int expandedPositionCount = getFirstValueIndex(valueCount);
        if (validityBuffer == null) {
            return blockConstructor().create(valueBuffer, null, null, expandedPositionCount, 0, blockFactory);
        }
        var allocator = valueBuffer.getReferenceManager().getAllocator();
        ArrowBuf newValidity = allocator.buffer(validityBufferLength(expandedPositionCount));
        // All expanded positions are valid by default
        for (int i = 0; i < newValidity.capacity(); i++) {
            newValidity.setByte(i, (byte) 0xFF);
        }
        // Clear validity bits at expanded positions corresponding to original null positions
        for (int p = 0; p < valueCount; p++) {
            if (isNull(p)) {
                int expandedPos = getFirstValueIndex(p);
                int byteIndex = expandedPos / 8;
                newValidity.setByte(byteIndex, newValidity.getByte(byteIndex) & ~(1 << (expandedPos % 8)));
            }
        }
        try {
            return blockConstructor().create(valueBuffer, newValidity, null, expandedPositionCount, 0, blockFactory);
        } finally {
            releaseBuffers(newValidity);
        }
    }

    @Override
    public int getPositionCount() {
        return valueCount;
    }

    @Override
    public int getFirstValueIndex(int position) {
        if (offsetBuffer == null) {
            return position;
        } else {
            return offsetBuffer.getInt((long) position * Integer.BYTES);
        }
    }

    @Override
    public int getValueCount(int position) {
        if (isNull(position)) {
            return 0;
        }
        if (offsetBuffer == null) {
            return 1;
        }
        return getFirstValueIndex(position + 1) - getFirstValueIndex(position);
    }

    @Override
    public int getTotalValueCount() {
        if (offsetBuffer == null) {
            return valueCount - nullCount();
        }
        int total = 0;
        for (int p = 0; p < valueCount; p++) {
            total += getValueCount(p);
        }
        return total;
    }
}
