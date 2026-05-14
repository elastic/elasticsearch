/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.PagedBytesCursor;
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
        this.valueOffsetsBuffer = valueOffsetsBuffer;
    }

    public static BytesRefBlock of(ValueVector arrowVector, BlockFactory blockFactory) {
        BytesRefBlock constant = tryConstant(arrowVector, blockFactory);
        if (constant != null) {
            return constant;
        }

        BytesRefArrowBufBlock result;

        // BaseVariableWidthVector is the parent of VarBinaryVector and VarCharVector
        if (arrowVector instanceof BaseVariableWidthVector base) {
            boolean hasNulls = base.getNullCount() > 0;
            result = new BytesRefArrowBufBlock(
                base.getDataBuffer(),
                base.getOffsetBuffer(),
                hasNulls ? base.getValidityBuffer() : null,
                null,
                base.getValueCount(),
                0,
                blockFactory
            );

        } else if (arrowVector instanceof ListVector listVec) {
            if (listVec.getDataVector() instanceof BaseVariableWidthVector base) {
                if (base.getNullCount() > 0) {
                    throw new IllegalArgumentException("Nulls multi-valued entries aren't supported.");
                }
                boolean hasNulls = listVec.getNullCount() > 0;
                result = new BytesRefArrowBufBlock(
                    base.getDataBuffer(),
                    base.getOffsetBuffer(),
                    hasNulls ? listVec.getValidityBuffer() : null,
                    listVec.getOffsetBuffer(),
                    listVec.getValueCount(),
                    listVec.getValueCount() + 1,
                    blockFactory
                );
            } else {
                throw new IllegalArgumentException("Unsupported vector type: " + arrowVector.getField().getType());
            }
        } else {
            throw new IllegalArgumentException("Unsupported vector type: " + arrowVector.getField().getType());
        }

        result.retainBuffers();
        return result;
    }

    /**
     * Returns a constant block when the variable-width vector is fully present and all values
     * are byte-identical, a constant-null block when all values are null, or {@code null} when
     * the caller should fall through to the zero-copy {@link BytesRefArrowBufBlock} path.
     * <p>
     * Multi-valued ({@link ListVector}) inputs return {@code null}; the value-level offsets
     * inside a list make uniformity comparison position-by-position more involved and the
     * benefit is rare.
     * <p>
     * Two-step check: first the per-row lengths via the offset buffer (cheap, single-int
     * compare per row), then the bytes themselves once a uniform length is established.
     */
    private static BytesRefBlock tryConstant(ValueVector arrowVector, BlockFactory blockFactory) {
        if (arrowVector instanceof BaseVariableWidthVector base) {
            int rowCount = base.getValueCount();
            if (rowCount == 0) {
                return null;
            }
            if (base.getNullCount() == rowCount) {
                return (BytesRefBlock) blockFactory.newConstantNullBlock(rowCount);
            }
            if (base.getNullCount() != 0) {
                return null;
            }
            ArrowBuf offsets = base.getOffsetBuffer();
            int firstStart = offsets.getInt(0);
            int firstEnd = offsets.getInt(Integer.BYTES);
            int firstLen = firstEnd - firstStart;
            for (int i = 1; i < rowCount; i++) {
                int s = offsets.getInt((long) i * Integer.BYTES);
                int e = offsets.getInt((long) (i + 1) * Integer.BYTES);
                if (e - s != firstLen) {
                    return null;
                }
            }
            ArrowBuf data = base.getDataBuffer();
            for (int i = 1; i < rowCount; i++) {
                int s = offsets.getInt((long) i * Integer.BYTES);
                for (int j = 0; j < firstLen; j++) {
                    if (data.getByte(s + j) != data.getByte(firstStart + j)) {
                        return null;
                    }
                }
            }
            byte[] bytes = new byte[firstLen];
            for (int j = 0; j < firstLen; j++) {
                bytes[j] = data.getByte(firstStart + j);
            }
            return blockFactory.newConstantBytesRefBlockWith(new BytesRef(bytes), rowCount);
        }
        return null;
    }

    @Override
    public void retainBuffers() {
        super.retainBuffers();
        valueOffsetsBuffer.getReferenceManager().retain();
    }

    @Override
    public void releaseBuffers() {
        super.releaseBuffers();
        valueOffsetsBuffer.getReferenceManager().release();
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
    public PagedBytesCursor get(int valueIndex, PagedBytesCursor scratch) {
        int start = valueOffsetsBuffer.getInt((long) valueIndex * Integer.BYTES);
        int end = valueOffsetsBuffer.getInt((long) (valueIndex + 1) * Integer.BYTES);
        int length = end - start;
        byte[] buf = new byte[length];
        valueBuffer.getBytes(start, buf, 0, length);
        scratch.init(buf, 0, length);
        return scratch;
    }

    @Override
    public ElementType elementType() {
        return ElementType.BYTES_REF;
    }

    @Override
    public int valueMaxByteSize() {
        int max = 0;
        int prev = valueOffsetsBuffer.getInt(0);
        for (int i = 1; i <= getTotalValueCount(); i++) {
            int curr = valueOffsetsBuffer.getInt((long) i * Integer.BYTES);
            max = Math.max(max, curr - prev);
            prev = curr;
        }
        return max;
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
        var allocator = blockFactory.arrowAllocator();

        if (offsetBuffer == null) {
            int totalBytes = 0;
            for (int pos : positions) {
                if (isNull(pos) == false) {
                    totalBytes += valueByteLength(pos);
                }
            }

            ArrowBuf newValues = null, newValueOffsets = null, newValidity = null;
            boolean success = false;
            try {
                newValues = allocator.buffer(totalBytes);
                newValueOffsets = allocator.buffer((long) (positions.length + 1) * Integer.BYTES);
                if (validityBuffer != null) {
                    newValidity = allocator.buffer(validityBufferLength(positions.length));
                    newValidity.setZero(0, newValidity.capacity());
                }
                success = true;
            } finally {
                if (success == false) {
                    ArrowUtils.releaseBuffers(newValues, newValueOffsets, newValidity);
                }
            }

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

        ArrowBuf newValues = null, newValueOffsets = null, newValidity = null, newOffsets = null;
        boolean success = false;
        try {
            newValues = allocator.buffer(Math.max(1, totalBytes));
            newValueOffsets = allocator.buffer((long) (totalValues + 1) * Integer.BYTES);
            if (validityBuffer != null) {
                newValidity = allocator.buffer(validityBufferLength(positions.length));
                newValidity.setZero(0, newValidity.capacity());
            }
            newOffsets = allocator.buffer((long) (positions.length + 1) * Integer.BYTES);
            success = true;
        } finally {
            if (success == false) {
                ArrowUtils.releaseBuffers(newValues, newValueOffsets, newValidity, newOffsets);
            }
        }

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
        var allocator = blockFactory.arrowAllocator();
        ArrowBuf newValidity = allocator.buffer(validityBufferLength(valueCount));
        newValidity.setZero(0, newValidity.capacity());
        for (int i = 0; i < valueCount; i++) {
            if (mask.getBoolean(i) && isNull(i) == false) {
                setValidityBit(newValidity, i);
            }
        }

        ArrowUtils.retainBuffers(valueBuffer, valueOffsetsBuffer, offsetBuffer);
        return new BytesRefArrowBufBlock(valueBuffer, valueOffsetsBuffer, newValidity, offsetBuffer, valueCount, offsetCount, blockFactory);
    }

    @Override
    public BytesRefBlock expand() {
        if (offsetBuffer == null) {
            incRef();
            return this;
        }
        int expandedPositionCount = getFirstValueIndex(valueCount);
        if (validityBuffer == null) {
            ArrowUtils.retainBuffers(valueBuffer, valueOffsetsBuffer);
            return new BytesRefArrowBufBlock(valueBuffer, valueOffsetsBuffer, null, null, expandedPositionCount, 0, blockFactory);
        }
        var allocator = blockFactory.arrowAllocator();
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
        ArrowUtils.retainBuffers(valueBuffer, valueOffsetsBuffer);
        return new BytesRefArrowBufBlock(valueBuffer, valueOffsetsBuffer, newValidity, null, expandedPositionCount, 0, blockFactory);
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
            BlockFactory factory = positions.blockFactory();
            var allocator = blockFactory.arrowAllocator();

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

            ArrowBuf newValues = null, newValueOffsets = null, newOffsets = null, newValidity = null;
            boolean success = false;
            try {
                newValues = allocator.buffer(Math.max(1, totalOutputBytes));
                newValueOffsets = allocator.buffer((long) (totalOutputValues + 1) * Integer.BYTES);
                newOffsets = allocator.buffer((long) (batchSize + 1) * Integer.BYTES);
                newValidity = allocator.buffer(validityBufferLength(batchSize));
                newValidity.setZero(0, newValidity.capacity());
                success = true;
            } finally {
                if (success == false) {
                    ArrowUtils.releaseBuffers(newValues, newValueOffsets, newOffsets, newValidity);
                }
            }

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

            return new BytesRefArrowBufBlock(newValues, newValueOffsets, newValidity, newOffsets, batchSize, batchSize + 1, factory);
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
