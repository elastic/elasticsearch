/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BitVector;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.ToMask;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

/**
 * Arrow buffer backed {@link BooleanBlock}. Booleans are bit-packed in Arrow (1 bit per value),
 * so this class overrides {@link #filter} and {@link #lookup} with bit-level implementations.
 * The inherited {@link #keepMask} and {@link #expand} work unchanged since they only manipulate
 * validity and offset buffers.
 */
public final class BooleanArrowBufBlock extends AbstractArrowBufBlock<BooleanVector, BooleanBlock> implements BooleanBlock {

    public BooleanArrowBufBlock(
        ArrowBuf valueBuffer,
        @Nullable ArrowBuf validityBuffer,
        @Nullable ArrowBuf offsetBuffer,
        int valueCount,
        int offsetCount,
        BlockFactory blockFactory
    ) {
        super(valueBuffer, validityBuffer, offsetBuffer, valueCount, offsetCount, blockFactory);
    }

    public static BooleanArrowBufBlock of(BitVector bitVector, BlockFactory blockFactory) {
        var result = new BooleanArrowBufBlock(
            bitVector.getDataBuffer(),
            bitVector.getNullCount() == 0 ? null : bitVector.getValidityBuffer(),
            null,
            bitVector.getValueCount(),
            0,
            blockFactory
        );

        ArrowUtils.retainBuffers(result.valueBuffer, result.validityBuffer);
        return result;
    }

    @Override
    protected int byteSize() {
        throw new UnsupportedOperationException("booleans are bit-packed; byteSize() is not applicable");
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
    public boolean getBoolean(int valueIndex) {
        return (valueBuffer.getByte(valueIndex / 8) & (1 << (valueIndex % 8))) != 0;
    }

    @Override
    public ElementType elementType() {
        return ElementType.BOOLEAN;
    }

    @Override
    public int valueMaxByteSize() {
        return Byte.BYTES;
    }

    @Override
    public ToMask toMask() {
        if (getPositionCount() == 0) {
            return new ToMask(blockFactory().newConstantBooleanVector(false, 0), false);
        }
        try (BooleanVector.FixedBuilder builder = blockFactory().newBooleanVectorFixedBuilder(getPositionCount())) {
            boolean hasMv = false;
            for (int p = 0; p < getPositionCount(); p++) {
                builder.appendBoolean(switch (getValueCount(p)) {
                    case 0 -> false;
                    case 1 -> getBoolean(getFirstValueIndex(p));
                    default -> {
                        hasMv = true;
                        yield false;
                    }
                });
            }
            return new ToMask(builder.build(), hasMv);
        }
    }

    private static int bitBufferLength(int bits) {
        return ((bits + 63) / 64) * Long.BYTES;
    }

    private static void setBit(ArrowBuf buf, int position) {
        int byteIndex = position / 8;
        buf.setByte(byteIndex, buf.getByte(byteIndex) | (1 << (position % 8)));
    }

    private static void copyBit(ArrowBuf src, int srcPos, ArrowBuf dst, int dstPos) {
        if ((src.getByte(srcPos / 8) & (1 << (srcPos % 8))) != 0) {
            setBit(dst, dstPos);
        }
    }

    @Override
    public BooleanBlock filter(boolean mayContainDuplicates, int... positions) {
        var allocator = blockFactory.arrowAllocator();

        if (offsetBuffer == null) {
            ArrowBuf newValues = null, newValidity = null;
            boolean success = false;
            try {
                newValues = allocator.buffer(bitBufferLength(positions.length));
                newValues.setZero(0, newValues.capacity());
                if (validityBuffer != null) {
                    newValidity = allocator.buffer(bitBufferLength(positions.length));
                    newValidity.setZero(0, newValidity.capacity());
                }
                success = true;
            } finally {
                if (success == false) {
                    ArrowUtils.releaseBuffers(newValues, newValidity);
                }
            }

            for (int i = 0; i < positions.length; i++) {
                int pos = positions[i];
                copyBit(valueBuffer, pos, newValues, i);
                if (newValidity != null && isNull(pos) == false) {
                    setBit(newValidity, i);
                }
            }
            return new BooleanArrowBufBlock(newValues, newValidity, null, positions.length, 0, blockFactory);
        }

        int totalValues = 0;
        for (int pos : positions) {
            totalValues += getValueCount(pos);
        }

        ArrowBuf newValues = null, newValidity = null, newOffsets = null;
        boolean success = false;
        try {
            newValues = allocator.buffer(bitBufferLength(totalValues));
            newValues.setZero(0, newValues.capacity());
            if (validityBuffer != null) {
                newValidity = allocator.buffer(bitBufferLength(positions.length));
                newValidity.setZero(0, newValidity.capacity());
            }
            newOffsets = allocator.buffer((long) (positions.length + 1) * Integer.BYTES);
            success = true;
        } finally {
            if (success == false) {
                ArrowUtils.releaseBuffers(newValues, newValidity, newOffsets);
            }
        }

        int valueIdx = 0;
        for (int i = 0; i < positions.length; i++) {
            int pos = positions[i];
            newOffsets.setInt((long) i * Integer.BYTES, valueIdx);
            if (isNull(pos) == false) {
                if (newValidity != null) {
                    setBit(newValidity, i);
                }
                int first = getFirstValueIndex(pos);
                int count = getValueCount(pos);
                for (int j = 0; j < count; j++) {
                    copyBit(valueBuffer, first + j, newValues, valueIdx + j);
                }
                valueIdx += count;
            }
        }
        newOffsets.setInt((long) positions.length * Integer.BYTES, valueIdx);
        return new BooleanArrowBufBlock(newValues, newValidity, newOffsets, positions.length, positions.length + 1, blockFactory);
    }

    @Override
    public ReleasableIterator<? extends BooleanBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        return new BooleanArrowBufLookup(positions, targetBlockSize);
    }

    private class BooleanArrowBufLookup implements ReleasableIterator<BooleanBlock> {
        private final IntBlock positions;
        private final long targetByteSize;
        private int position;

        BooleanArrowBufLookup(IntBlock positions, ByteSizeValue targetBlockSize) {
            BooleanArrowBufBlock.this.incRef();
            positions.incRef();
            this.positions = positions;
            this.targetByteSize = targetBlockSize.getBytes();
        }

        @Override
        public boolean hasNext() {
            return position < positions.getPositionCount();
        }

        @Override
        public BooleanBlock next() {
            var allocator = blockFactory.arrowAllocator();
            BlockFactory factory = positions.blockFactory();

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
                if (count > Operator.MIN_TARGET_PAGE_SIZE && (totalOutputValues + 7) / 8 >= targetByteSize) {
                    break;
                }
            }
            int batchEnd = scanPos;
            int batchSize = batchEnd - batchStart;

            ArrowBuf newValues = null, newOffsets = null, newValidity = null;
            boolean success = false;
            try {
                newValues = allocator.buffer(bitBufferLength(totalOutputValues));
                newValues.setZero(0, newValues.capacity());
                newOffsets = allocator.buffer((long) (batchSize + 1) * Integer.BYTES);
                newValidity = allocator.buffer(bitBufferLength(batchSize));
                newValidity.setZero(0, newValidity.capacity());
                success = true;
            } finally {
                if (success == false) {
                    ArrowUtils.releaseBuffers(newValues, newOffsets, newValidity);
                }
            }

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
                    for (int j = 0; j < vCount; j++) {
                        copyBit(valueBuffer, vStart + j, newValues, valueIdx + j);
                    }
                    valueIdx += vCount;
                    valuesForPos += vCount;
                }
                if (valuesForPos > 0) {
                    setBit(newValidity, outPos);
                }
            }
            newOffsets.setInt((long) batchSize * Integer.BYTES, valueIdx);
            position = batchEnd;

            return new BooleanArrowBufBlock(newValues, newValidity, newOffsets, batchSize, batchSize + 1, factory);
        }

        @Override
        public void close() {
            Releasables.close(BooleanArrowBufBlock.this, positions);
        }
    }
}
