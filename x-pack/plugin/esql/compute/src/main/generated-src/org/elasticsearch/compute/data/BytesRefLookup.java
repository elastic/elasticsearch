/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

// begin generated imports
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
// end generated imports

/**
 * Generic {@link Block#lookup} implementation {@link BytesRefBlock}s.
 * This class is generated. Edit {@code X-Lookup.java.st} instead.
 */
public final class BytesRefLookup implements ReleasableIterator<BytesRefBlock> {
    private static final int UNSET_FIXED_LENGTH = -1;
    private static final int VARIABLE_LENGTH = -2;

    private final BytesRef firstScratch = new BytesRef();
    private final BytesRef valueScratch = new BytesRef();
    private final BytesRefBlock values;
    private final IntBlock positions;
    private final long targetByteSize;
    private int position;
    private boolean closed;

    private long batchValues;
    private long batchSlots;
    private boolean batchHasMultiValues;
    private long batchValueBytes;
    private int batchFixedLength;

    private BytesRef first;
    private int valuesInPosition;

    public BytesRefLookup(BytesRefBlock values, IntBlock positions, ByteSizeValue targetBlockSize) {
        values.incRef();
        positions.incRef();
        this.values = values;
        this.positions = positions;
        this.targetByteSize = targetBlockSize.getBytes();
    }

    @Override
    public boolean hasNext() {
        return position < positions.getPositionCount();
    }

    @Override
    public BytesRefBlock next() {
        int batchStart = position;
        int batchEnd = scanBatch(batchStart);
        position = batchEnd;
        if (batchValues == 0) {
            return (BytesRefBlock) positions.blockFactory().newConstantNullBlock(batchEnd - batchStart);
        }
        return buildBatch(batchStart, batchEnd);
    }

    private int scanBatch(int batchStart) {
        batchValues = 0;
        batchSlots = 0;
        batchHasMultiValues = false;
        batchValueBytes = 0;
        batchFixedLength = UNSET_FIXED_LENGTH;
        int scanPos = batchStart;
        int count = 0;
        while (scanPos < positions.getPositionCount()) {
            if (scanPosition(scanPos, count) == false) {
                break;
            }
            count++;
            scanPos++;
            if (count > Operator.MIN_TARGET_PAGE_SIZE && batchValues > 0 && batchOutputBytes(count) >= targetByteSize) {
                break;
            }
        }
        return scanPos;
    }

    private boolean scanPosition(int pos, int count) {
        int pStart = positions.getFirstValueIndex(pos);
        int pEnd = pStart + positions.getValueCount(pos);
        int valuePositionCount = values.getPositionCount();
        long valuesForPos = 0;
        long posValueBytes = 0;
        int posFixedLength = batchFixedLength;
        for (int i = pStart; i < pEnd; i++) {
            int vp = positions.getInt(i);
            if (vp < 0) {
                throw new IllegalArgumentException("invalid position [" + vp + "]");
            }
            if (vp < valuePositionCount) {
                int vCount = values.getValueCount(vp);
                valuesForPos += vCount;
                if (valuesForPos > Block.MAX_LOOKUP) {
                    // TODO replace this with a warning and break
                    throw new IllegalArgumentException("Found a single entry with " + valuesForPos + " entries");
                }
                int vStart = values.getFirstValueIndex(vp);
                for (int v = vStart; v < vStart + vCount; v++) {
                    int len = values.getBytesRef(v, firstScratch).length;
                    posValueBytes += len;
                    posFixedLength = mergeFixedLength(posFixedLength, len);
                }
            }
        }
        long slotCount = Math.max(1L, valuesForPos);
        if (count > 0 && batchSlots + slotCount > ArrayUtil.MAX_ARRAY_LENGTH) {
            return false;
        }
        if (valuesForPos == 0) {
            posFixedLength = mergeFixedLength(posFixedLength, 0);
        }
        // Flush a long all-null prefix as a ConstantNullBlock before the first non-null match forces an oversized array.
        if (count >= Operator.MIN_TARGET_PAGE_SIZE && batchValues == 0 && valuesForPos > 0) {
            long prospectiveBytes = computeOutputBytes(
                batchSlots + slotCount,
                count + 1,
                batchHasMultiValues || valuesForPos > 1,
                batchValueBytes + posValueBytes,
                posFixedLength
            );
            if (prospectiveBytes >= targetByteSize) {
                return false;
            }
        }
        batchValues += valuesForPos;
        batchSlots += slotCount;
        if (valuesForPos > 1) {
            batchHasMultiValues = true;
        }
        batchValueBytes += posValueBytes;
        batchFixedLength = posFixedLength;
        return true;
    }

    private long batchOutputBytes(int count) {
        return computeOutputBytes(batchSlots, count, batchHasMultiValues, batchValueBytes, batchFixedLength);
    }

    private static long computeOutputBytes(long slots, int count, boolean hasMultiValues, long valueBytes, int fixedLength) {
        long outputBytes = BytesRefArrayBlock.BASE_RAM_BYTES_USED + valueBytes;
        if (fixedLength == VARIABLE_LENGTH) {
            outputBytes += (slots + 1L) * Integer.BYTES;
        }
        if (hasMultiValues) {
            outputBytes += RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (count + 1L) * Integer.BYTES;
        }
        return outputBytes;
    }

    private BytesRefBlock buildBatch(int batchStart, int batchEnd) {
        int exactSlots = Math.toIntExact(batchSlots);
        try (BytesRefBlock.Builder builder = positions.blockFactory().newBytesRefBlockBuilder(exactSlots, Math.max(1L, batchValueBytes))) {
            for (int p = batchStart; p < batchEnd; p++) {
                int start = positions.getFirstValueIndex(p);
                int end = start + positions.getValueCount(p);
                valuesInPosition = 0;
                for (int i = start; i < end; i++) {
                    copy(builder, positions.getInt(i));
                }
                switch (valuesInPosition) {
                    case 0 -> builder.appendNull();
                    case 1 -> builder.appendBytesRef(first);
                    default -> builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }

    private static int mergeFixedLength(int current, int length) {
        if (current == UNSET_FIXED_LENGTH || current == length) {
            return length;
        }
        return VARIABLE_LENGTH;
    }

    private void copy(BytesRefBlock.Builder builder, int valuePosition) {
        if (valuePosition >= values.getPositionCount()) {
            return;
        }
        int start = values.getFirstValueIndex(valuePosition);
        int end = start + values.getValueCount(valuePosition);
        for (int i = start; i < end; i++) {
            if (valuesInPosition == 0) {
                first = values.getBytesRef(i, firstScratch);
                valuesInPosition++;
                continue;
            }
            if (valuesInPosition == 1) {
                builder.beginPositionEntry();
                builder.appendBytesRef(first);
            }
            builder.appendBytesRef(values.getBytesRef(i, valueScratch));
            valuesInPosition++;
        }
    }

    @Override
    public void close() {
        if (closed == false) {
            closed = true;
            Releasables.close(values, positions);
        }
    }
}
