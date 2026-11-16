/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

// begin generated imports
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
// end generated imports

/**
 * Generic {@link Block#lookup} implementation {@link LongBlock}s.
 * This class is generated. Edit {@code X-Lookup.java.st} instead.
 */
public final class LongLookup implements ReleasableIterator<LongBlock> {
    private final LongBlock values;
    private final IntBlock positions;
    private final long targetByteSize;
    private int position;
    private boolean closed;

    private long batchValues;
    private long batchSlots;
    private boolean batchHasMultiValues;
    private long first;
    private int valuesInPosition;

    public LongLookup(LongBlock values, IntBlock positions, ByteSizeValue targetBlockSize) {
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
    public LongBlock next() {
        int batchStart = position;
        int batchEnd = scanBatch(batchStart);
        position = batchEnd;
        if (batchValues == 0) {
            return (LongBlock) positions.blockFactory().newConstantNullBlock(batchEnd - batchStart);
        }
        return buildBatch(batchStart, batchEnd);
    }

    private int scanBatch(int batchStart) {
        batchValues = 0;
        batchSlots = 0;
        batchHasMultiValues = false;
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
            }
        }
        long slotCount = Math.max(1L, valuesForPos);
        if (count > 0 && batchSlots + slotCount > ArrayUtil.MAX_ARRAY_LENGTH) {
            return false;
        }
        // Flush a long all-null prefix as a ConstantNullBlock before the first non-null match forces an oversized array.
        if (count >= Operator.MIN_TARGET_PAGE_SIZE && batchValues == 0 && valuesForPos > 0) {
            long prospectiveBytes = computeOutputBytes(batchSlots + slotCount, count + 1, batchHasMultiValues || valuesForPos > 1);
            if (prospectiveBytes >= targetByteSize) {
                return false;
            }
        }
        batchValues += valuesForPos;
        batchSlots += slotCount;
        if (valuesForPos > 1) {
            batchHasMultiValues = true;
        }
        return true;
    }

    private long batchOutputBytes(int count) {
        return computeOutputBytes(batchSlots, count, batchHasMultiValues);
    }

    private static long computeOutputBytes(long slots, int count, boolean hasMultiValues) {
        long outputBytes = RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + slots * Long.BYTES;
        if (hasMultiValues) {
            outputBytes += RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (count + 1L) * Integer.BYTES;
        }
        return outputBytes;
    }

    private LongBlock buildBatch(int batchStart, int batchEnd) {
        int exactSlots = Math.toIntExact(batchSlots);
        try (LongBlock.Builder builder = positions.blockFactory().newLongBlockBuilder(exactSlots)) {
            for (int p = batchStart; p < batchEnd; p++) {
                int start = positions.getFirstValueIndex(p);
                int end = start + positions.getValueCount(p);
                valuesInPosition = 0;
                for (int i = start; i < end; i++) {
                    copy(builder, positions.getInt(i));
                }
                switch (valuesInPosition) {
                    case 0 -> builder.appendNull();
                    case 1 -> builder.appendLong(first);
                    default -> builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }

    private void copy(LongBlock.Builder builder, int valuePosition) {
        if (valuePosition >= values.getPositionCount()) {
            return;
        }
        int start = values.getFirstValueIndex(valuePosition);
        int end = start + values.getValueCount(valuePosition);
        for (int i = start; i < end; i++) {
            if (valuesInPosition == 0) {
                first = values.getLong(i);
                valuesInPosition++;
                continue;
            }
            if (valuesInPosition == 1) {
                builder.beginPositionEntry();
                builder.appendLong(first);
            }
            builder.appendLong(values.getLong(i));
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
