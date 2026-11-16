/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

// begin generated imports
import org.apache.lucene.util.ArrayUtil;
$if(BytesRef)$
import org.apache.lucene.util.BytesRef;
$endif$
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
// end generated imports

/**
 * Generic {@link Block#lookup} implementation {@link $Type$Block}s.
 * This class is generated. Edit {@code X-Lookup.java.st} instead.
 */
public final class $Type$Lookup implements ReleasableIterator<$Type$Block> {
$if(BytesRef)$
    private static final int UNSET_FIXED_LENGTH = -1;
    private static final int VARIABLE_LENGTH = -2;

    private final BytesRef firstScratch = new BytesRef();
    private final BytesRef valueScratch = new BytesRef();
$endif$
    private final $Type$Block values;
    private final IntBlock positions;
    private final long targetByteSize;
    private int position;
    private boolean closed;

    private long batchValues;
    private long batchSlots;
    private boolean batchHasMultiValues;
$if(BytesRef)$
    private long batchValueBytes;
    private int batchFixedLength;
$endif$

    private $type$ first;
    private int valuesInPosition;

    public $Type$Lookup($Type$Block values, IntBlock positions, ByteSizeValue targetBlockSize) {
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
    public $Type$Block next() {
        int batchStart = position;
        int batchEnd = scanBatch(batchStart);
        position = batchEnd;
        if (batchValues == 0) {
            return ($Type$Block) positions.blockFactory().newConstantNullBlock(batchEnd - batchStart);
        }
        return buildBatch(batchStart, batchEnd);
    }

    private int scanBatch(int batchStart) {
        batchValues = 0;
        batchSlots = 0;
        batchHasMultiValues = false;
$if(BytesRef)$
        batchValueBytes = 0;
        batchFixedLength = UNSET_FIXED_LENGTH;
$endif$
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
$if(BytesRef)$
        long posValueBytes = 0;
        int posFixedLength = batchFixedLength;
$endif$
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
$if(BytesRef)$
                int vStart = values.getFirstValueIndex(vp);
                for (int v = vStart; v < vStart + vCount; v++) {
                    int len = values.getBytesRef(v, firstScratch).length;
                    posValueBytes += len;
                    posFixedLength = mergeFixedLength(posFixedLength, len);
                }
$endif$
            }
        }
        long slotCount = Math.max(1L, valuesForPos);
        if (count > 0 && batchSlots + slotCount > ArrayUtil.MAX_ARRAY_LENGTH) {
            return false;
        }
$if(BytesRef)$
        if (valuesForPos == 0) {
            posFixedLength = mergeFixedLength(posFixedLength, 0);
        }
$endif$
        // Flush a long all-null prefix as a ConstantNullBlock before the first non-null match forces an oversized array.
        if (count >= Operator.MIN_TARGET_PAGE_SIZE && batchValues == 0 && valuesForPos > 0) {
            long prospectiveBytes = computeOutputBytes(
                batchSlots + slotCount,
                count + 1,
                batchHasMultiValues || valuesForPos > 1$if(BytesRef)$,
                batchValueBytes + posValueBytes,
                posFixedLength$endif$
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
$if(BytesRef)$
        batchValueBytes += posValueBytes;
        batchFixedLength = posFixedLength;
$endif$
        return true;
    }

    private long batchOutputBytes(int count) {
        return computeOutputBytes(batchSlots, count, batchHasMultiValues$if(BytesRef)$, batchValueBytes, batchFixedLength$endif$);
    }

    private static long computeOutputBytes(
        long slots,
        int count,
        boolean hasMultiValues$if(BytesRef)$,
        long valueBytes,
        int fixedLength$endif$
    ) {
$if(BytesRef)$
        long outputBytes = BytesRefArrayBlock.BASE_RAM_BYTES_USED + valueBytes;
        if (fixedLength == VARIABLE_LENGTH) {
            outputBytes += (slots + 1L) * Integer.BYTES;
        }
$else$
        long outputBytes = RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + slots * $BYTES$;
$endif$
        if (hasMultiValues) {
            outputBytes += RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (count + 1L) * Integer.BYTES;
        }
        return outputBytes;
    }

    private $Type$Block buildBatch(int batchStart, int batchEnd) {
        int exactSlots = Math.toIntExact(batchSlots);
$if(BytesRef)$
        try ($Type$Block.Builder builder = positions.blockFactory().newBytesRefBlockBuilder(exactSlots, Math.max(1L, batchValueBytes))) {
$else$
        try ($Type$Block.Builder builder = positions.blockFactory().new$Type$BlockBuilder(exactSlots)) {
$endif$
            for (int p = batchStart; p < batchEnd; p++) {
                int start = positions.getFirstValueIndex(p);
                int end = start + positions.getValueCount(p);
                valuesInPosition = 0;
                for (int i = start; i < end; i++) {
                    copy(builder, positions.getInt(i));
                }
                switch (valuesInPosition) {
                    case 0 -> builder.appendNull();
                    case 1 -> builder.append$Type$(first);
                    default -> builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }

$if(BytesRef)$
    private static int mergeFixedLength(int current, int length) {
        if (current == UNSET_FIXED_LENGTH || current == length) {
            return length;
        }
        return VARIABLE_LENGTH;
    }

$endif$
    private void copy($Type$Block.Builder builder, int valuePosition) {
        if (valuePosition >= values.getPositionCount()) {
            return;
        }
        int start = values.getFirstValueIndex(valuePosition);
        int end = start + values.getValueCount(valuePosition);
        for (int i = start; i < end; i++) {
            if (valuesInPosition == 0) {
$if(BytesRef)$
                first = values.get$Type$(i, firstScratch);
$else$
                first = values.get$Type$(i);
$endif$
                valuesInPosition++;
                continue;
            }
            if (valuesInPosition == 1) {
                builder.beginPositionEntry();
                builder.append$Type$(first);
            }
$if(BytesRef)$
            builder.append$Type$(values.get$Type$(i, valueScratch));
$else$
            builder.append$Type$(values.get$Type$(i));
$endif$
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
