/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

// begin generated imports
$if(BytesRef)$
import org.apache.lucene.util.BytesRef;
$endif$
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
    private final BytesRef firstScratch = new BytesRef();
    private final BytesRef valueScratch = new BytesRef();
$endif$
    private final $Type$Block values;
    private final IntBlock positions;
    private final long targetByteSize;
    private int position;
    private long usedBytes;

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
        // build() keeps unused capacity, so pre-size for at most the values that fit in the target.
$if(BytesRef)$
        long maxValues = targetByteSize / Integer.BYTES;
$else$
        long maxValues = targetByteSize / $BYTES$;
$endif$
        int remaining = positions.getPositionCount() - position;
        int estimatedSize = (int) Math.min(remaining, Math.max(Operator.MIN_TARGET_PAGE_SIZE + 1, maxValues));
        try ($Type$Block.Builder builder = positions.blockFactory().new$Type$BlockBuilder(estimatedSize)) {
            int count = 0;
            usedBytes = 0;
            while (position < positions.getPositionCount()) {
                int start = positions.getFirstValueIndex(position);
                int end = start + positions.getValueCount(position);
                valuesInPosition = 0;
                for (int i = start; i < end; i++) {
                    copy(builder, positions.getInt(i));
                }
                switch (valuesInPosition) {
                    case 0 -> {
                        // A null still takes a value slot.
                        builder.appendNull();
$if(BytesRef)$
                        usedBytes += Integer.BYTES;
$else$
                        usedBytes += $BYTES$;
$endif$
                    }
                    case 1 -> builder.append$Type$(first);
                    default -> {
                        // A multivalued position adds a first value index.
                        builder.endPositionEntry();
                        usedBytes += Integer.BYTES;
                    }
                }
                position++;
                // TOOD what if the estimate is super huge? should we break even with less than MIN_TARGET?
                // builder.estimatedBytes() includes the pre-sized capacity, so track the bytes actually used.
                if (++count > Operator.MIN_TARGET_PAGE_SIZE && usedBytes >= targetByteSize) {
                    break;
                }
            }
            return builder.build();
        }
    }

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
                usedBytes += first.length + Integer.BYTES;
$else$
                first = values.get$Type$(i);
                usedBytes += $BYTES$;
$endif$
                valuesInPosition++;
                continue;
            }
            if (valuesInPosition == 1) {
                builder.beginPositionEntry();
                builder.append$Type$(first);
            }
            if (valuesInPosition > Block.MAX_LOOKUP) {
                // TODO replace this with a warning and break
                throw new IllegalArgumentException("Found a single entry with " + valuesInPosition + " entries");
            }
$if(BytesRef)$
            $type$ value = values.get$Type$(i, valueScratch);
            usedBytes += value.length + Integer.BYTES;
            builder.append$Type$(value);
$else$
            builder.append$Type$(values.get$Type$(i));
            usedBytes += $BYTES$;
$endif$
            valuesInPosition++;
        }
    }

    @Override
    public void close() {
        Releasables.close(values, positions);
    }
}
