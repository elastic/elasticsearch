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
final class $Type$Lookup implements ReleasableIterator<$Type$Block> {
$if(BytesRef)$
    private final BytesRef firstScratch = new BytesRef();
    private final BytesRef valueScratch = new BytesRef();
$endif$
    private final $Type$Block values;
    private final IntBlock positions;
    private final long targetByteSize;
    private int position;

    private $type$ first;
    private int valuesInPosition;

    $Type$Lookup($Type$Block values, IntBlock positions, ByteSizeValue targetBlockSize) {
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
        try ($Type$Block.Builder builder = positions.blockFactory().new$Type$BlockBuilder(positions.getTotalValueCount())) {
            int count = 0;
            while (position < positions.getPositionCount()) {
                int start = positions.getFirstValueIndex(position);
                int end = start + positions.getValueCount(position);
                valuesInPosition = 0;
                for (int i = start; i < end; i++) {
                    copy(builder, positions.getInt(i));
                }
                switch (valuesInPosition) {
                    case 0 -> builder.appendNull();
                    case 1 -> builder.append$Type$(first);
                    default -> builder.endPositionEntry();
                }
                position++;
                // TOOD what if the estimate is super huge? should we break even with less than MIN_TARGET?
                if (++count > Operator.MIN_TARGET_PAGE_SIZE && builder.estimatedBytes() < targetByteSize) {
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
            if (valuesInPosition > Block.MAX_LOOKUP) {
                // TODO replace this with a warning and break
                throw new IllegalArgumentException("Found a single entry with " + valuesInPosition + " entries");
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
        Releasables.close(values, positions);
    }
}
