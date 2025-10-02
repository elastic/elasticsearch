/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

// begin generated imports
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
// end generated imports

/**
 * Generic {@link Block#lookup} implementation {@link BooleanBlock}s.
 * This class is generated. Edit {@code X-Lookup.java.st} instead.
 */
final class BooleanLookup implements ReleasableIterator<BooleanBlock> {
    private final BooleanBlock values;
    private final IntBlock positions;
    private final long targetByteSize;
    private int position;

    private boolean first;
    private int valuesInPosition;

    BooleanLookup(BooleanBlock values, IntBlock positions, ByteSizeValue targetBlockSize) {
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
    public BooleanBlock next() {
        try (BooleanBlock.Builder builder = positions.blockFactory().newBooleanBlockBuilder(positions.getTotalValueCount())) {
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
                    case 1 -> builder.appendBoolean(first);
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

    private void copy(BooleanBlock.Builder builder, int valuePosition) {
        if (valuePosition >= values.getPositionCount()) {
            return;
        }
        int start = values.getFirstValueIndex(valuePosition);
        int end = start + values.getValueCount(valuePosition);
        for (int i = start; i < end; i++) {
            if (valuesInPosition == 0) {
                first = values.getBoolean(i);
                valuesInPosition++;
                continue;
            }
            if (valuesInPosition == 1) {
                builder.beginPositionEntry();
                builder.appendBoolean(first);
            }
            if (valuesInPosition > Block.MAX_LOOKUP) {
                // TODO replace this with a warning and break
                throw new IllegalArgumentException("Found a single entry with " + valuesInPosition + " entries");
            }
            builder.appendBoolean(values.getBoolean(i));
            valuesInPosition++;
        }
    }

    @Override
    public void close() {
        Releasables.close(values, positions);
    }
}
