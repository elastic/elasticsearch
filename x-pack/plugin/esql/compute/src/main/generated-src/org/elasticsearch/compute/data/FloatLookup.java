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
 * Generic {@link Block#lookup} implementation {@link FloatBlock}s.
 * This class is generated. Edit {@code X-Lookup.java.st} instead.
 */
public final class FloatLookup implements ReleasableIterator<FloatBlock> {
    private static final int PREALLOCATED_BYTES_PER_VALUE = Float.BYTES;
    private final FloatBlock values;
    private final IntBlock positions;
    private final long targetByteSize;
    private final int initialPositions;
    private int position;

    private float first;
    private int valuesInPosition;

    public FloatLookup(FloatBlock values, IntBlock positions, ByteSizeValue targetBlockSize) {
        values.incRef();
        positions.incRef();
        this.values = values;
        this.positions = positions;
        this.targetByteSize = targetBlockSize.getBytes();
        // The builder counts preallocated values in estimatedBytes, so start at half the target.
        this.initialPositions = (int) Math.min(Integer.MAX_VALUE, Math.max(1, targetByteSize / 2 / PREALLOCATED_BYTES_PER_VALUE));
    }

    @Override
    public boolean hasNext() {
        return position < positions.getPositionCount();
    }

    @Override
    public FloatBlock next() {
        int initialSize = Math.min(positions.getPositionCount() - position, initialPositions);
        try (FloatBlock.Builder builder = positions.blockFactory().newFloatBlockBuilder(initialSize)) {
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
                    case 1 -> builder.appendFloat(first);
                    default -> builder.endPositionEntry();
                }
                position++;
                // TOOD what if the estimate is super huge? should we break even with less than MIN_TARGET?
                if (++count > Operator.MIN_TARGET_PAGE_SIZE && builder.estimatedBytes() >= targetByteSize) {
                    break;
                }
            }
            return builder.build();
        }
    }

    private void copy(FloatBlock.Builder builder, int valuePosition) {
        if (valuePosition >= values.getPositionCount()) {
            return;
        }
        int start = values.getFirstValueIndex(valuePosition);
        int end = start + values.getValueCount(valuePosition);
        for (int i = start; i < end; i++) {
            if (valuesInPosition == 0) {
                first = values.getFloat(i);
                valuesInPosition++;
                continue;
            }
            if (valuesInPosition == 1) {
                builder.beginPositionEntry();
                builder.appendFloat(first);
            }
            if (valuesInPosition > Block.MAX_LOOKUP) {
                // TODO replace this with a warning and break
                throw new IllegalArgumentException("Found a single entry with " + valuesInPosition + " entries");
            }
            builder.appendFloat(values.getFloat(i));
            valuesInPosition++;
        }
    }

    @Override
    public void close() {
        Releasables.close(values, positions);
    }
}
