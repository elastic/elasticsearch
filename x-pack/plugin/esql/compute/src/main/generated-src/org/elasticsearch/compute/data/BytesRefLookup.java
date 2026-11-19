/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

// begin generated imports
import org.apache.lucene.util.BytesRef;
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
    private final BytesRef firstScratch = new BytesRef();
    private final BytesRef valueScratch = new BytesRef();
    private final BytesRefBlock values;
    private final IntBlock positions;
    private final long targetByteSize;
    private int position;
    private long usedBytes;

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
        // build() keeps unused capacity, so pre-size for at most the values that fit in the target.
        long maxValues = targetByteSize / Integer.BYTES;
        int remaining = positions.getPositionCount() - position;
        int estimatedSize = (int) Math.min(remaining, Math.max(Operator.MIN_TARGET_PAGE_SIZE + 1, maxValues));
        try (BytesRefBlock.Builder builder = positions.blockFactory().newBytesRefBlockBuilder(estimatedSize)) {
            int positionsInBlock = 0;
            boolean multivalued = false;
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
                        usedBytes += Integer.BYTES;
                    }
                    case 1 -> builder.appendBytesRef(first);
                    default -> {
                        builder.endPositionEntry();
                        multivalued = true;
                    }
                }
                position++;
                positionsInBlock++;
                // TOOD what if the estimate is super huge? should we break even with less than MIN_TARGET?
                // builder.estimatedBytes() includes the pre-sized capacity, so track the bytes actually used.
                // Once a block has a multivalued position, every position has a first value index.
                long indexBytes = multivalued ? (long) Integer.BYTES * (positionsInBlock + 1) : 0;
                if (positionsInBlock > Operator.MIN_TARGET_PAGE_SIZE && usedBytes + indexBytes >= targetByteSize) {
                    break;
                }
            }
            return builder.build();
        }
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
                usedBytes += first.length + Integer.BYTES;
                valuesInPosition++;
                continue;
            }
            if (valuesInPosition == 1) {
                builder.beginPositionEntry();
                builder.appendBytesRef(first);
            }
            if (valuesInPosition > Block.MAX_LOOKUP) {
                // TODO replace this with a warning and break
                throw new IllegalArgumentException("Found a single entry with " + valuesInPosition + " entries");
            }
            BytesRef value = values.getBytesRef(i, valueScratch);
            usedBytes += value.length + Integer.BYTES;
            builder.appendBytesRef(value);
            valuesInPosition++;
        }
    }

    @Override
    public void close() {
        Releasables.close(values, positions);
    }
}
