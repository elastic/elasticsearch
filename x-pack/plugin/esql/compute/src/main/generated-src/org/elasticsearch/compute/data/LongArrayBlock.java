/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.RamUsageEstimator;

import java.util.Arrays;
import java.util.BitSet;

/**
 * Block implementation that stores an array of long.
 * This class is generated. Do not edit it.
 */
public final class LongArrayBlock extends AbstractArrayBlock implements LongBlock {

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(LongArrayBlock.class);

    private final long[] values;

    public LongArrayBlock(long[] values, int positionCount, int[] firstValueIndexes, BitSet nulls, MvOrdering mvOrdering) {
        this(values, positionCount, firstValueIndexes, nulls, mvOrdering, BlockFactory.getNonBreakingInstance());
    }

    public LongArrayBlock(
        long[] values,
        int positionCount,
        int[] firstValueIndexes,
        BitSet nulls,
        MvOrdering mvOrdering,
        BlockFactory blockFactory
    ) {
        super(positionCount, firstValueIndexes, nulls, mvOrdering, blockFactory);
        this.values = values;
    }

    @Override
    public LongVector asVector() {
        return null;
    }

    @Override
    public long getLong(int valueIndex) {
        return values[valueIndex];
    }

    @Override
    public LongBlock filter(int... positions) {
        try (var builder = blockFactory.newLongBlockBuilder(positions.length)) {
            for (int pos : positions) {
                if (isNull(pos)) {
                    builder.appendNull();
                    continue;
                }
                int valueCount = getValueCount(pos);
                int first = getFirstValueIndex(pos);
                if (valueCount == 1) {
                    builder.appendLong(getLong(getFirstValueIndex(pos)));
                } else {
                    builder.beginPositionEntry();
                    for (int c = 0; c < valueCount; c++) {
                        builder.appendLong(getLong(first + c));
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.mvOrdering(mvOrdering()).build();
        }
    }

    @Override
    public ElementType elementType() {
        return ElementType.LONG;
    }

    @Override
    public LongBlock expand() {
        if (firstValueIndexes == null) {
            incRef();
            return this;
        }
        if (nullsMask == null) {
            incRef();
            return new LongArrayBlock.Expanded();
        }
        // TODO use reference counting to share the values
        try (var builder = blockFactory.newLongBlockBuilder(firstValueIndexes[getPositionCount()])) {
            for (int pos = 0; pos < getPositionCount(); pos++) {
                if (isNull(pos)) {
                    builder.appendNull();
                    continue;
                }
                int first = getFirstValueIndex(pos);
                int end = first + getValueCount(pos);
                for (int i = first; i < end; i++) {
                    builder.appendLong(getLong(i));
                }
            }
            return builder.mvOrdering(MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING).build();
        }
    }

    public static long ramBytesEstimated(long[] values, int[] firstValueIndexes, BitSet nullsMask) {
        return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(values) + BlockRamUsageEstimator.sizeOf(firstValueIndexes)
            + BlockRamUsageEstimator.sizeOfBitSet(nullsMask);
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesEstimated(values, firstValueIndexes, nullsMask);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof LongBlock that) {
            return LongBlock.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return LongBlock.hash(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()
            + "[positions="
            + getPositionCount()
            + ", mvOrdering="
            + mvOrdering()
            + ", values="
            + Arrays.toString(values)
            + ']';
    }

    @Override
    public void closeInternal() {
        blockFactory.adjustBreaker(-ramBytesUsed(), true);
    }

    final class Expanded implements LongBlock {
        private Expanded() {}

        @Override
        public int getFirstValueIndex(int position) {
            return position;
        }

        @Override
        public int getValueCount(int position) {
            return isNull(position) ? 0 : 1;
        }

        @Override
        public boolean isNull(int position) {
            // TODO: add null mask
            return false;
        }

        @Override
        public boolean mayHaveNulls() {
            // TODO: add null mask
            return false;
        }

        @Override
        public int nullValuesCount() {
            // TODO: add null mask
            return 0;
        }

        @Override
        public boolean areAllValuesNull() {
            return nullValuesCount() == getPositionCount();
        }

        @Override
        public BlockFactory blockFactory() {
            return blockFactory;
        }

        @Override
        public int getTotalValueCount() {
            return getPositionCount();
        }

        @Override
        public int getPositionCount() {
            return LongArrayBlock.this.getTotalValueCount();
        }

        @Override
        public boolean mayHaveMultivaluedFields() {
            return false;
        }

        @Override
        public MvOrdering mvOrdering() {
            return MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING;
        }

        @Override
        public LongVector asVector() {
            return null;
        }

        @Override
        public long getLong(int valueIndex) {
            return values[valueIndex];
        }

        // TODO: avoid deep copying and incRef instead.
        @Override
        public LongBlock filter(int... positions) {
            try (var builder = blockFactory.newLongBlockBuilder(positions.length)) {
                for (int pos : positions) {
                    if (isNull(pos)) {
                        builder.appendNull();
                        continue;
                    }
                    builder.appendLong(getLong(getFirstValueIndex(pos)));
                }
                return builder.mvOrdering(mvOrdering()).build();
            }
        }

        @Override
        public ElementType elementType() {
            return LongArrayBlock.this.elementType();
        }

        @Override
        public LongBlock expand() {
            incRef();
            return this;
        }

        @Override
        public long ramBytesUsed() {
            return LongArrayBlock.this.ramBytesUsed();
        }

        @Override
        public boolean equals(Object obj) {
            throw new AssertionError("TODO");
        }

        @Override
        public int hashCode() {
            throw new AssertionError("TODO");
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "[positions=" + getPositionCount() + ", values=" + Arrays.toString(values) + ']';
        }

        @Override
        public boolean isReleased() {
            return hasReferences() == false;
        }

        @Override
        public void incRef() {
            LongArrayBlock.this.incRef();
        }

        @Override
        public boolean tryIncRef() {
            return LongArrayBlock.this.tryIncRef();
        }

        @Override
        public boolean decRef() {
            return LongArrayBlock.this.decRef();
        }

        @Override
        public boolean hasReferences() {
            return LongArrayBlock.this.hasReferences();
        }

        @Override
        public void close() {
            LongArrayBlock.this.close();
        }
    }
}
