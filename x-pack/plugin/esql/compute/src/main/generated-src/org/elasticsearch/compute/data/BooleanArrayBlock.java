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
 * Block implementation that stores an array of boolean.
 * This class is generated. Do not edit it.
 */
public final class BooleanArrayBlock extends AbstractArrayBlock implements BooleanBlock {

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(BooleanArrayBlock.class);

    private final boolean[] values;

    public BooleanArrayBlock(boolean[] values, int positionCount, int[] firstValueIndexes, BitSet nulls, MvOrdering mvOrdering) {
        this(values, positionCount, firstValueIndexes, nulls, mvOrdering, BlockFactory.getNonBreakingInstance());
    }

    public BooleanArrayBlock(
        boolean[] values,
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
    public BooleanVector asVector() {
        return null;
    }

    @Override
    public boolean getBoolean(int valueIndex) {
        return values[valueIndex];
    }

    @Override
    public BooleanBlock filter(int... positions) {
        try (var builder = blockFactory.newBooleanBlockBuilder(positions.length)) {
            for (int pos : positions) {
                if (isNull(pos)) {
                    builder.appendNull();
                    continue;
                }
                int valueCount = getValueCount(pos);
                int first = getFirstValueIndex(pos);
                if (valueCount == 1) {
                    builder.appendBoolean(getBoolean(getFirstValueIndex(pos)));
                } else {
                    builder.beginPositionEntry();
                    for (int c = 0; c < valueCount; c++) {
                        builder.appendBoolean(getBoolean(first + c));
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.mvOrdering(mvOrdering()).build();
        }
    }

    @Override
    public ElementType elementType() {
        return ElementType.BOOLEAN;
    }

    @Override
    public BooleanBlock expand() {
        if (firstValueIndexes == null) {
            incRef();
            return this;
        }
        // TODO use reference counting to share the values
        try (var builder = blockFactory.newBooleanBlockBuilder(firstValueIndexes[getPositionCount()])) {
            for (int pos = 0; pos < getPositionCount(); pos++) {
                if (isNull(pos)) {
                    builder.appendNull();
                    continue;
                }
                int first = getFirstValueIndex(pos);
                int end = first + getValueCount(pos);
                for (int i = first; i < end; i++) {
                    builder.appendBoolean(getBoolean(i));
                }
            }
            return builder.mvOrdering(MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING).build();
        }
    }

    public static long ramBytesEstimated(boolean[] values, int[] firstValueIndexes, BitSet nullsMask) {
        return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(values) + BlockRamUsageEstimator.sizeOf(firstValueIndexes)
            + BlockRamUsageEstimator.sizeOfBitSet(nullsMask);
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesEstimated(values, firstValueIndexes, nullsMask);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof BooleanBlock that) {
            return BooleanBlock.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return BooleanBlock.hash(this);
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

    final class Expanded implements BooleanBlock {
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
            return BooleanArrayBlock.this.getTotalValueCount();
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
        public BooleanVector asVector() {
            return null;
        }

        @Override
        public boolean getBoolean(int valueIndex) {
            return values[valueIndex];
        }

        // TODO: avoid deep copying and incRef instead.
        @Override
        public BooleanBlock filter(int... positions) {
            try (var builder = blockFactory.newBooleanBlockBuilder(positions.length)) {
                for (int pos : positions) {
                    if (isNull(pos)) {
                        builder.appendNull();
                        continue;
                    }
                    builder.appendBoolean(getBoolean(getFirstValueIndex(pos)));
                }
                return builder.mvOrdering(mvOrdering()).build();
            }
        }

        @Override
        public ElementType elementType() {
            return BooleanArrayBlock.this.elementType();
        }

        @Override
        public BooleanBlock expand() {
            incRef();
            return this;
        }

        @Override
        public long ramBytesUsed() {
            return BooleanArrayBlock.this.ramBytesUsed();
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
            BooleanArrayBlock.this.incRef();
        }

        @Override
        public boolean tryIncRef() {
            return BooleanArrayBlock.this.tryIncRef();
        }

        @Override
        public boolean decRef() {
            return BooleanArrayBlock.this.decRef();
        }

        @Override
        public boolean hasReferences() {
            return BooleanArrayBlock.this.hasReferences();
        }

        @Override
        public void close() {
            BooleanArrayBlock.this.close();
        }
    }
}
