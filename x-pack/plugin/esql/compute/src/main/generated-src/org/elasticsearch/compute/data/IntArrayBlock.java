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
 * Block implementation that stores an array of int.
 * This class is generated. Do not edit it.
 */
public final class IntArrayBlock extends AbstractArrayBlock implements IntBlock {

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(IntArrayBlock.class);

    private final int[] values;

    public IntArrayBlock(int[] values, int positionCount, int[] firstValueIndexes, BitSet nulls, MvOrdering mvOrdering) {
        this(values, positionCount, firstValueIndexes, nulls, mvOrdering, BlockFactory.getNonBreakingInstance());
    }

    public IntArrayBlock(
        int[] values,
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
    public IntVector asVector() {
        return null;
    }

    @Override
    public int getInt(int valueIndex) {
        return values[valueIndex];
    }

    @Override
    public IntBlock filter(int... positions) {
        try (var builder = blockFactory.newIntBlockBuilder(positions.length)) {
            for (int pos : positions) {
                if (isNull(pos)) {
                    builder.appendNull();
                    continue;
                }
                int valueCount = getValueCount(pos);
                int first = getFirstValueIndex(pos);
                if (valueCount == 1) {
                    builder.appendInt(getInt(getFirstValueIndex(pos)));
                } else {
                    builder.beginPositionEntry();
                    for (int c = 0; c < valueCount; c++) {
                        builder.appendInt(getInt(first + c));
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.mvOrdering(mvOrdering()).build();
        }
    }

    @Override
    public ElementType elementType() {
        return ElementType.INT;
    }

    @Override
    public IntBlock expand() {
        if (firstValueIndexes == null) {
            incRef();
            return this;
        }
        // TODO use reference counting to share the values
        try (var builder = blockFactory.newIntBlockBuilder(firstValueIndexes[getPositionCount()])) {
            for (int pos = 0; pos < getPositionCount(); pos++) {
                if (isNull(pos)) {
                    builder.appendNull();
                    continue;
                }
                int first = getFirstValueIndex(pos);
                int end = first + getValueCount(pos);
                for (int i = first; i < end; i++) {
                    builder.appendInt(getInt(i));
                }
            }
            return builder.mvOrdering(MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING).build();
        }
    }

    public static long ramBytesEstimated(int[] values, int[] firstValueIndexes, BitSet nullsMask) {
        return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(values) + BlockRamUsageEstimator.sizeOf(firstValueIndexes)
            + BlockRamUsageEstimator.sizeOfBitSet(nullsMask);
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesEstimated(values, firstValueIndexes, nullsMask);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof IntBlock that) {
            return IntBlock.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return IntBlock.hash(this);
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

    final class Expanded implements IntBlock {
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
            return IntArrayBlock.this.getTotalValueCount();
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
        public IntVector asVector() {
            return null;
        }

        @Override
        public int getInt(int valueIndex) {
            return values[valueIndex];
        }

        // TODO: avoid deep copying and incRef instead.
        @Override
        public IntBlock filter(int... positions) {
            try (var builder = blockFactory.newIntBlockBuilder(positions.length)) {
                for (int pos : positions) {
                    if (isNull(pos)) {
                        builder.appendNull();
                        continue;
                    }
                    builder.appendInt(getInt(getFirstValueIndex(pos)));
                }
                return builder.mvOrdering(mvOrdering()).build();
            }
        }

        @Override
        public ElementType elementType() {
            return IntArrayBlock.this.elementType();
        }

        @Override
        public IntBlock expand() {
            incRef();
            return this;
        }

        @Override
        public long ramBytesUsed() {
            return IntArrayBlock.this.ramBytesUsed();
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
            IntArrayBlock.this.incRef();
        }

        @Override
        public boolean tryIncRef() {
            return IntArrayBlock.this.tryIncRef();
        }

        @Override
        public boolean decRef() {
            return IntArrayBlock.this.decRef();
        }

        @Override
        public boolean hasReferences() {
            return IntArrayBlock.this.hasReferences();
        }

        @Override
        public void close() {
            IntArrayBlock.this.close();
        }
    }
}
