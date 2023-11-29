/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

$if(BytesRef)$
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.core.Releasables;

$else$
import org.apache.lucene.util.RamUsageEstimator;

import java.util.Arrays;
$endif$
import java.util.BitSet;

/**
 * Block implementation that stores an array of $type$.
 * This class is generated. Do not edit it.
 */
public final class $Type$ArrayBlock extends AbstractArrayBlock implements $Type$Block {

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance($Type$ArrayBlock.class);

$if(BytesRef)$
    private final BytesRefArray values;

$else$
    private final $type$[] values;
$endif$

    public $Type$ArrayBlock($if(BytesRef)$BytesRefArray$else$$type$[]$endif$ values, int positionCount, int[] firstValueIndexes, BitSet nulls, MvOrdering mvOrdering) {
        this(values, positionCount, firstValueIndexes, nulls, mvOrdering, BlockFactory.getNonBreakingInstance());
    }

    public $Type$ArrayBlock(
        $if(BytesRef)$BytesRefArray$else$$type$[]$endif$ values,
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
    public $Type$Vector asVector() {
        return null;
    }

    @Override
$if(BytesRef)$
    public BytesRef getBytesRef(int valueIndex, BytesRef dest) {
        return values.get(valueIndex, dest);
$else$
    public $type$ get$Type$(int valueIndex) {
        return values[valueIndex];
$endif$
    }

    @Override
    public $Type$Block filter(int... positions) {
$if(BytesRef)$
        final BytesRef scratch = new BytesRef();
$endif$
        try (var builder = blockFactory.new$Type$BlockBuilder(positions.length)) {
            for (int pos : positions) {
                if (isNull(pos)) {
                    builder.appendNull();
                    continue;
                }
                int valueCount = getValueCount(pos);
                int first = getFirstValueIndex(pos);
                if (valueCount == 1) {
                    builder.append$Type$(get$Type$(getFirstValueIndex(pos)$if(BytesRef)$, scratch$endif$));
                } else {
                    builder.beginPositionEntry();
                    for (int c = 0; c < valueCount; c++) {
                        builder.append$Type$(get$Type$(first + c$if(BytesRef)$, scratch$endif$));
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.mvOrdering(mvOrdering()).build();
        }
    }

    @Override
    public ElementType elementType() {
        return ElementType.$TYPE$;
    }

    @Override
    public $Type$Block expand() {
        if (firstValueIndexes == null) {
            incRef();
            return this;
        }
        if (nullsMask == null) {
            incRef();
            return new $Type$ArrayBlock.Expanded();
        }
        // TODO use reference counting to share the values
$if(BytesRef)$
        final BytesRef scratch = new BytesRef();
$endif$
        try (var builder = blockFactory.new$Type$BlockBuilder(firstValueIndexes[getPositionCount()])) {
            for (int pos = 0; pos < getPositionCount(); pos++) {
                if (isNull(pos)) {
                    builder.appendNull();
                    continue;
                }
                int first = getFirstValueIndex(pos);
                int end = first + getValueCount(pos);
                for (int i = first; i < end; i++) {
$if(BytesRef)$
                    builder.append$Type$(get$Type$(i, scratch));
$else$
                    builder.append$Type$(get$Type$(i));
$endif$
                }
            }
            return builder.mvOrdering(MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING).build();
        }
    }

    public static long ramBytesEstimated($if(BytesRef)$BytesRefArray$else$$type$[]$endif$ values, int[] firstValueIndexes, BitSet nullsMask) {
        return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(values) + BlockRamUsageEstimator.sizeOf(firstValueIndexes)
            + BlockRamUsageEstimator.sizeOfBitSet(nullsMask);
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesEstimated(values, firstValueIndexes, nullsMask);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof $Type$Block that) {
            return $Type$Block.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return $Type$Block.hash(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()
            + "[positions="
            + getPositionCount()
            + ", mvOrdering="
            + mvOrdering()
$if(BytesRef)$
            + ", values="
            + values.size()
$else$
            + ", values="
            + Arrays.toString(values)
$endif$
            + ']';
    }

    @Override
    public void closeInternal() {
    $if(BytesRef)$
        blockFactory.adjustBreaker(-ramBytesUsed() + values.bigArraysRamBytesUsed(), true);
        Releasables.closeExpectNoException(values);
    $else$
        blockFactory.adjustBreaker(-ramBytesUsed(), true);
    $endif$
    }

    final class Expanded implements $Type$Block {
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
            return $Type$ArrayBlock.this.getTotalValueCount();
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
        public $Type$Vector asVector() {
            return null;
        }

        @Override
    $if(BytesRef)$
        public BytesRef getBytesRef(int valueIndex, BytesRef dest) {
            return values.get(valueIndex, dest);
    $else$
        public $type$ get$Type$(int valueIndex) {
            return values[valueIndex];
    $endif$
        }

        // TODO: avoid deep copying and incRef instead.
        @Override
        public $Type$Block filter(int... positions) {
    $if(BytesRef)$
            final BytesRef scratch = new BytesRef();
    $endif$
            try (var builder = blockFactory.new$Type$BlockBuilder(positions.length)) {
                for (int pos : positions) {
                    if (isNull(pos)) {
                        builder.appendNull();
                        continue;
                    }
                    builder.append$Type$(get$Type$(getFirstValueIndex(pos)$if(BytesRef)$, scratch$endif$));
                }
                return builder.mvOrdering(mvOrdering()).build();
            }
        }

        @Override
        public ElementType elementType() {
            return $Type$ArrayBlock.this.elementType();
        }

        @Override
        public $Type$Block expand() {
            incRef();
            return this;
        }

        @Override
        public long ramBytesUsed() {
            return $Type$ArrayBlock.this.ramBytesUsed();
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
            return getClass().getSimpleName() + "[positions=" + getPositionCount() $if(BytesRef)$+ ", values=" + values.size()$else$+ ", values=" + Arrays.toString(values)$endif$ + ']';
        }

        @Override
        public boolean isReleased() {
            return hasReferences() == false;
        }

        @Override
        public void incRef() {
            $Type$ArrayBlock.this.incRef();
        }

        @Override
        public boolean tryIncRef() {
            return $Type$ArrayBlock.this.tryIncRef();
        }

        @Override
        public boolean decRef() {
            return $Type$ArrayBlock.this.decRef();
        }

        @Override
        public boolean hasReferences() {
            return $Type$ArrayBlock.this.hasReferences();
        }

        @Override
        public void close() {
            $Type$ArrayBlock.this.close();
        }
    }
}
