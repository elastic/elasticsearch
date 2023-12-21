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
import org.elasticsearch.core.Releasables;

$endif$
import java.util.BitSet;

/**
 * Block implementation that stores an array of $type$.
 * This class is generated. Do not edit it.
 */
final class $Type$ArrayBlock extends AbstractArrayBlock implements $Type$Block {

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance($Type$ArrayBlock.class);

    private final $Type$ArrayVector values;

    $Type$ArrayBlock(
        $if(BytesRef)$BytesRefArray$else$$type$[]$endif$ values,
        int positionCount,
        int[] firstValueIndexes,
        BitSet nulls,
        MvOrdering mvOrdering,
        BlockFactory blockFactory
    ) {
        super(positionCount, firstValueIndexes, nulls, mvOrdering, blockFactory);
        $if(BytesRef)$
        this.values = new BytesRefArrayVector(values, (int) values.size());
        $else$
        this.values = new $Type$ArrayVector(values, values.length);
        $endif$
    }

    @Override
    public $Type$Vector asVector() {
        return null;
    }

    @Override
$if(BytesRef)$
    public BytesRef getBytesRef(int valueIndex, BytesRef dest) {
        return values.getBytesRef(valueIndex, dest);
$else$
    public $type$ get$Type$(int valueIndex) {
        return values.get$Type$(valueIndex);
$endif$
    }

    @Override
    public $Type$Block filter(int... positions) {
$if(BytesRef)$
        final BytesRef scratch = new BytesRef();
$endif$
        try (var builder = blockFactory().new$Type$BlockBuilder(positions.length)) {
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
        // TODO use reference counting to share the values
$if(BytesRef)$
        final BytesRef scratch = new BytesRef();
$endif$
        try (var builder = blockFactory().new$Type$BlockBuilder(firstValueIndexes[getPositionCount()])) {
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

    public long ramBytesUsedOnlyBlock() {
        return BASE_RAM_BYTES_USED + BlockRamUsageEstimator.sizeOf(firstValueIndexes) + BlockRamUsageEstimator.sizeOfBitSet(nullsMask);
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesUsedOnlyBlock() + values.ramBytesUsed();
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
            + values.getPositionCount()
$else$
            + ", values="
            + values
$endif$
            + ']';
    }

    @Override
    public void closeInternal() {
        blockFactory().adjustBreaker(-ramBytesUsedOnlyBlock(), true);
        Releasables.closeExpectNoException(values);
    }
}
