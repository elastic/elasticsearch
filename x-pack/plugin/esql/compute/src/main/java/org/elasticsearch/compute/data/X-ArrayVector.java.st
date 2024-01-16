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

/**
 * Vector implementation that stores an array of $type$ values.
$if(BytesRef)$
 * Does not take ownership of the given {@link BytesRefArray} and does not adjust circuit breakers to account for it.
$endif$
 * This class is generated. Do not edit it.
 */
final class $Type$ArrayVector extends AbstractVector implements $Type$Vector {

    static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance($Type$ArrayVector.class)
        // TODO: remove these extra bytes once `asBlock` returns a block with a separate reference to the vector.
        + RamUsageEstimator.shallowSizeOfInstance($Type$VectorBlock.class);

$if(BytesRef)$
    private final BytesRefArray values;

$else$
    private final $type$[] values;
$endif$

    $Type$ArrayVector($if(BytesRef)$BytesRefArray$else$$type$[]$endif$ values, int positionCount, BlockFactory blockFactory) {
        super(positionCount, blockFactory);
        this.values = values;
    }

    @Override
    public $Type$Block asBlock() {
        return new $Type$VectorBlock(this);
    }

$if(BytesRef)$
    @Override
    public BytesRef getBytesRef(int position, BytesRef dest) {
        return values.get(position, dest);
    }

$else$
    @Override
    public $type$ get$Type$(int position) {
        return values[position];
    }
$endif$

    @Override
    public ElementType elementType() {
        return ElementType.$TYPE$;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public $Type$Vector filter(int... positions) {
    $if(BytesRef)$
        final var scratch = new BytesRef();
    $endif$
        try ($Type$Vector.Builder builder = blockFactory().new$Type$VectorBuilder(positions.length)) {
            for (int pos : positions) {
            $if(BytesRef)$
                builder.append$Type$(values.get(pos, scratch));
            $else$
                builder.append$Type$(values[pos]);
            $endif$
            }
            return builder.build();
        }
    }

    public static long ramBytesEstimated($if(BytesRef)$BytesRefArray$else$$type$[]$endif$ values) {
        return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(values);
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesEstimated(values);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof $Type$Vector that) {
            return $Type$Vector.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return $Type$Vector.hash(this);
    }

    @Override
    public String toString() {
$if(BytesRef)$
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ']';
$else$
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ", values=" + Arrays.toString(values) + ']';
$endif$
    }

$if(BytesRef)$
    @Override
    public void closeInternal() {
        // The circuit breaker that tracks the values {@link BytesRefArray} is adjusted outside
        // of this class.
        blockFactory().adjustBreaker(-ramBytesUsed() + values.bigArraysRamBytesUsed());
        Releasables.closeExpectNoException(values);
    }
$endif$
}
