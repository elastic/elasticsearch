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
 * This class is generated. Do not edit it.
 */
public final class $Type$ArrayVector extends AbstractVector implements $Type$Vector {

    static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance($Type$ArrayVector.class);

$if(BytesRef)$
    private final BytesRefArray values;

$else$
    private final $type$[] values;
$endif$

    private final $Type$Block block;

    public $Type$ArrayVector($if(BytesRef)$BytesRefArray$else$$type$[]$endif$ values, int positionCount) {
        this(values, positionCount, BlockFactory.getNonBreakingInstance());
    }

    public $Type$ArrayVector($if(BytesRef)$BytesRefArray$else$$type$[]$endif$ values, int positionCount, BlockFactory blockFactory) {
        super(positionCount, blockFactory);
        this.values = values;
        this.block = new $Type$VectorBlock(this);
    }

    @Override
    public $Type$Block asBlock() {
        return block;
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
        try ($Type$Vector.Builder builder = blockFactory.new$Type$VectorBuilder(positions.length)) {
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
    public void close() {
        if (released) {
            throw new IllegalStateException("can't release already released vector [" + this + "]");
        }
        released = true;
        blockFactory.adjustBreaker(-ramBytesUsed() + values.bigArraysRamBytesUsed(), true);
        Releasables.closeExpectNoException(values);
    }
$endif$
}
