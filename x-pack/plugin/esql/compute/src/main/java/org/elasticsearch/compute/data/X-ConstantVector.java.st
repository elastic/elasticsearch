/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

$if(BytesRef)$
import org.apache.lucene.util.BytesRef;
$endif$
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Vector implementation that stores a constant $type$ value.
 * This class is generated. Do not edit it.
 */
public final class Constant$Type$Vector extends AbstractVector implements $Type$Vector {

$if(BytesRef)$
    static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ConstantBytesRefVector.class) + RamUsageEstimator
        .shallowSizeOfInstance(BytesRef.class);
$else$
    static final long RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Constant$Type$Vector.class);
$endif$

    private final $type$ value;

    private final $Type$Block block;

    public Constant$Type$Vector($type$ value, int positionCount) {
        this(value, positionCount, BlockFactory.getNonBreakingInstance());
    }

    public Constant$Type$Vector($type$ value, int positionCount, BlockFactory blockFactory) {
        super(positionCount, blockFactory);
        this.value = value;
        this.block = new $Type$VectorBlock(this);
    }

    @Override
$if(BytesRef)$
    public BytesRef getBytesRef(int position, BytesRef ignore) {
$else$
    public $type$ get$Type$(int position) {
$endif$
        return value;
    }

    @Override
    public $Type$Block asBlock() {
        return block;
    }

    @Override
    public $Type$Vector filter(int... positions) {
        return new Constant$Type$Vector(value, positions.length);
    }

    @Override
    public ElementType elementType() {
        return ElementType.$TYPE$;
    }

    @Override
    public boolean isConstant() {
        return true;
    }

$if(BytesRef)$
    public static long ramBytesUsed(BytesRef value) {
        return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(value.bytes);
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesUsed(value);
    }

$else$
    @Override
    public long ramBytesUsed() {
        return RAM_BYTES_USED;
    }

$endif$
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

    public String toString() {
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ", value=" + value + ']';
    }

    @Override
    public void close() {
        if (released) {
            throw new IllegalStateException("can't release already released vector [" + this + "]");
        }
        released = true;
        blockFactory.adjustBreaker(-ramBytesUsed(), true);
    }
}
