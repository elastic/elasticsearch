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
final class Constant$Type$Vector extends AbstractVector implements $Type$Vector {

$if(BytesRef)$
    static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ConstantBytesRefVector.class) + RamUsageEstimator
        .shallowSizeOfInstance(BytesRef.class);
$else$
    static final long RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Constant$Type$Vector.class);
$endif$

    private final $type$ value;

    Constant$Type$Vector($type$ value, int positionCount, BlockFactory blockFactory) {
        super(positionCount, blockFactory);
        this.value = value;
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
        return new $Type$VectorBlock(this);
    }

$if(BytesRef)$
    @Override
    public OrdinalBytesRefVector asOrdinals() {
        return null;
    }
$endif$

    @Override
    public $Type$Vector filter(int... positions) {
        return blockFactory().newConstant$Type$Vector(value, positions.length);
    }

$if(int)$
    /**
     * The minimum value in the block.
     */
    @Override
    public int min() {
        return value;
    }

    /**
     * The maximum value in the block.
     */
    @Override
    public int max() {
        return value;
    }
$endif$

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
}
