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
import org.elasticsearch.common.unit.ByteSizeValue;
$if(BytesRef)$
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

$else$
import org.elasticsearch.core.ReleasableIterator;

$endif$
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

    @Override
    public $Type$Block keepMask(BooleanVector mask) {
        if (getPositionCount() == 0) {
            incRef();
            return new $Type$VectorBlock(this);
        }
        if (mask.isConstant()) {
            if (mask.getBoolean(0)) {
                incRef();
                return new $Type$VectorBlock(this);
            }
            return ($Type$Block) blockFactory().newConstantNullBlock(getPositionCount());
        }
$if(BytesRef)$
        IntBlock ordinals = null;
        BytesRefVector bytes = null;
        try {
            try (IntVector unmaskedOrdinals = blockFactory().newConstantIntVector(0, getPositionCount())) {
                ordinals = unmaskedOrdinals.keepMask(mask);
            }
            bytes = blockFactory().newConstantBytesRefVector(value, getPositionCount());
            OrdinalBytesRefBlock result = new OrdinalBytesRefBlock(ordinals, bytes);
            ordinals = null;
            bytes = null;
            return result;
        } finally {
            Releasables.close(ordinals, bytes);
        }
$else$
        try ($Type$Block.Builder builder = blockFactory().new$Type$BlockBuilder(getPositionCount())) {
            // TODO if X-ArrayBlock used BooleanVector for it's null mask then we could shuffle references here.
            for (int p = 0; p < getPositionCount(); p++) {
                if (mask.getBoolean(p)) {
                    builder.append$Type$(value);
                } else {
                    builder.appendNull();
                }
            }
            return builder.build();
        }
$endif$
    }

    @Override
    public ReleasableIterator<$Type$Block> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        if (positions.getPositionCount() == 0) {
            return ReleasableIterator.empty();
        }
        IntVector positionsVector = positions.asVector();
        if (positionsVector == null) {
            return new $Type$Lookup(asBlock(), positions, targetBlockSize);
        }
        int min = positionsVector.min();
        if (min < 0) {
            throw new IllegalArgumentException("invalid position [" + min + "]");
        }
        if (min > getPositionCount()) {
            return ReleasableIterator.single(($Type$Block) positions.blockFactory().newConstantNullBlock(positions.getPositionCount()));
        }
        if (positionsVector.max() < getPositionCount()) {
            return ReleasableIterator.single(positions.blockFactory().newConstant$Type$BlockWith(value, positions.getPositionCount()));
        }
        return new $Type$Lookup(asBlock(), positions, targetBlockSize);
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
