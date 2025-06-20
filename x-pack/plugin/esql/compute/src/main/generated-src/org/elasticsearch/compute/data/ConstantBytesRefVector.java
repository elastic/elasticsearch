/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

// begin generated imports
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.ReleasableIterator;
// end generated imports

/**
 * Vector implementation that stores a constant BytesRef value.
 * This class is generated. Edit {@code X-ConstantVector.java.st} instead.
 */
final class ConstantBytesRefVector extends AbstractVector implements BytesRefVector {

    static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ConstantBytesRefVector.class) + RamUsageEstimator
        .shallowSizeOfInstance(BytesRef.class);
    private final BytesRef value;

    ConstantBytesRefVector(BytesRef value, int positionCount, BlockFactory blockFactory) {
        super(positionCount, blockFactory);
        this.value = value;
    }

    @Override
    public BytesRef getBytesRef(int position, BytesRef ignore) {
        return value;
    }

    @Override
    public BytesRefBlock asBlock() {
        return new BytesRefVectorBlock(this);
    }

    @Override
    public OrdinalBytesRefVector asOrdinals() {
        return null;
    }

    @Override
    public BytesRefVector filter(int... positions) {
        return blockFactory().newConstantBytesRefVector(value, positions.length);
    }

    @Override
    public BytesRefBlock keepMask(BooleanVector mask) {
        if (getPositionCount() == 0) {
            incRef();
            return new BytesRefVectorBlock(this);
        }
        if (mask.isConstant()) {
            if (mask.getBoolean(0)) {
                incRef();
                return new BytesRefVectorBlock(this);
            }
            return (BytesRefBlock) blockFactory().newConstantNullBlock(getPositionCount());
        }
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
    }

    @Override
    public ReleasableIterator<BytesRefBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        if (positions.getPositionCount() == 0) {
            return ReleasableIterator.empty();
        }
        IntVector positionsVector = positions.asVector();
        if (positionsVector == null) {
            return new BytesRefLookup(asBlock(), positions, targetBlockSize);
        }
        int min = positionsVector.min();
        if (min < 0) {
            throw new IllegalArgumentException("invalid position [" + min + "]");
        }
        if (min > getPositionCount()) {
            return ReleasableIterator.single((BytesRefBlock) positions.blockFactory().newConstantNullBlock(positions.getPositionCount()));
        }
        if (positionsVector.max() < getPositionCount()) {
            return ReleasableIterator.single(positions.blockFactory().newConstantBytesRefBlockWith(value, positions.getPositionCount()));
        }
        return new BytesRefLookup(asBlock(), positions, targetBlockSize);
    }

    @Override
    public ElementType elementType() {
        return ElementType.BYTES_REF;
    }

    @Override
    public boolean isConstant() {
        return true;
    }

    public static long ramBytesUsed(BytesRef value) {
        return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(value.bytes);
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesUsed(value);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof BytesRefVector that) {
            return BytesRefVector.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return BytesRefVector.hash(this);
    }

    public String toString() {
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ", value=" + value + ']';
    }
}
