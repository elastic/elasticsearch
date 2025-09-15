/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

// begin generated imports
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
// end generated imports

/**
 * Block view of a {@link BooleanVector}. Cannot represent multi-values or nulls.
 * This class is generated. Edit {@code X-VectorBlock.java.st} instead.
 */
public final class BooleanVectorBlock extends AbstractVectorBlock implements BooleanBlock {

    private final BooleanVector vector;

    /**
     * @param vector considered owned by the current block; must not be used in any other {@code Block}
     */
    BooleanVectorBlock(BooleanVector vector) {
        this.vector = vector;
    }

    @Override
    public BooleanVector asVector() {
        return vector;
    }

    @Override
    public ToMask toMask() {
        vector.incRef();
        return new ToMask(vector, false);
    }

    @Override
    public boolean getBoolean(int valueIndex) {
        return vector.getBoolean(valueIndex);
    }

    @Override
    public int getPositionCount() {
        return vector.getPositionCount();
    }

    @Override
    public ElementType elementType() {
        return vector.elementType();
    }

    @Override
    public BooleanBlock filter(int... positions) {
        return vector.filter(positions).asBlock();
    }

    @Override
    public BooleanBlock keepMask(BooleanVector mask) {
        return vector.keepMask(mask);
    }

    @Override
    public ReleasableIterator<? extends BooleanBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        return vector.lookup(positions, targetBlockSize);
    }

    @Override
    public BooleanBlock expand() {
        incRef();
        return this;
    }

    @Override
    public long ramBytesUsed() {
        return vector.ramBytesUsed();
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
        return getClass().getSimpleName() + "[vector=" + vector + "]";
    }

    @Override
    public void closeInternal() {
        assert (vector.isReleased() == false) : "can't release block [" + this + "] containing already released vector";
        Releasables.closeExpectNoException(vector);
    }

    @Override
    public void allowPassingToDifferentDriver() {
        vector.allowPassingToDifferentDriver();
    }

    @Override
    public BlockFactory blockFactory() {
        return vector.blockFactory();
    }
}
