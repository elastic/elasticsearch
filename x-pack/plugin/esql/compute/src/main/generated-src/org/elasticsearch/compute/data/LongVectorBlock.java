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
 * Block view of a {@link LongVector}. Cannot represent multi-values or nulls.
 * This class is generated. Edit {@code X-VectorBlock.java.st} instead.
 */
public final class LongVectorBlock extends AbstractVectorBlock implements LongBlock {

    private final LongVector vector;

    /**
     * @param vector considered owned by the current block; must not be used in any other {@code Block}
     */
    LongVectorBlock(LongVector vector) {
        this.vector = vector;
    }

    @Override
    public LongVector asVector() {
        return vector;
    }

    @Override
    public long getLong(int valueIndex) {
        return vector.getLong(valueIndex);
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
    public LongBlock filter(int... positions) {
        return vector.filter(positions).asBlock();
    }

    @Override
    public LongBlock keepMask(BooleanVector mask) {
        return vector.keepMask(mask);
    }

    @Override
    public ReleasableIterator<? extends LongBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        return vector.lookup(positions, targetBlockSize);
    }

    @Override
    public LongBlock expand() {
        incRef();
        return this;
    }

    @Override
    public long ramBytesUsed() {
        return vector.ramBytesUsed();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof LongBlock that) {
            return LongBlock.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return LongBlock.hash(this);
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
