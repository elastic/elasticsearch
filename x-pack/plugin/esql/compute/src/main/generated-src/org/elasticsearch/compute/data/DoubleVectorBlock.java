/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

/**
 * Block view of a {@link DoubleVector}. Cannot represent multi-values or nulls.
 * This class is generated. Do not edit it.
 */
public final class DoubleVectorBlock extends AbstractVectorBlock implements DoubleBlock {

    private final DoubleVector vector;

    /**
     * @param vector considered owned by the current block; must not be used in any other {@code Block}
     */
    DoubleVectorBlock(DoubleVector vector) {
        this.vector = vector;
    }

    @Override
    public DoubleVector asVector() {
        return vector;
    }

    @Override
    public double getDouble(int valueIndex) {
        return vector.getDouble(valueIndex);
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
    public DoubleBlock filter(int... positions) {
        return vector.filter(positions).asBlock();
    }

    @Override
    public DoubleBlock keepMask(BooleanVector mask) {
        return vector.keepMask(mask);
    }

    @Override
    public ReleasableIterator<? extends DoubleBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        return vector.lookup(positions, targetBlockSize);
    }

    @Override
    public DoubleBlock expand() {
        incRef();
        return this;
    }

    @Override
    public long ramBytesUsed() {
        return vector.ramBytesUsed();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DoubleBlock that) {
            return DoubleBlock.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return DoubleBlock.hash(this);
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
