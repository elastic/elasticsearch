/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.geo.SpatialPoint;
import org.elasticsearch.core.Releasables;

/**
 * Block view of a PointVector.
 * This class is generated. Do not edit it.
 */
public final class PointVectorBlock extends AbstractVectorBlock implements PointBlock {

    private final PointVector vector;

    /**
     * @param vector considered owned by the current block; must not be used in any other {@code Block}
     */
    PointVectorBlock(PointVector vector) {
        super(vector.getPositionCount(), vector.blockFactory());
        this.vector = vector;
    }

    @Override
    public PointVector asVector() {
        return vector;
    }

    @Override
    public SpatialPoint getPoint(int valueIndex) {
        return vector.getPoint(valueIndex);
    }

    @Override
    public int getTotalValueCount() {
        return vector.getPositionCount();
    }

    @Override
    public ElementType elementType() {
        return vector.elementType();
    }

    @Override
    public PointBlock filter(int... positions) {
        return vector.filter(positions).asBlock();
    }

    @Override
    public long ramBytesUsed() {
        return vector.ramBytesUsed();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof PointBlock that) {
            return PointBlock.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return PointBlock.hash(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[vector=" + vector + "]";
    }

    @Override
    public boolean isReleased() {
        return super.isReleased() || vector.isReleased();
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
}
