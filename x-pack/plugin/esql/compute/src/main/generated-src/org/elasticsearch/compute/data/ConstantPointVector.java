/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.geo.SpatialPoint;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Vector implementation that stores a constant SpatialPoint value.
 * This class is generated. Do not edit it.
 */
public final class ConstantPointVector extends AbstractVector implements PointVector {

    static final long RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ConstantPointVector.class);

    private final SpatialPoint value;

    private final PointBlock block;

    public ConstantPointVector(SpatialPoint value, int positionCount) {
        this(value, positionCount, BlockFactory.getNonBreakingInstance());
    }

    public ConstantPointVector(SpatialPoint value, int positionCount, BlockFactory blockFactory) {
        super(positionCount, blockFactory);
        this.value = value;
        this.block = new PointVectorBlock(this);
    }

    @Override
    public SpatialPoint getPoint(int position) {
        return value;
    }

    @Override
    public PointBlock asBlock() {
        return block;
    }

    @Override
    public PointVector filter(int... positions) {
        return new ConstantPointVector(value, positions.length);
    }

    @Override
    public ElementType elementType() {
        return ElementType.POINT;
    }

    @Override
    public boolean isConstant() {
        return true;
    }

    @Override
    public long ramBytesUsed() {
        return RAM_BYTES_USED;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof PointVector that) {
            return PointVector.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return PointVector.hash(this);
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
        blockFactory().adjustBreaker(-ramBytesUsed(), true);
    }
}
