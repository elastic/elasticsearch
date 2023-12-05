/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.geo.SpatialPoint;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.core.Releasable;

/**
 * Vector implementation that defers to an enclosed PointArray.
 * This class is generated. Do not edit it.
 */
public final class PointBigArrayVector extends AbstractVector implements PointVector, Releasable {

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(PointBigArrayVector.class);

    private final ObjectArray<SpatialPoint> values;

    private final PointBlock block;

    public PointBigArrayVector(ObjectArray<SpatialPoint> values, int positionCount) {
        this(values, positionCount, BlockFactory.getNonBreakingInstance());
    }

    public PointBigArrayVector(ObjectArray<SpatialPoint> values, int positionCount, BlockFactory blockFactory) {
        super(positionCount, blockFactory);
        this.values = values;
        this.block = new PointVectorBlock(this);
    }

    @Override
    public PointBlock asBlock() {
        return block;
    }

    @Override
    public SpatialPoint getPoint(int position) {
        return values.get(position);
    }

    @Override
    public ElementType elementType() {
        return ElementType.POINT;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public long ramBytesUsed() {
        return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(values);
    }

    @Override
    public PointVector filter(int... positions) {
        var blockFactory = blockFactory();
        final ObjectArray<SpatialPoint> filtered = blockFactory.bigArrays().newObjectArray(positions.length);
        for (int i = 0; i < positions.length; i++) {
            filtered.set(i, values.get(positions[i]));
        }
        return new PointBigArrayVector(filtered, positions.length, blockFactory);
    }

    @Override
    public void close() {
        if (released) {
            throw new IllegalStateException("can't release already released vector [" + this + "]");
        }
        released = true;
        values.close();
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

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ", values=" + values + ']';
    }
}
