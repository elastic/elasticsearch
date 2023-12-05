/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.geo.SpatialPoint;
import org.apache.lucene.util.RamUsageEstimator;

import java.util.Arrays;

import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
/**
 * Vector implementation that stores an array of SpatialPoint values.
 * This class is generated. Do not edit it.
 */
public final class PointArrayVector extends AbstractVector implements PointVector {

    static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(PointArrayVector.class);

    private final SpatialPoint[] values;

    private final PointBlock block;

    public PointArrayVector(SpatialPoint[] values, int positionCount) {
        this(values, positionCount, BlockFactory.getNonBreakingInstance());
    }

    public PointArrayVector(SpatialPoint[] values, int positionCount, BlockFactory blockFactory) {
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
        return values[position];
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
    public PointVector filter(int... positions) {
        try (PointVector.Builder builder = blockFactory().newPointVectorBuilder(positions.length)) {
            for (int pos : positions) {
                builder.appendPoint(values[pos]);
            }
            return builder.build();
        }
    }

    public static long ramBytesEstimated(SpatialPoint[] values) {
        long valuesEstimate = RamUsageEstimator.alignObjectSize((long) NUM_BYTES_ARRAY_HEADER + (long) Long.BYTES * values.length * 2);
        return BASE_RAM_BYTES_USED + valuesEstimate;
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesEstimated(values);
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
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ", values=" + Arrays.toString(values) + ']';
    }

}
