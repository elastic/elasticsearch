/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.core.Releasable;

/**
 * Vector implementation that defers to an enclosed DoubleArray.
 * This class is generated. Do not edit it.
 */
public final class DoubleBigArrayVector extends AbstractVector implements DoubleVector, Releasable {

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DoubleBigArrayVector.class);

    private final DoubleArray values;

    private final DoubleBlock block;

    public DoubleBigArrayVector(DoubleArray values, int positionCount) {
        this(values, positionCount, BlockFactory.getNonBreakingInstance());
    }

    public DoubleBigArrayVector(DoubleArray values, int positionCount, BlockFactory blockFactory) {
        super(positionCount, blockFactory);
        this.values = values;
        this.block = new DoubleVectorBlock(this);
    }

    @Override
    public DoubleBlock asBlock() {
        return block;
    }

    @Override
    public double getDouble(int position) {
        return values.get(position);
    }

    @Override
    public ElementType elementType() {
        return ElementType.DOUBLE;
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
    public DoubleVector filter(int... positions) {
        final DoubleArray filtered = blockFactory.bigArrays().newDoubleArray(positions.length, true);
        for (int i = 0; i < positions.length; i++) {
            filtered.set(i, values.get(positions[i]));
        }
        return new DoubleBigArrayVector(filtered, positions.length, blockFactory);
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
        if (obj instanceof DoubleVector that) {
            return DoubleVector.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return DoubleVector.hash(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ", values=" + values + ']';
    }
}
