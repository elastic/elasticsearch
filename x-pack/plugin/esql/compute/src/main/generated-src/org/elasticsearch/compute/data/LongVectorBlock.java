/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.core.Releasables;

/**
 * Block view of a LongVector.
 * This class is generated. Do not edit it.
 */
public final class LongVectorBlock extends AbstractVectorBlock implements LongBlock {

    private static final long RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(LongVectorBlock.class);

    private final LongVector vector;

    LongVectorBlock(LongVector vector) {
        super(vector.getPositionCount());
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
    public int getTotalValueCount() {
        return vector.getPositionCount();
    }

    @Override
    public ElementType elementType() {
        return vector.elementType();
    }

    @Override
    public LongBlock filter(int... positions) {
        return new FilterLongVector(vector, positions).asBlock();
    }

    @Override
    public long ramBytesUsed() {
        return RAM_BYTES_USED + RamUsageEstimator.sizeOf(vector);
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
    public void close() {
        Releasables.closeExpectNoException(vector);
    }
}
