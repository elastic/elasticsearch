/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

/**
 * Block view of a IntVector.
 * This class is generated. Do not edit it.
 */
public final class IntVectorBlock extends AbstractVectorBlock implements IntBlock {

    private final IntVector vector;

    IntVectorBlock(IntVector vector) {
        super(vector.getPositionCount());
        this.vector = vector;
    }

    @Override
    public IntVector asVector() {
        return vector;
    }

    @Override
    public int getInt(int valueIndex) {
        return vector.getInt(valueIndex);
    }

    @Override
    public int getTotalValueCount() {
        return vector.getPositionCount();
    }

    @Override
    public ElementType elementType() {
        return vector.elementType();
    }

    public LongBlock asLongBlock() {  // copy rather than view, for now
        final int positions = getPositionCount();
        long[] longValues = new long[positions];
        for (int i = 0; i < positions; i++) {
            longValues[i] = vector.getInt(i);
        }
        return new LongArrayVector(longValues, getPositionCount()).asBlock();
    }

    @Override
    public IntBlock getRow(int position) {
        return filter(position);
    }

    @Override
    public IntBlock filter(int... positions) {
        return new FilterIntVector(vector, positions).asBlock();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof IntBlock that) {
            return IntBlock.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return IntBlock.hash(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[vector=" + vector + "]";
    }
}
