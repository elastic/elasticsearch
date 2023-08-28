/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

/**
 * Block view of a BooleanVector.
 * This class is generated. Do not edit it.
 */
public final class BooleanVectorBlock extends AbstractVectorBlock implements BooleanBlock {

    private final BooleanVector vector;

    BooleanVectorBlock(BooleanVector vector) {
        super(vector.getPositionCount());
        this.vector = vector;
    }

    @Override
    public BooleanVector asVector() {
        return vector;
    }

    @Override
    public boolean getBoolean(int valueIndex) {
        return vector.getBoolean(valueIndex);
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
    public BooleanBlock filter(int... positions) {
        return new FilterBooleanVector(vector, positions).asBlock();
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
}
