/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

/**
 * Filter vector for BooleanVectors.
 * This class is generated. Do not edit it.
 */
public final class FilterBooleanVector extends AbstractFilterVector implements BooleanVector {

    private final BooleanVector vector;

    FilterBooleanVector(BooleanVector vector, int... positions) {
        super(positions);
        this.vector = vector;
    }

    @Override
    public boolean getBoolean(int position) {
        return vector.getBoolean(mapPosition(position));
    }

    @Override
    public BooleanBlock asBlock() {
        return new BooleanVectorBlock(this);
    }

    @Override
    public ElementType elementType() {
        return ElementType.BOOLEAN;
    }

    @Override
    public boolean isConstant() {
        return vector.isConstant();
    }

    @Override
    public BooleanVector filter(int... positions) {
        return new FilterBooleanVector(this, positions);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof BooleanVector that) {
            return BooleanVector.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return BooleanVector.hash(this);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName());
        sb.append("[positions=" + getPositionCount() + ", values=[");
        appendValues(sb);
        sb.append("]]");
        return sb.toString();
    }

    private void appendValues(StringBuilder sb) {
        final int positions = getPositionCount();
        for (int i = 0; i < positions; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(getBoolean(i));
        }
    }
}
