/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

/**
 * Filter vector for LongVectors.
 * This class is generated. Do not edit it.
 */
public final class FilterLongVector extends AbstractFilterVector implements LongVector {

    private final LongVector vector;

    FilterLongVector(LongVector vector, int... positions) {
        super(positions);
        this.vector = vector;
    }

    @Override
    public long getLong(int position) {
        return vector.getLong(mapPosition(position));
    }

    @Override
    public LongBlock asBlock() {
        return new LongVectorBlock(this);
    }

    @Override
    public ElementType elementType() {
        return ElementType.LONG;
    }

    @Override
    public boolean isConstant() {
        return vector.isConstant();
    }

    @Override
    public LongVector filter(int... positions) {
        return new FilterLongVector(this, positions);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof LongVector that) {
            return LongVector.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return LongVector.hash(this);
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
            sb.append(getLong(i));
        }
    }
}
