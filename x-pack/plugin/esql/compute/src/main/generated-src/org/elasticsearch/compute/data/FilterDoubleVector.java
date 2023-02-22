/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

/**
 * Filter vector for DoubleVectors.
 * This class is generated. Do not edit it.
 */
public final class FilterDoubleVector extends AbstractFilterVector implements DoubleVector {

    private final DoubleVector vector;

    FilterDoubleVector(DoubleVector vector, int... positions) {
        super(positions);
        this.vector = vector;
    }

    @Override
    public double getDouble(int position) {
        return vector.getDouble(mapPosition(position));
    }

    @Override
    public DoubleBlock asBlock() {
        return new DoubleVectorBlock(this);
    }

    @Override
    public ElementType elementType() {
        return ElementType.DOUBLE;
    }

    @Override
    public boolean isConstant() {
        return vector.isConstant();
    }

    @Override
    public DoubleVector filter(int... positions) {
        return new FilterDoubleVector(this, positions);
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
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName());
        sb.append("[positions=" + getPositionCount() + ", values=[");
        appendValues(sb);
        sb.append("]]");
        return sb.toString();
    }

    private void appendValues(StringBuilder sb) {
        final int positionsIndex = getPositionCount() - 1;
        for (int i = 0;; i++) {
            sb.append(getDouble(i));
            if (i == positionsIndex) {
                return;
            }
            sb.append(", ");
        }
    }
}
