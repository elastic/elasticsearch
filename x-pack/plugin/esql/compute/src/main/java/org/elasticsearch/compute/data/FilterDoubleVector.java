/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

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
    public String toString() {
        return getClass().getSimpleName() + "[vector=" + vector + "]";
    }
}
