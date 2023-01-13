/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

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
    public String toString() {
        return getClass().getSimpleName() + "[vector=" + vector + "]";
    }
}
