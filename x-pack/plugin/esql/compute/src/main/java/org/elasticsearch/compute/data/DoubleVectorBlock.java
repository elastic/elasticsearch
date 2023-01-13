/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

public final class DoubleVectorBlock extends AbstractVectorBlock implements DoubleBlock {

    private final DoubleVector vector;

    DoubleVectorBlock(DoubleVector vector) {
        super(vector.getPositionCount());
        this.vector = vector;
    }

    @Override
    public DoubleVector asVector() {
        return vector;
    }

    @Override
    public double getDouble(int valueIndex) {
        return vector.getDouble(valueIndex);
    }

    @Override
    public Object getObject(int position) {
        return getDouble(position);
    }

    @Override
    public int getTotalValueCount() {
        return vector.getPositionCount();
    }

    @Override
    public ElementType elementType() {
        return ElementType.DOUBLE;
    }

    @Override
    public DoubleBlock getRow(int position) {
        return filter(position);
    }

    @Override
    public DoubleBlock filter(int... positions) {
        return new FilterDoubleVector(vector, positions).asBlock();
    }
}
