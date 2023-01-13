/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

public final class LongVectorBlock extends AbstractVectorBlock implements LongBlock {

    private final LongVector vector;

    LongVectorBlock(LongVector vector) {
        super(vector.getPositionCount());
        this.vector = vector;
    }

    @Override
    public long getLong(int valueIndex) {
        return vector.getLong(valueIndex);
    }

    @Override
    public Object getObject(int position) {
        return getLong(position);
    }

    @Override
    public LongVector asVector() {
        return vector;
    }

    @Override
    public int getTotalValueCount() {
        return vector.getPositionCount();
    }

    @Override
    public ElementType elementType() {
        return ElementType.LONG;
    }

    @Override
    public LongBlock getRow(int position) {
        return filter(position);
    }

    @Override
    public LongBlock filter(int... positions) {
        return new FilterLongVector(vector, positions).asBlock();
    }
}
