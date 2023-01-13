/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

final class FilterDoubleBlock extends AbstractFilterBlock implements DoubleBlock {

    private final DoubleBlock doubleBlock;

    FilterDoubleBlock(DoubleBlock block, int... positions) {
        super(block, positions);
        this.doubleBlock = block;
    }

    @Override
    public DoubleVector asVector() {
        return null;
    }

    @Override
    public double getDouble(int valueIndex) {
        return doubleBlock.getDouble(mapPosition(valueIndex));
    }

    @Override
    public Object getObject(int position) {
        return getDouble(position);
    }

    @Override
    public ElementType elementType() {
        return doubleBlock.elementType();
    }

    @Override
    public DoubleBlock getRow(int position) {
        return filter(position);
    }

    @Override
    public DoubleBlock filter(int... positions) {
        return new FilterDoubleBlock(this, positions);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[block=" + doubleBlock + "]";
    }
}
