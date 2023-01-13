/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

final class FilterIntBlock extends AbstractFilterBlock implements IntBlock {

    private final IntBlock intBlock;

    FilterIntBlock(IntBlock block, int... positions) {
        super(block, positions);
        this.intBlock = block;
    }

    @Override
    public IntVector asVector() {
        return null;
    }

    @Override
    public int getInt(int valueIndex) {
        return intBlock.getInt(mapPosition(valueIndex));
    }

    @Override
    public Object getObject(int position) {
        return getInt(position);
    }

    @Override
    public IntBlock getRow(int position) {
        return filter(position);
    }

    @Override
    public IntBlock filter(int... positions) {
        return new FilterIntBlock(this, positions);
    }

    @Override
    public LongBlock asLongBlock() {
        LongBlock lb = intBlock.asLongBlock();
        return new FilterLongBlock(lb, positions);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[block=" + intBlock + "]";
    }
}
