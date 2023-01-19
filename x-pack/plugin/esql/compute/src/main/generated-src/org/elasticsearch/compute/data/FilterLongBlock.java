/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

/**
 * Filter block for LongBlocks.
 * This class is generated. Do not edit it.
 */
final class FilterLongBlock extends AbstractFilterBlock implements LongBlock {

    private final LongBlock block;

    FilterLongBlock(LongBlock block, int... positions) {
        super(block, positions);
        this.block = block;
    }

    @Override
    public LongVector asVector() {
        return null;
    }

    @Override
    public long getLong(int valueIndex) {
        return block.getLong(mapPosition(valueIndex));
    }

    @Override
    public Object getObject(int position) {
        return getLong(position);
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
        return new FilterLongBlock(this, positions);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[block=" + block + "]";
    }
}
