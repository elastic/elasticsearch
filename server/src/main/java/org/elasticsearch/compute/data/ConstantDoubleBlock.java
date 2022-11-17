/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.data;

/**
 * Block implementation that stores a constant double value.
 */
public final class ConstantDoubleBlock extends Block {

    private final double value;

    public ConstantDoubleBlock(double value, int positionCount) {
        super(positionCount);
        this.value = value;
    }

    @Override
    public double getDouble(int position) {
        assert assertPosition(position);
        return value;
    }

    @Override
    public Object getObject(int position) {
        return getDouble(position);
    }

    @Override
    public Block filter(int... positions) {
        return new ConstantDoubleBlock(value, positions.length);
    }

    @Override
    public String toString() {
        return "ConstantDoubleBlock{positions=" + getPositionCount() + ", value=" + value + '}';
    }
}
