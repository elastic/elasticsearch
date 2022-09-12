/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute.data;

import java.util.Arrays;

/**
 * Block implementation that stores a list of double values
 */
public class DoubleBlock extends Block {

    private final double[] values;

    public DoubleBlock(double[] values, int positionCount) {
        super(positionCount);
        this.values = values;
    }

    @Override
    public double getDouble(int position) {
        return values[checkPosition(position)];
    }

    @Override
    public String toString() {
        return "DoubleBlock{" + "values=" + Arrays.toString(values) + '}';
    }

    private int checkPosition(int position) {
        if (position < 0 || position > getPositionCount()) {
            throw new IllegalArgumentException("illegal position, " + position + ", position count:" + getPositionCount());
        }
        return position;
    }
}
