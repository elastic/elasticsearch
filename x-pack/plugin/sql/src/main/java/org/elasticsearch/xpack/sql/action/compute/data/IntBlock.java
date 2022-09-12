/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute.data;

import java.util.Arrays;

/**
 * Block implementation that stores a list of integers
 */
public class IntBlock extends Block {
    private final int[] values;

    public IntBlock(int[] values, int positionCount) {
        super(positionCount);
        this.values = values;
    }

    @Override
    public int getInt(int position) {
        return values[position];
    }

    @Override
    public double getDouble(int position) {
        return getInt(position);  // Widening primitive conversions, no loss of precision
    }

    @Override
    public String toString() {
        return "IntBlock{" + "values=" + Arrays.toString(values) + '}';
    }
}
