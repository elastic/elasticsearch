/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.data;

import java.util.BitSet;

/**
 * Block implementation that stores a constant long value.
 */
public final class ConstantLongBlock extends Block {

    private final long value;

    public ConstantLongBlock(long value, int positionCount) {
        super(positionCount);
        this.value = value;
    }

    public ConstantLongBlock(long value, int positionCount, BitSet nulls) {
        super(positionCount, nulls);
        this.value = value;
    }

    @Override
    public long getLong(int position) {
        assert assertPosition(position);
        assert isNull(position) == false;
        return value;
    }

    @Override
    public double getDouble(int position) {
        assert assertPosition(position);
        return value;  // Widening primitive conversions, no loss of precision
    }

    @Override
    public Object getObject(int position) {
        return getLong(position);
    }

    @Override
    public Block filter(int... positions) {
        return new ConstantLongBlock(value, positions.length);
    }

    @Override
    public String toString() {
        return "ConstantLongBlock{positions=" + getPositionCount() + ", value=" + value + '}';
    }
}
