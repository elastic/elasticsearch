/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.core.Releasable;

/**
 * Vector implementation that defers to an enclosed IntArray.
 * This class is generated. Do not edit it.
 */
public final class IntBigArrayVector extends AbstractVector implements IntVector, Releasable {

    private final IntArray values;

    public IntBigArrayVector(IntArray values, int positionCount) {
        super(positionCount);
        this.values = values;
    }

    @Override
    public IntBlock asBlock() {
        return new IntVectorBlock(this);
    }

    @Override
    public int getInt(int position) {
        return values.get(position);
    }

    @Override
    public ElementType elementType() {
        return ElementType.INT;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public IntVector filter(int... positions) {
        return new FilterIntVector(this, positions);
    }

    @Override
    public void close() {
        values.close();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof IntVector that) {
            return IntVector.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return IntVector.hash(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ", values=" + values + ']';
    }
}
