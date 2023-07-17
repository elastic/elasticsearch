/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.core.Releasable;

/**
 * Vector implementation that defers to an enclosed BooleanArray.
 * This class is generated. Do not edit it.
 */
public final class BooleanBigArrayVector extends AbstractVector implements BooleanVector, Releasable {

    private final BitArray values;

    public BooleanBigArrayVector(BitArray values, int positionCount) {
        super(positionCount);
        this.values = values;
    }

    @Override
    public BooleanBlock asBlock() {
        return new BooleanVectorBlock(this);
    }

    @Override
    public boolean getBoolean(int position) {
        return values.get(position);
    }

    @Override
    public ElementType elementType() {
        return ElementType.BOOLEAN;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public BooleanVector filter(int... positions) {
        return new FilterBooleanVector(this, positions);
    }

    @Override
    public void close() {
        values.close();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof BooleanVector that) {
            return BooleanVector.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return BooleanVector.hash(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ", values=" + values + ']';
    }
}
