/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.core.Releasable;

/**
 * Vector implementation that defers to an enclosed LongArray.
 * This class is generated. Do not edit it.
 */
public final class LongBigArrayVector extends AbstractVector implements LongVector, Releasable {

    private final LongArray values;

    private boolean closed;

    public LongBigArrayVector(LongArray values, int positionCount) {
        super(positionCount);
        this.values = values;
    }

    @Override
    public LongBlock asBlock() {
        return new LongVectorBlock(this);
    }

    @Override
    public long getLong(int position) {
        return values.get(position);
    }

    @Override
    public ElementType elementType() {
        return ElementType.LONG;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public LongVector filter(int... positions) {
        return new FilterLongVector(this, positions);
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        values.close();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof LongVector that) {
            return LongVector.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return LongVector.hash(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ", values=" + values + ']';
    }
}
