/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;

/**
 * Vector implementation that stores a constant BytesRef value.
 */
public final class ConstantBytesRefVector extends AbstractVector implements BytesRefVector {

    private final BytesRef value;

    ConstantBytesRefVector(BytesRef value, int positionCount) {
        super(positionCount);
        this.value = value;
    }

    @Override
    public BytesRef getBytesRef(int position, BytesRef ignore) {
        return value;
    }

    @Override
    public BytesRefBlock asBlock() {
        return new BytesRefVectorBlock(this);
    }

    @Override
    public BytesRefVector filter(int... positions) {
        return new ConstantBytesRefVector(value, positions.length);
    }

    @Override
    public ElementType elementType() {
        return ElementType.BYTES_REF;
    }

    @Override
    public boolean isConstant() {
        return true;
    }

    public String toString() {
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ", value=" + value + ']';
    }
}
