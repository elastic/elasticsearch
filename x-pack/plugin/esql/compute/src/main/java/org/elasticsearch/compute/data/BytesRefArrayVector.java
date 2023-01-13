/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BytesRefArray;

/**
 * Vector implementation that stores an array of BytesRef values.
 */
public final class BytesRefArrayVector extends AbstractVector implements BytesRefVector {

    private final BytesRefArray values;  // this is diff, no []

    public BytesRefArrayVector(BytesRefArray values, int positionCount) {  // this is diff, no []
        super(positionCount);
        this.values = values;
    }

    @Override
    public BytesRefBlock asBlock() {
        return new BytesRefVectorBlock(this);
    }

    @Override
    public BytesRef getBytesRef(int position, BytesRef holder) {  // this is diff, spare
        return values.get(position, holder);
    }

    @Override
    public ElementType elementType() {
        return ElementType.BYTES_REF;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public BytesRefVector filter(int... positions) {
        return new FilterBytesRefVector(this, positions);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + "]";  // this toString is diff
    }
}
