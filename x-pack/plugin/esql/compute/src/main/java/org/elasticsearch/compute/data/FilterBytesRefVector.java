/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;

public final class FilterBytesRefVector extends AbstractFilterVector implements BytesRefVector {

    private final BytesRefVector vector;

    FilterBytesRefVector(BytesRefVector vector, int... positions) {
        super(positions);
        this.vector = vector;
    }

    @Override
    public BytesRef getBytesRef(int position, BytesRef spare) { // diff, spare
        return vector.getBytesRef(mapPosition(position), spare);
    }

    @Override
    public BytesRefBlock asBlock() {
        return new BytesRefVectorBlock(this);
    }

    @Override
    public ElementType elementType() {
        return vector.elementType();
    }

    @Override
    public boolean isConstant() {
        return vector.isConstant();
    }

    @Override
    public BytesRefVector filter(int... positions) {
        return new FilterBytesRefVector(this, positions);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[vector=" + vector + "]";
    }
}
