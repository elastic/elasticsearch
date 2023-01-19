/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;

/**
 * Filter vector for BytesRefVectors.
 * This class is generated. Do not edit it.
 */
public final class FilterBytesRefVector extends AbstractFilterVector implements BytesRefVector {

    private final BytesRefVector vector;

    FilterBytesRefVector(BytesRefVector vector, int... positions) {
        super(positions);
        this.vector = vector;
    }

    @Override
    public BytesRef getBytesRef(int position, BytesRef dest) {
        return vector.getBytesRef(mapPosition(position), dest);
    }

    @Override
    public BytesRefBlock asBlock() {
        return new BytesRefVectorBlock(this);
    }

    @Override
    public ElementType elementType() {
        return ElementType.BYTES_REF;
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
