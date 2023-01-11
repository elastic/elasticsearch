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
public final class BytesRefVector extends AbstractVector {

    private final BytesRefArray bytesRefArray;

    public BytesRefVector(BytesRefArray bytesRefArray, int positionCount) {
        super(positionCount);
        this.bytesRefArray = bytesRefArray;
    }

    @Override
    public BytesRef getBytesRef(int position, BytesRef spare) {
        return bytesRefArray.get(position, spare);
    }

    @Override
    public Object getObject(int position) {
        return getBytesRef(position, new BytesRef());
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
    public String toString() {
        return "BytesRefVector[positions=" + getPositionCount() + "]";
    }
}
