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
 * Block implementation that stores an array of {@link org.apache.lucene.util.BytesRef}.
 */
public final class BytesRefArrayBlock extends AbstractVector {

    private final BytesRefArray bytes;

    public BytesRefArrayBlock(int positionCount, BytesRefArray bytes) {
        super(positionCount);
        assert bytes.size() == positionCount : bytes.size() + " != " + positionCount;
        this.bytes = bytes;
    }

    @Override
    public BytesRef getBytesRef(int position, BytesRef spare) {
        return bytes.get(position, spare);
    }

    @Override
    public Object getObject(int position) {
        return getBytesRef(position, new BytesRef());
    }

    @Override
    public Class<?> elementType() {
        return BytesRef.class;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public String toString() {
        return "BytesRefArrayBlock{positions=" + getPositionCount() + '}';
    }
}
