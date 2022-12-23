/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;

/**
 * A dense Vector of single values.
 */
abstract class AbstractVector implements Vector {

    private final int positionCount;

    /**
     * @param positionCount the number of values in this vector
     */
    protected AbstractVector(int positionCount) {
        this.positionCount = positionCount;
    }

    @Override
    public Block asBlock() {
        return new VectorBlock(this);
    }

    public final int getPositionCount() {
        return positionCount;
    }

    @Override
    public int getInt(int position) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public long getLong(int position) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public double getDouble(int position) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public BytesRef getBytesRef(int position, BytesRef spare) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public Object getObject(int position) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public final Vector getRow(int position) {
        return filter(position);
    }

    public Vector filter(int... positions) {
        return new FilterVector(this, positions);
    }
}
