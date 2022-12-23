/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;

import java.util.Optional;

/**
 * A Block view of a Vector.
 */
final class VectorBlock extends AbstractBlock {

    private final Vector vector;

    VectorBlock(Vector vector) {
        super(vector.getPositionCount());
        this.vector = vector;
    }

    @Override
    public Optional<Vector> asVector() {
        return Optional.of(vector);
    }

    @Override
    public int getTotalValueCount() {
        return vector.getPositionCount();
    }

    @Override
    public int getFirstValueIndex(int position) {
        return position;
    }

    public int getValueCount(int position) {
        return 1;
    }

    @Override
    public int getInt(int valueIndex) {
        return vector.getInt(valueIndex);
    }

    @Override
    public long getLong(int valueIndex) {
        return vector.getLong(valueIndex);
    }

    @Override
    public double getDouble(int valueIndex) {
        return vector.getDouble(valueIndex);
    }

    @Override
    public BytesRef getBytesRef(int valueIndex, BytesRef spare) {
        return vector.getBytesRef(valueIndex, spare);
    }

    @Override
    public Object getObject(int valueIndex) {
        return vector.getObject(valueIndex);
    }

    @Override
    public Class<?> elementType() {
        return vector.elementType();
    }

    @Override
    public boolean isNull(int position) {
        return false;
    }

    @Override
    public int nullValuesCount() {
        return 0;
    }

    @Override
    public boolean mayHaveNulls() {
        return false;
    }

    @Override
    public boolean areAllValuesNull() {
        return false;
    }

    @Override
    public Block getRow(int position) {
        return filter(position);
    }

    @Override
    public Block filter(int... positions) {
        return new FilterVector(vector, positions).asBlock();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + vector + "]";
    }
}
