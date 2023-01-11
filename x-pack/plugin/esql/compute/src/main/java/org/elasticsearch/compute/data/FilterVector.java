/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;

import java.util.Arrays;

/**
 * Wraps another single-value block and only allows access to positions that have not been filtered out.
 *
 * To ensure fast access, the filter is implemented as an array of positions that map positions in the filtered block to positions in the
 * wrapped block.
 */
final class FilterVector extends AbstractVector {

    private final int[] positions;
    private final Vector vector;

    FilterVector(Vector vector, int[] positions) {
        super(positions.length);
        this.positions = positions;
        this.vector = vector;
    }

    @Override
    public int getInt(int position) {
        return vector.getInt(mapPosition(position));
    }

    @Override
    public long getLong(int position) {
        return vector.getLong(mapPosition(position));
    }

    @Override
    public double getDouble(int position) {
        return vector.getDouble(mapPosition(position));
    }

    @Override
    public Object getObject(int position) {
        return vector.getObject(mapPosition(position));
    }

    @Override
    public ElementType elementType() {
        return vector.elementType();
    }

    @Override
    public BytesRef getBytesRef(int position, BytesRef spare) {
        return vector.getBytesRef(mapPosition(position), spare);
    }

    @Override
    public boolean isConstant() {
        return vector.isConstant();
    }

    private int mapPosition(int position) {
        return positions[position];
    }

    @Override
    public String toString() {
        return "FilteredVector[" + "positions=" + Arrays.toString(positions) + ", vector=" + vector + "]";
    }
}
