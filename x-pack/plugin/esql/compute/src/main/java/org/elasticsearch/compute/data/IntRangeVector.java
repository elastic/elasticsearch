/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

/**
 * A sequential ordered IntVector from startInclusive (inclusive) to endExclusive
 * (exclusive) by an incremental step of 1.
 */
public final class IntRangeVector implements IntVector {

    private final int startInclusive;
    private final int endExclusive;

    /**
     * Returns true if the given vector is {@link IntVector#ascending()} and has a range of values
     * between m (inclusive), and n (exclusive).
     */
    public static boolean isRangeFromMToN(IntVector vector, int m, int n) {
        return vector.ascending() && (vector.getPositionCount() == 0 || vector.getInt(0) == m && vector.getPositionCount() + m == n);
    }

    IntRangeVector(int startInclusive, int endExclusive) {
        this.startInclusive = startInclusive;
        this.endExclusive = endExclusive;
    }

    @Override
    public boolean ascending() {
        return true;
    }

    @Override
    public int getInt(int position) {
        assert position < getPositionCount();
        return startInclusive + position;
    }

    @Override
    public IntBlock asBlock() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getPositionCount() {
        return endExclusive - startInclusive;
    }

    @Override
    public Vector getRow(int position) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IntVector filter(int... positions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ElementType elementType() {
        return ElementType.INT;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof IntVector that) {
            return IntVector.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return IntVector.hash(this);
    }

    @Override
    public String toString() {
        String values = "startInclusive=" + startInclusive + ", endExclusive=" + endExclusive;
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ", " + values + ']';
    }
}
