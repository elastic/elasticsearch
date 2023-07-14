/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

/**
 * Vector that stores int values.
 * This class is generated. Do not edit it.
 */
public sealed interface IntVector extends Vector permits ConstantIntVector, FilterIntVector, IntArrayVector, IntBigArrayVector,
    IntRangeVector {
    int getInt(int position);

    @Override
    IntBlock asBlock();

    @Override
    IntVector filter(int... positions);

    /**
     * Compares the given object with this vector for equality. Returns {@code true} if and only if the
     * given object is a IntVector, and both vectors are {@link #equals(IntVector, IntVector) equal}.
     */
    @Override
    boolean equals(Object obj);

    /** Returns the hash code of this vector, as defined by {@link #hash(IntVector)}. */
    @Override
    int hashCode();

    /**
     * Returns {@code true} if the given vectors are equal to each other, otherwise {@code false}.
     * Two vectors are considered equal if they have the same position count, and contain the same
     * values in the same order. This definition ensures that the equals method works properly
     * across different implementations of the IntVector interface.
     */
    static boolean equals(IntVector vector1, IntVector vector2) {
        final int positions = vector1.getPositionCount();
        if (positions != vector2.getPositionCount()) {
            return false;
        }
        for (int pos = 0; pos < positions; pos++) {
            if (vector1.getInt(pos) != vector2.getInt(pos)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Generates the hash code for the given vector. The hash code is computed from the vector's values.
     * This ensures that {@code vector1.equals(vector2)} implies that {@code vector1.hashCode()==vector2.hashCode()}
     * for any two vectors, {@code vector1} and {@code vector2}, as required by the general contract of
     * {@link Object#hashCode}.
     */
    static int hash(IntVector vector) {
        final int len = vector.getPositionCount();
        int result = 1;
        for (int pos = 0; pos < len; pos++) {
            result = 31 * result + vector.getInt(pos);
        }
        return result;
    }

    static Builder newVectorBuilder(int estimatedSize) {
        return new IntVectorBuilder(estimatedSize);
    }

    /**
     * Returns true iff the values in this vector are known to be ascending.
     * A return value of false does not necessarily indicate that the values are not ascending, just
     * that it is not known.
     */
    default boolean ascending() {
        return false;
    }

    /**
     * Returns an IntVector containing a sequence of values from startInclusive to endExclusive,
     * where each value is equal to the previous value + 1. Vectors returned by this factory method
     * have the {@link #ascending} property.
     */
    static IntVector range(int startInclusive, int endExclusive) {
        return new IntRangeVector(startInclusive, endExclusive);
    }

    sealed interface Builder extends Vector.Builder permits IntVectorBuilder {
        /**
         * Appends a int to the current entry.
         */
        Builder appendInt(int value);

        @Override
        IntVector build();
    }
}
