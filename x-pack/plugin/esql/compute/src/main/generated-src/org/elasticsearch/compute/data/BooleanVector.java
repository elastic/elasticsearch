/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

/**
 * Vector that stores boolean values.
 * This class is generated. Do not edit it.
 */
public sealed interface BooleanVector extends Vector permits ConstantBooleanVector, FilterBooleanVector, BooleanArrayVector {

    boolean getBoolean(int position);

    @Override
    BooleanBlock asBlock();

    @Override
    BooleanVector filter(int... positions);

    /**
     * Compares the given object with this vector for equality. Returns {@code true} if and only if the
     * given object is a BooleanVector, and both vectors are {@link #equals(BooleanVector, BooleanVector) equal}.
     */
    @Override
    boolean equals(Object obj);

    /** Returns the hash code of this vector, as defined by {@link #hash(BooleanVector)}. */
    @Override
    int hashCode();

    /**
     * Returns {@code true} if the given vectors are equal to each other, otherwise {@code false}.
     * Two vectors are considered equal if they have the same position count, and contain the same
     * values in the same order. This definition ensures that the equals method works properly
     * across different implementations of the BooleanVector interface.
     */
    static boolean equals(BooleanVector vector1, BooleanVector vector2) {
        final int positions = vector1.getPositionCount();
        if (positions != vector2.getPositionCount()) {
            return false;
        }
        for (int pos = 0; pos < positions; pos++) {
            if (vector1.getBoolean(pos) != vector2.getBoolean(pos)) {
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
    static int hash(BooleanVector vector) {
        final int len = vector.getPositionCount();
        int result = 1;
        for (int pos = 0; pos < len; pos++) {
            result = 31 * result + Boolean.hashCode(vector.getBoolean(pos));
        }
        return result;
    }

    static Builder newVectorBuilder(int estimatedSize) {
        return new BooleanVectorBuilder(estimatedSize);
    }

    sealed interface Builder extends Vector.Builder permits BooleanVectorBuilder {
        /**
         * Appends a boolean to the current entry.
         */
        Builder appendBoolean(boolean value);

        @Override
        BooleanVector build();
    }
}
