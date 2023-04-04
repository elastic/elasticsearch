/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;

/**
 * Vector that stores BytesRef values.
 * This class is generated. Do not edit it.
 */
public sealed interface BytesRefVector extends Vector permits ConstantBytesRefVector, FilterBytesRefVector, BytesRefArrayVector {

    BytesRef getBytesRef(int position, BytesRef dest);

    @Override
    BytesRefBlock asBlock();

    @Override
    BytesRefVector filter(int... positions);

    /**
     * Compares the given object with this vector for equality. Returns {@code true} if and only if the
     * given object is a BytesRefVector, and both vectors are {@link #equals(BytesRefVector, BytesRefVector) equal}.
     */
    @Override
    boolean equals(Object obj);

    /** Returns the hash code of this vector, as defined by {@link #hash(BytesRefVector)}. */
    @Override
    int hashCode();

    /**
     * Returns {@code true} if the given vectors are equal to each other, otherwise {@code false}.
     * Two vectors are considered equal if they have the same position count, and contain the same
     * values in the same order. This definition ensures that the equals method works properly
     * across different implementations of the BytesRefVector interface.
     */
    static boolean equals(BytesRefVector vector1, BytesRefVector vector2) {
        final int positions = vector1.getPositionCount();
        if (positions != vector2.getPositionCount()) {
            return false;
        }
        for (int pos = 0; pos < positions; pos++) {
            if (vector1.getBytesRef(pos, new BytesRef()).equals(vector2.getBytesRef(pos, new BytesRef())) == false) {
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
    static int hash(BytesRefVector vector) {
        final int len = vector.getPositionCount();
        int result = 1;
        for (int pos = 0; pos < len; pos++) {
            result = 31 * result + vector.getBytesRef(pos, new BytesRef()).hashCode();
        }
        return result;
    }

    static Builder newVectorBuilder(int estimatedSize) {
        return new BytesRefVectorBuilder(estimatedSize);
    }

    sealed interface Builder extends Vector.Builder permits BytesRefVectorBuilder {
        /**
         * Appends a BytesRef to the current entry.
         */
        Builder appendBytesRef(BytesRef value);

        @Override
        BytesRefVector build();
    }
}
