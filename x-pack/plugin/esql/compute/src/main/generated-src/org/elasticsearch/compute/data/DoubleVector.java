/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Vector that stores double values.
 * This class is generated. Do not edit it.
 */
public sealed interface DoubleVector extends Vector permits ConstantDoubleVector, DoubleArrayVector, DoubleBigArrayVector,
    ConstantNullVector {
    double getDouble(int position);

    @Override
    DoubleBlock asBlock();

    @Override
    DoubleVector filter(int... positions);

    /**
     * Compares the given object with this vector for equality. Returns {@code true} if and only if the
     * given object is a DoubleVector, and both vectors are {@link #equals(DoubleVector, DoubleVector) equal}.
     */
    @Override
    boolean equals(Object obj);

    /** Returns the hash code of this vector, as defined by {@link #hash(DoubleVector)}. */
    @Override
    int hashCode();

    /**
     * Returns {@code true} if the given vectors are equal to each other, otherwise {@code false}.
     * Two vectors are considered equal if they have the same position count, and contain the same
     * values in the same order. This definition ensures that the equals method works properly
     * across different implementations of the DoubleVector interface.
     */
    static boolean equals(DoubleVector vector1, DoubleVector vector2) {
        final int positions = vector1.getPositionCount();
        if (positions != vector2.getPositionCount()) {
            return false;
        }
        for (int pos = 0; pos < positions; pos++) {
            if (vector1.getDouble(pos) != vector2.getDouble(pos)) {
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
    static int hash(DoubleVector vector) {
        final int len = vector.getPositionCount();
        int result = 1;
        for (int pos = 0; pos < len; pos++) {
            long element = Double.doubleToLongBits(vector.getDouble(pos));
            result = 31 * result + (int) (element ^ (element >>> 32));
        }
        return result;
    }

    /** Deserializes a Vector from the given stream input. */
    static DoubleVector readFrom(BlockFactory blockFactory, StreamInput in) throws IOException {
        final int positions = in.readVInt();
        final boolean constant = in.readBoolean();
        if (constant && positions > 0) {
            return blockFactory.newConstantDoubleVector(in.readDouble(), positions);
        } else {
            try (var builder = blockFactory.newDoubleVectorFixedBuilder(positions)) {
                for (int i = 0; i < positions; i++) {
                    builder.appendDouble(in.readDouble());
                }
                return builder.build();
            }
        }
    }

    /** Serializes this Vector to the given stream output. */
    default void writeTo(StreamOutput out) throws IOException {
        final int positions = getPositionCount();
        out.writeVInt(positions);
        out.writeBoolean(isConstant());
        if (isConstant() && positions > 0) {
            out.writeDouble(getDouble(0));
        } else {
            for (int i = 0; i < positions; i++) {
                out.writeDouble(getDouble(i));
            }
        }
    }

    /**
     * Returns a builder using the {@link BlockFactory#getNonBreakingInstance nonbreaking block factory}.
     * @deprecated use {@link BlockFactory#newDoubleVectorBuilder}
     */
    // Eventually, we want to remove this entirely, always passing an explicit BlockFactory
    @Deprecated
    static Builder newVectorBuilder(int estimatedSize) {
        return newVectorBuilder(estimatedSize, BlockFactory.getNonBreakingInstance());
    }

    /**
     * Creates a builder that grows as needed. Prefer {@link #newVectorFixedBuilder}
     * if you know the size up front because it's faster.
     * @deprecated use {@link BlockFactory#newDoubleVectorBuilder}
     */
    @Deprecated
    static Builder newVectorBuilder(int estimatedSize, BlockFactory blockFactory) {
        return blockFactory.newDoubleVectorBuilder(estimatedSize);
    }

    /**
     * Creates a builder that never grows. Prefer this over {@link #newVectorBuilder}
     * if you know the size up front because it's faster.
     * @deprecated use {@link BlockFactory#newDoubleVectorFixedBuilder}
     */
    @Deprecated
    static FixedBuilder newVectorFixedBuilder(int size, BlockFactory blockFactory) {
        return blockFactory.newDoubleVectorFixedBuilder(size);
    }

    /**
     * A builder that grows as needed.
     */
    sealed interface Builder extends Vector.Builder permits DoubleVectorBuilder {
        /**
         * Appends a double to the current entry.
         */
        Builder appendDouble(double value);

        @Override
        DoubleVector build();
    }

    /**
     * A builder that never grows.
     */
    sealed interface FixedBuilder extends Vector.Builder permits DoubleVectorFixedBuilder {
        /**
         * Appends a double to the current entry.
         */
        FixedBuilder appendDouble(double value);

        @Override
        DoubleVector build();
    }
}
