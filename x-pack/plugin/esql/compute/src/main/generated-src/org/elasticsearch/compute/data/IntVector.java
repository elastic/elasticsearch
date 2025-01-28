/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.ReleasableIterator;

import java.io.IOException;

/**
 * Vector that stores int values.
 * This class is generated. Do not edit it.
 */
public sealed interface IntVector extends Vector permits ConstantIntVector, IntArrayVector, IntBigArrayVector, ConstantNullVector {

    int getInt(int position);

    @Override
    IntBlock asBlock();

    @Override
    IntVector filter(int... positions);

    @Override
    IntBlock keepMask(BooleanVector mask);

    @Override
    ReleasableIterator<? extends IntBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize);

    /**
     * The minimum value in the Vector. An empty Vector will return {@link Integer#MAX_VALUE}.
     */
    int min();

    /**
     * The maximum value in the Vector. An empty Vector will return {@link Integer#MIN_VALUE}.
     */
    int max();

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

    /** Deserializes a Vector from the given stream input. */
    static IntVector readFrom(BlockFactory blockFactory, StreamInput in) throws IOException {
        final int positions = in.readVInt();
        final byte serializationType = in.readByte();
        return switch (serializationType) {
            case SERIALIZE_VECTOR_VALUES -> readValues(positions, in, blockFactory);
            case SERIALIZE_VECTOR_CONSTANT -> blockFactory.newConstantIntVector(in.readInt(), positions);
            case SERIALIZE_VECTOR_ARRAY -> IntArrayVector.readArrayVector(positions, in, blockFactory);
            case SERIALIZE_VECTOR_BIG_ARRAY -> IntBigArrayVector.readArrayVector(positions, in, blockFactory);
            default -> {
                assert false : "invalid vector serialization type [" + serializationType + "]";
                throw new IllegalStateException("invalid vector serialization type [" + serializationType + "]");
            }
        };
    }

    /** Serializes this Vector to the given stream output. */
    default void writeTo(StreamOutput out) throws IOException {
        final int positions = getPositionCount();
        final var version = out.getTransportVersion();
        out.writeVInt(positions);
        if (isConstant() && positions > 0) {
            out.writeByte(SERIALIZE_VECTOR_CONSTANT);
            out.writeInt(getInt(0));
        } else if (version.onOrAfter(TransportVersions.V_8_14_0) && this instanceof IntArrayVector v) {
            out.writeByte(SERIALIZE_VECTOR_ARRAY);
            v.writeArrayVector(positions, out);
        } else if (version.onOrAfter(TransportVersions.V_8_14_0) && this instanceof IntBigArrayVector v) {
            out.writeByte(SERIALIZE_VECTOR_BIG_ARRAY);
            v.writeArrayVector(positions, out);
        } else {
            out.writeByte(SERIALIZE_VECTOR_VALUES);
            writeValues(this, positions, out);
        }
    }

    private static IntVector readValues(int positions, StreamInput in, BlockFactory blockFactory) throws IOException {
        try (var builder = blockFactory.newIntVectorFixedBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                builder.appendInt(i, in.readInt());
            }
            return builder.build();
        }
    }

    private static void writeValues(IntVector v, int positions, StreamOutput out) throws IOException {
        for (int i = 0; i < positions; i++) {
            out.writeInt(v.getInt(i));
        }
    }

    /** Create a vector for a range of ints. */
    static IntVector range(int startInclusive, int endExclusive, BlockFactory blockFactory) {
        int[] values = new int[endExclusive - startInclusive];
        for (int i = 0; i < values.length; i++) {
            values[i] = startInclusive + i;
        }
        return blockFactory.newIntArrayVector(values, values.length);
    }

    /**
     * A builder that grows as needed.
     */
    sealed interface Builder extends Vector.Builder permits IntVectorBuilder, FixedBuilder {
        /**
         * Appends a int to the current entry.
         */
        Builder appendInt(int value);

        @Override
        IntVector build();
    }

    /**
     * A builder that never grows.
     */
    sealed interface FixedBuilder extends Builder permits IntVectorFixedBuilder {
        /**
         * Appends a int to the current entry.
         */
        @Override
        FixedBuilder appendInt(int value);

        FixedBuilder appendInt(int index, int value);

    }
}
