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
 * Vector that stores long values.
 * This class is generated. Do not edit it.
 */
public sealed interface LongVector extends Vector permits ConstantLongVector, LongArrayVector, LongBigArrayVector, ConstantNullVector {

    long getLong(int position);

    @Override
    LongBlock asBlock();

    @Override
    LongVector filter(int... positions);

    @Override
    LongBlock keepMask(BooleanVector mask);

    @Override
    ReleasableIterator<? extends LongBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize);

    /**
     * Compares the given object with this vector for equality. Returns {@code true} if and only if the
     * given object is a LongVector, and both vectors are {@link #equals(LongVector, LongVector) equal}.
     */
    @Override
    boolean equals(Object obj);

    /** Returns the hash code of this vector, as defined by {@link #hash(LongVector)}. */
    @Override
    int hashCode();

    /**
     * Returns {@code true} if the given vectors are equal to each other, otherwise {@code false}.
     * Two vectors are considered equal if they have the same position count, and contain the same
     * values in the same order. This definition ensures that the equals method works properly
     * across different implementations of the LongVector interface.
     */
    static boolean equals(LongVector vector1, LongVector vector2) {
        final int positions = vector1.getPositionCount();
        if (positions != vector2.getPositionCount()) {
            return false;
        }
        for (int pos = 0; pos < positions; pos++) {
            if (vector1.getLong(pos) != vector2.getLong(pos)) {
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
    static int hash(LongVector vector) {
        final int len = vector.getPositionCount();
        int result = 1;
        for (int pos = 0; pos < len; pos++) {
            long element = vector.getLong(pos);
            result = 31 * result + (int) (element ^ (element >>> 32));
        }
        return result;
    }

    /** Deserializes a Vector from the given stream input. */
    static LongVector readFrom(BlockFactory blockFactory, StreamInput in) throws IOException {
        final int positions = in.readVInt();
        final byte serializationType = in.readByte();
        return switch (serializationType) {
            case SERIALIZE_VECTOR_VALUES -> readValues(positions, in, blockFactory);
            case SERIALIZE_VECTOR_CONSTANT -> blockFactory.newConstantLongVector(in.readLong(), positions);
            case SERIALIZE_VECTOR_ARRAY -> LongArrayVector.readArrayVector(positions, in, blockFactory);
            case SERIALIZE_VECTOR_BIG_ARRAY -> LongBigArrayVector.readArrayVector(positions, in, blockFactory);
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
            out.writeLong(getLong(0));
        } else if (version.onOrAfter(TransportVersions.V_8_14_0) && this instanceof LongArrayVector v) {
            out.writeByte(SERIALIZE_VECTOR_ARRAY);
            v.writeArrayVector(positions, out);
        } else if (version.onOrAfter(TransportVersions.V_8_14_0) && this instanceof LongBigArrayVector v) {
            out.writeByte(SERIALIZE_VECTOR_BIG_ARRAY);
            v.writeArrayVector(positions, out);
        } else {
            out.writeByte(SERIALIZE_VECTOR_VALUES);
            writeValues(this, positions, out);
        }
    }

    private static LongVector readValues(int positions, StreamInput in, BlockFactory blockFactory) throws IOException {
        try (var builder = blockFactory.newLongVectorFixedBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                builder.appendLong(i, in.readLong());
            }
            return builder.build();
        }
    }

    private static void writeValues(LongVector v, int positions, StreamOutput out) throws IOException {
        for (int i = 0; i < positions; i++) {
            out.writeLong(v.getLong(i));
        }
    }

    /**
     * A builder that grows as needed.
     */
    sealed interface Builder extends Vector.Builder permits LongVectorBuilder, FixedBuilder {
        /**
         * Appends a long to the current entry.
         */
        Builder appendLong(long value);

        @Override
        LongVector build();
    }

    /**
     * A builder that never grows.
     */
    sealed interface FixedBuilder extends Builder permits LongVectorFixedBuilder {
        /**
         * Appends a long to the current entry.
         */
        @Override
        FixedBuilder appendLong(long value);

        FixedBuilder appendLong(int index, long value);

    }
}
