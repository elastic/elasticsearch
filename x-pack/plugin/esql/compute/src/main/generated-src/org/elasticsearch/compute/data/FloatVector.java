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
 * Vector that stores float values.
 * This class is generated. Do not edit it.
 */
public sealed interface FloatVector extends Vector permits ConstantFloatVector, FloatArrayVector, FloatBigArrayVector, ConstantNullVector {

    float getFloat(int position);

    @Override
    FloatBlock asBlock();

    @Override
    FloatVector filter(int... positions);

    @Override
    ReleasableIterator<? extends FloatBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize);

    /**
     * Compares the given object with this vector for equality. Returns {@code true} if and only if the
     * given object is a FloatVector, and both vectors are {@link #equals(FloatVector, FloatVector) equal}.
     */
    @Override
    boolean equals(Object obj);

    /** Returns the hash code of this vector, as defined by {@link #hash(FloatVector)}. */
    @Override
    int hashCode();

    /**
     * Returns {@code true} if the given vectors are equal to each other, otherwise {@code false}.
     * Two vectors are considered equal if they have the same position count, and contain the same
     * values in the same order. This definition ensures that the equals method works properly
     * across different implementations of the FloatVector interface.
     */
    static boolean equals(FloatVector vector1, FloatVector vector2) {
        final int positions = vector1.getPositionCount();
        if (positions != vector2.getPositionCount()) {
            return false;
        }
        for (int pos = 0; pos < positions; pos++) {
            if (vector1.getFloat(pos) != vector2.getFloat(pos)) {
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
    static int hash(FloatVector vector) {
        final int len = vector.getPositionCount();
        int result = 1;
        for (int pos = 0; pos < len; pos++) {
            result = 31 * result + Float.floatToIntBits(vector.getFloat(pos));
        }
        return result;
    }

    /** Deserializes a Vector from the given stream input. */
    static FloatVector readFrom(BlockFactory blockFactory, StreamInput in) throws IOException {
        final int positions = in.readVInt();
        final byte serializationType = in.readByte();
        return switch (serializationType) {
            case SERIALIZE_VECTOR_VALUES -> readValues(positions, in, blockFactory);
            case SERIALIZE_VECTOR_CONSTANT -> blockFactory.newConstantFloatVector(in.readFloat(), positions);
            case SERIALIZE_VECTOR_ARRAY -> FloatArrayVector.readArrayVector(positions, in, blockFactory);
            case SERIALIZE_VECTOR_BIG_ARRAY -> FloatBigArrayVector.readArrayVector(positions, in, blockFactory);
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
            out.writeFloat(getFloat(0));
        } else if (version.onOrAfter(TransportVersions.V_8_14_0) && this instanceof FloatArrayVector v) {
            out.writeByte(SERIALIZE_VECTOR_ARRAY);
            v.writeArrayVector(positions, out);
        } else if (version.onOrAfter(TransportVersions.V_8_14_0) && this instanceof FloatBigArrayVector v) {
            out.writeByte(SERIALIZE_VECTOR_BIG_ARRAY);
            v.writeArrayVector(positions, out);
        } else {
            out.writeByte(SERIALIZE_VECTOR_VALUES);
            writeValues(this, positions, out);
        }
    }

    private static FloatVector readValues(int positions, StreamInput in, BlockFactory blockFactory) throws IOException {
        try (var builder = blockFactory.newFloatVectorFixedBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                builder.appendFloat(i, in.readFloat());
            }
            return builder.build();
        }
    }

    private static void writeValues(FloatVector v, int positions, StreamOutput out) throws IOException {
        for (int i = 0; i < positions; i++) {
            out.writeFloat(v.getFloat(i));
        }
    }

    /**
     * A builder that grows as needed.
     */
    sealed interface Builder extends Vector.Builder permits FloatVectorBuilder, FixedBuilder {
        /**
         * Appends a float to the current entry.
         */
        Builder appendFloat(float value);

        @Override
        FloatVector build();
    }

    /**
     * A builder that never grows.
     */
    sealed interface FixedBuilder extends Builder permits FloatVectorFixedBuilder {
        /**
         * Appends a float to the current entry.
         */
        @Override
        FixedBuilder appendFloat(float value);

        FixedBuilder appendFloat(int index, float value);

    }
}
