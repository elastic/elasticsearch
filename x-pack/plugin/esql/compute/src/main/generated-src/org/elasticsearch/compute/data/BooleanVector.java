/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

// begin generated imports
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.ReleasableIterator;

import java.io.IOException;
// end generated imports

/**
 * Vector that stores boolean values.
 * This class is generated. Edit {@code X-Vector.java.st} instead.
 */
public sealed interface BooleanVector extends Vector permits ConstantBooleanVector, BooleanArrayVector, BooleanBigArrayVector,
    ConstantNullVector {
    boolean getBoolean(int position);

    @Override
    BooleanBlock asBlock();

    @Override
    BooleanVector filter(int... positions);

    @Override
    BooleanBlock keepMask(BooleanVector mask);

    @Override
    ReleasableIterator<? extends BooleanBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize);

    /**
     * Are all values {@code true}? This will scan all values to check and always answer accurately.
     */
    boolean allTrue();

    /**
     * Are all values {@code false}? This will scan all values to check and always answer accurately.
     */
    boolean allFalse();

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

    /** Deserializes a Vector from the given stream input. */
    static BooleanVector readFrom(BlockFactory blockFactory, StreamInput in) throws IOException {
        final int positions = in.readVInt();
        final byte serializationType = in.readByte();
        return switch (serializationType) {
            case SERIALIZE_VECTOR_VALUES -> readValues(positions, in, blockFactory);
            case SERIALIZE_VECTOR_CONSTANT -> blockFactory.newConstantBooleanVector(in.readBoolean(), positions);
            case SERIALIZE_VECTOR_ARRAY -> BooleanArrayVector.readArrayVector(positions, in, blockFactory);
            case SERIALIZE_VECTOR_BIG_ARRAY -> BooleanBigArrayVector.readArrayVector(positions, in, blockFactory);
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
            out.writeBoolean(getBoolean(0));
        } else if (version.onOrAfter(TransportVersions.V_8_14_0) && this instanceof BooleanArrayVector v) {
            out.writeByte(SERIALIZE_VECTOR_ARRAY);
            v.writeArrayVector(positions, out);
        } else if (version.onOrAfter(TransportVersions.V_8_14_0) && this instanceof BooleanBigArrayVector v) {
            out.writeByte(SERIALIZE_VECTOR_BIG_ARRAY);
            v.writeArrayVector(positions, out);
        } else {
            out.writeByte(SERIALIZE_VECTOR_VALUES);
            writeValues(this, positions, out);
        }
    }

    private static BooleanVector readValues(int positions, StreamInput in, BlockFactory blockFactory) throws IOException {
        try (var builder = blockFactory.newBooleanVectorFixedBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                builder.appendBoolean(i, in.readBoolean());
            }
            return builder.build();
        }
    }

    private static void writeValues(BooleanVector v, int positions, StreamOutput out) throws IOException {
        for (int i = 0; i < positions; i++) {
            out.writeBoolean(v.getBoolean(i));
        }
    }

    /**
     * A builder that grows as needed.
     */
    sealed interface Builder extends Vector.Builder permits BooleanVectorBuilder, FixedBuilder {
        /**
         * Appends a boolean to the current entry.
         */
        Builder appendBoolean(boolean value);

        @Override
        BooleanVector build();
    }

    /**
     * A builder that never grows.
     */
    sealed interface FixedBuilder extends Builder permits BooleanVectorFixedBuilder {
        /**
         * Appends a boolean to the current entry.
         */
        @Override
        FixedBuilder appendBoolean(boolean value);

        FixedBuilder appendBoolean(int index, boolean value);

    }
}
