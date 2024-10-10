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

    @Override
    DoubleBlock keepMask(BooleanVector mask);

    @Override
    ReleasableIterator<? extends DoubleBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize);

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
        final byte serializationType = in.readByte();
        return switch (serializationType) {
            case SERIALIZE_VECTOR_VALUES -> readValues(positions, in, blockFactory);
            case SERIALIZE_VECTOR_CONSTANT -> blockFactory.newConstantDoubleVector(in.readDouble(), positions);
            case SERIALIZE_VECTOR_ARRAY -> DoubleArrayVector.readArrayVector(positions, in, blockFactory);
            case SERIALIZE_VECTOR_BIG_ARRAY -> DoubleBigArrayVector.readArrayVector(positions, in, blockFactory);
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
            out.writeDouble(getDouble(0));
        } else if (version.onOrAfter(TransportVersions.V_8_14_0) && this instanceof DoubleArrayVector v) {
            out.writeByte(SERIALIZE_VECTOR_ARRAY);
            v.writeArrayVector(positions, out);
        } else if (version.onOrAfter(TransportVersions.V_8_14_0) && this instanceof DoubleBigArrayVector v) {
            out.writeByte(SERIALIZE_VECTOR_BIG_ARRAY);
            v.writeArrayVector(positions, out);
        } else {
            out.writeByte(SERIALIZE_VECTOR_VALUES);
            writeValues(this, positions, out);
        }
    }

    private static DoubleVector readValues(int positions, StreamInput in, BlockFactory blockFactory) throws IOException {
        try (var builder = blockFactory.newDoubleVectorFixedBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                builder.appendDouble(i, in.readDouble());
            }
            return builder.build();
        }
    }

    private static void writeValues(DoubleVector v, int positions, StreamOutput out) throws IOException {
        for (int i = 0; i < positions; i++) {
            out.writeDouble(v.getDouble(i));
        }
    }

    /**
     * A builder that grows as needed.
     */
    sealed interface Builder extends Vector.Builder permits DoubleVectorBuilder, FixedBuilder {
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
    sealed interface FixedBuilder extends Builder permits DoubleVectorFixedBuilder {
        /**
         * Appends a double to the current entry.
         */
        @Override
        FixedBuilder appendDouble(double value);

        FixedBuilder appendDouble(int index, double value);

    }
}
