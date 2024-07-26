/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.ReleasableIterator;

import java.io.IOException;

/**
 * Vector that stores BytesRef values.
 * This class is generated. Do not edit it.
 */
public sealed interface BytesRefVector extends Vector permits ConstantBytesRefVector, BytesRefArrayVector, ConstantNullVector,
    OrdinalBytesRefVector {
    BytesRef getBytesRef(int position, BytesRef dest);

    @Override
    BytesRefBlock asBlock();

    /**
     * Returns an ordinal BytesRef vector if this vector is backed by a dictionary and ordinals; otherwise,
     * returns null. Callers must not release the returned vector as no extra reference is retained by this method.
     */
    OrdinalBytesRefVector asOrdinals();

    @Override
    BytesRefVector filter(int... positions);

    @Override
    ReleasableIterator<? extends BytesRefBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize);

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

    /** Deserializes a Vector from the given stream input. */
    static BytesRefVector readFrom(BlockFactory blockFactory, StreamInput in) throws IOException {
        final int positions = in.readVInt();
        final byte serializationType = in.readByte();
        return switch (serializationType) {
            case SERIALIZE_VECTOR_VALUES -> readValues(positions, in, blockFactory);
            case SERIALIZE_VECTOR_CONSTANT -> blockFactory.newConstantBytesRefVector(in.readBytesRef(), positions);
            case SERIALIZE_VECTOR_ARRAY -> BytesRefArrayVector.readArrayVector(positions, in, blockFactory);
            case SERIALIZE_VECTOR_ORDINAL -> OrdinalBytesRefVector.readOrdinalVector(blockFactory, in);
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
            out.writeBytesRef(getBytesRef(0, new BytesRef()));
        } else if (version.onOrAfter(TransportVersions.V_8_14_0) && this instanceof BytesRefArrayVector v) {
            out.writeByte(SERIALIZE_VECTOR_ARRAY);
            v.writeArrayVector(positions, out);
        } else if (version.onOrAfter(TransportVersions.V_8_14_0) && this instanceof OrdinalBytesRefVector v && v.isDense()) {
            out.writeByte(SERIALIZE_VECTOR_ORDINAL);
            v.writeOrdinalVector(out);
        } else {
            out.writeByte(SERIALIZE_VECTOR_VALUES);
            writeValues(this, positions, out);
        }
    }

    private static BytesRefVector readValues(int positions, StreamInput in, BlockFactory blockFactory) throws IOException {
        try (var builder = blockFactory.newBytesRefVectorBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                builder.appendBytesRef(in.readBytesRef());
            }
            return builder.build();
        }
    }

    private static void writeValues(BytesRefVector v, int positions, StreamOutput out) throws IOException {
        var scratch = new BytesRef();
        for (int i = 0; i < positions; i++) {
            out.writeBytesRef(v.getBytesRef(i, scratch));
        }
    }

    /**
     * A builder that grows as needed.
     */
    sealed interface Builder extends Vector.Builder permits BytesRefVectorBuilder {
        /**
         * Appends a BytesRef to the current entry.
         */
        Builder appendBytesRef(BytesRef value);

        @Override
        BytesRefVector build();
    }

}
