/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

$if(BytesRef)$
import org.apache.lucene.util.BytesRef;
$endif$
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.ReleasableIterator;

import java.io.IOException;

/**
 * Vector that stores $type$ values.
 * This class is generated. Do not edit it.
 */
$if(BytesRef)$
public sealed interface BytesRefVector extends Vector permits ConstantBytesRefVector, BytesRefArrayVector, ConstantNullVector,
    OrdinalBytesRefVector {
$elseif(boolean)$
public sealed interface $Type$Vector extends Vector permits Constant$Type$Vector, $Type$ArrayVector, $Type$BigArrayVector,
    ConstantNullVector {
$elseif(double)$
public sealed interface $Type$Vector extends Vector permits Constant$Type$Vector, $Type$ArrayVector, $Type$BigArrayVector,
    ConstantNullVector {
$else$
public sealed interface $Type$Vector extends Vector permits Constant$Type$Vector, $Type$ArrayVector, $Type$BigArrayVector, ConstantNullVector {
$endif$

$if(BytesRef)$
    BytesRef getBytesRef(int position, BytesRef dest);

$else$
    $type$ get$Type$(int position);
$endif$

    @Override
    $Type$Block asBlock();

$if(BytesRef)$
    /**
     * Returns an ordinal BytesRef vector if this vector is backed by a dictionary and ordinals; otherwise,
     * returns null. Callers must not release the returned vector as no extra reference is retained by this method.
     */
    OrdinalBytesRefVector asOrdinals();

$endif$
    @Override
    $Type$Vector filter(int... positions);

    @Override
    $Type$Block keepMask(BooleanVector mask);

    @Override
    ReleasableIterator<? extends $Type$Block> lookup(IntBlock positions, ByteSizeValue targetBlockSize);

$if(int)$
    /**
     * The minimum value in the Vector. An empty Vector will return {@link Integer#MAX_VALUE}.
     */
    int min();

    /**
     * The maximum value in the Vector. An empty Vector will return {@link Integer#MIN_VALUE}.
     */
    int max();

$elseif(boolean)$
    /**
     * Are all values {@code true}? This will scan all values to check and always answer accurately.
     */
    boolean allTrue();

    /**
     * Are all values {@code false}? This will scan all values to check and always answer accurately.
     */
    boolean allFalse();
$endif$

    /**
     * Compares the given object with this vector for equality. Returns {@code true} if and only if the
     * given object is a $Type$Vector, and both vectors are {@link #equals($Type$Vector, $Type$Vector) equal}.
     */
    @Override
    boolean equals(Object obj);

    /** Returns the hash code of this vector, as defined by {@link #hash($Type$Vector)}. */
    @Override
    int hashCode();

    /**
     * Returns {@code true} if the given vectors are equal to each other, otherwise {@code false}.
     * Two vectors are considered equal if they have the same position count, and contain the same
     * values in the same order. This definition ensures that the equals method works properly
     * across different implementations of the $Type$Vector interface.
     */
    static boolean equals($Type$Vector vector1, $Type$Vector vector2) {
        final int positions = vector1.getPositionCount();
        if (positions != vector2.getPositionCount()) {
            return false;
        }
        for (int pos = 0; pos < positions; pos++) {
$if(BytesRef)$
            if (vector1.getBytesRef(pos, new BytesRef()).equals(vector2.getBytesRef(pos, new BytesRef())) == false) {
$else$
            if (vector1.get$Type$(pos) != vector2.get$Type$(pos)) {
$endif$
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
    static int hash($Type$Vector vector) {
        final int len = vector.getPositionCount();
        int result = 1;
        for (int pos = 0; pos < len; pos++) {
$if(BytesRef)$
            result = 31 * result + vector.getBytesRef(pos, new BytesRef()).hashCode();
$elseif(boolean)$
            result = 31 * result + Boolean.hashCode(vector.getBoolean(pos));
$elseif(int)$
            result = 31 * result + vector.getInt(pos);
$elseif(float)$
            result = 31 * result + Float.floatToIntBits(vector.getFloat(pos));
$elseif(long)$
            long element = vector.getLong(pos);
            result = 31 * result + (int) (element ^ (element >>> 32));
$elseif(double)$
            long element = Double.doubleToLongBits(vector.getDouble(pos));
            result = 31 * result + (int) (element ^ (element >>> 32));
$endif$
        }
        return result;
    }

    /** Deserializes a Vector from the given stream input. */
    static $Type$Vector readFrom(BlockFactory blockFactory, StreamInput in) throws IOException {
        final int positions = in.readVInt();
        final byte serializationType = in.readByte();
        return switch (serializationType) {
            case SERIALIZE_VECTOR_VALUES -> readValues(positions, in, blockFactory);
            case SERIALIZE_VECTOR_CONSTANT -> blockFactory.newConstant$Type$Vector(in.read$Type$(), positions);
            case SERIALIZE_VECTOR_ARRAY -> $Type$ArrayVector.readArrayVector(positions, in, blockFactory);
$if(BytesRef)$
            case SERIALIZE_VECTOR_ORDINAL -> OrdinalBytesRefVector.readOrdinalVector(blockFactory, in);
$else$
            case SERIALIZE_VECTOR_BIG_ARRAY -> $Type$BigArrayVector.readArrayVector(positions, in, blockFactory);
$endif$
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
$if(BytesRef)$
            out.write$Type$(get$Type$(0, new BytesRef()));
$else$
            out.write$Type$(get$Type$(0));
$endif$
        } else if (version.onOrAfter(TransportVersions.V_8_14_0) && this instanceof $Type$ArrayVector v) {
            out.writeByte(SERIALIZE_VECTOR_ARRAY);
            v.writeArrayVector(positions, out);
$if(BytesRef)$
        } else if (version.onOrAfter(TransportVersions.V_8_14_0) && this instanceof OrdinalBytesRefVector v && v.isDense()) {
            out.writeByte(SERIALIZE_VECTOR_ORDINAL);
            v.writeOrdinalVector(out);
$else$
        } else if (version.onOrAfter(TransportVersions.V_8_14_0) && this instanceof $Type$BigArrayVector v) {
            out.writeByte(SERIALIZE_VECTOR_BIG_ARRAY);
            v.writeArrayVector(positions, out);
$endif$
        } else {
            out.writeByte(SERIALIZE_VECTOR_VALUES);
            writeValues(this, positions, out);
        }
    }

    private static $Type$Vector readValues(int positions, StreamInput in, BlockFactory blockFactory) throws IOException {
        try (var builder = blockFactory.new$Type$Vector$if(BytesRef)$$else$Fixed$endif$Builder(positions)) {
            for (int i = 0; i < positions; i++) {
                builder.append$Type$($if(BytesRef)$$else$i, $endif$in.read$Type$());
            }
            return builder.build();
        }
    }

    private static void writeValues($Type$Vector v, int positions, StreamOutput out) throws IOException {
$if(BytesRef)$
        var scratch = new BytesRef();
$endif$
        for (int i = 0; i < positions; i++) {
$if(BytesRef)$
            out.write$Type$(v.get$Type$(i, scratch));
$else$
            out.write$Type$(v.get$Type$(i));
$endif$
        }
    }

$if(int)$
    /** Create a vector for a range of ints. */
    static IntVector range(int startInclusive, int endExclusive, BlockFactory blockFactory) {
        int[] values = new int[endExclusive - startInclusive];
        for (int i = 0; i < values.length; i++) {
            values[i] = startInclusive + i;
        }
        return blockFactory.newIntArrayVector(values, values.length);
    }
$endif$

    /**
     * A builder that grows as needed.
     */
$if(BytesRef)$
    sealed interface Builder extends Vector.Builder permits $Type$VectorBuilder {
$else$
    sealed interface Builder extends Vector.Builder permits $Type$VectorBuilder, FixedBuilder {
$endif$
        /**
         * Appends a $type$ to the current entry.
         */
        Builder append$Type$($type$ value);

        @Override
        $Type$Vector build();
    }

$if(BytesRef)$
$else$
    /**
     * A builder that never grows.
     */
    sealed interface FixedBuilder extends Builder permits $Type$VectorFixedBuilder {
        /**
         * Appends a $type$ to the current entry.
         */
        @Override
        FixedBuilder append$Type$($type$ value);

        FixedBuilder append$Type$(int index, $type$ value);

    }
$endif$
}
