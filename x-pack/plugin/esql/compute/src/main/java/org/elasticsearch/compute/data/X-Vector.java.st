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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Vector that stores $type$ values.
 * This class is generated. Do not edit it.
 */
$if(BytesRef)$
public sealed interface $Type$Vector extends Vector permits Constant$Type$Vector, $Type$ArrayVector, ConstantNullVector {
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

    @Override
    $Type$Vector filter(int... positions);

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
        final boolean constant = in.readBoolean();
        if (constant && positions > 0) {
            return blockFactory.newConstant$Type$Vector(in.read$Type$(), positions);
        } else {
            try (var builder = blockFactory.new$Type$Vector$if(BytesRef)$$else$Fixed$endif$Builder(positions)) {
                for (int i = 0; i < positions; i++) {
                    builder.append$Type$(in.read$Type$());
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
$if(BytesRef)$
            out.write$Type$(get$Type$(0, new BytesRef()));
$else$
            out.write$Type$(get$Type$(0));
$endif$
        } else {
            for (int i = 0; i < positions; i++) {
$if(BytesRef)$
                out.write$Type$(get$Type$(i, new BytesRef()));
$else$
                out.write$Type$(get$Type$(i));
$endif$
            }
        }
    }

    /**
     * Returns a builder using the {@link BlockFactory#getNonBreakingInstance nonbreaking block factory}.
     * @deprecated use {@link BlockFactory#new$Type$VectorBuilder}
     */
    // Eventually, we want to remove this entirely, always passing an explicit BlockFactory
    @Deprecated
    static Builder newVectorBuilder(int estimatedSize) {
        return newVectorBuilder(estimatedSize, BlockFactory.getNonBreakingInstance());
    }

$if(BytesRef)$
    /**
     * Creates a builder that grows as needed.
     * @deprecated use {@link BlockFactory#new$Type$VectorBuilder}
     */
$else$
    /**
     * Creates a builder that grows as needed. Prefer {@link #newVectorFixedBuilder}
     * if you know the size up front because it's faster.
     * @deprecated use {@link BlockFactory#new$Type$VectorBuilder}
     */
$endif$
    @Deprecated
    static Builder newVectorBuilder(int estimatedSize, BlockFactory blockFactory) {
        return blockFactory.new$Type$VectorBuilder(estimatedSize);
    }

$if(BytesRef)$
$else$
    /**
     * Creates a builder that never grows. Prefer this over {@link #newVectorBuilder}
     * if you know the size up front because it's faster.
     * @deprecated use {@link BlockFactory#new$Type$VectorFixedBuilder}
     */
    @Deprecated
    static FixedBuilder newVectorFixedBuilder(int size, BlockFactory blockFactory) {
        return blockFactory.new$Type$VectorFixedBuilder(size);
    }
$endif$

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
    sealed interface Builder extends Vector.Builder permits $Type$VectorBuilder {
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
    sealed interface FixedBuilder extends Vector.Builder permits $Type$VectorFixedBuilder {
        /**
         * Appends a $type$ to the current entry.
         */
        FixedBuilder append$Type$($type$ value);

        @Override
        $Type$Vector build();
    }
$endif$
}
