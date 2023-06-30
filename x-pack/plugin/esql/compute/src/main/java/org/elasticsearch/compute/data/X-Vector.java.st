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

/**
 * Vector that stores $type$ values.
 * This class is generated. Do not edit it.
 */
$if(BytesRef)$
public sealed interface $Type$Vector extends Vector permits Constant$Type$Vector, Filter$Type$Vector, $Type$ArrayVector {
$elseif(boolean)$
public sealed interface $Type$Vector extends Vector permits Constant$Type$Vector, Filter$Type$Vector, $Type$ArrayVector,
    $Type$BigArrayVector {
$elseif(double)$
public sealed interface $Type$Vector extends Vector permits Constant$Type$Vector, Filter$Type$Vector, $Type$ArrayVector,
    $Type$BigArrayVector {
$else$
public sealed interface $Type$Vector extends Vector permits Constant$Type$Vector, Filter$Type$Vector, $Type$ArrayVector, $Type$BigArrayVector {
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

    static Builder newVectorBuilder(int estimatedSize) {
        return new $Type$VectorBuilder(estimatedSize);
    }

$if(int)$
    /** Create a vector for a range of ints. */
    static IntVector range(int startInclusive, int endExclusive) {
        int[] values = new int[endExclusive - startInclusive];
        for (int i = 0; i < values.length; i++) {
            values[i] = startInclusive + i;
        }
        return new IntArrayVector(values, values.length);
    }
$endif$

    sealed interface Builder extends Vector.Builder permits $Type$VectorBuilder {
        /**
         * Appends a $type$ to the current entry.
         */
        Builder append$Type$($type$ value);

        @Override
        $Type$Vector build();
    }
}
