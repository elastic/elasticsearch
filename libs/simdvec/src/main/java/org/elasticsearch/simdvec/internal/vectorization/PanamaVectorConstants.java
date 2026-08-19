/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal.vectorization;

import jdk.incubator.vector.VectorShape;
import jdk.incubator.vector.VectorSpecies;

import org.apache.lucene.util.Constants;

import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

/** Shared constants for implementations that take advantage of the Panama Vector API. */
public final class PanamaVectorConstants {

    /** Preferred width in bits for vectors. */
    public static final int PREFERRED_VECTOR_BITSIZE;

    /** Whether integer vectors can be trusted to actually be fast. */
    public static final boolean ENABLE_INTEGER_VECTORS;

    static final VectorSpecies<Byte> PREFERRED_BYTE_SPECIES;
    static final VectorSpecies<Integer> PREFERRED_INTEGER_SPECIES;
    static final VectorSpecies<Float> PREFERRED_FLOAT_SPECIES;
    static final VectorSpecies<Long> PREFERRED_LONG_SPECIES;

    /*
     * A byte species with the same number of elements as the preferred 4-byte species (float and int).
     * Normally the size of the int species /4.
     *
     * For 128-bits, there isn't a byte species small enough (panama only goes down to 64-bits),
     * so we're over-reading the bytes and throwing away the second half each iteration,
     * due to only using the 0th part when converting to 4-byte values.
     *
     * For real hot paths, it's worth creating separate 128-bit methods that don't do this,
     * but for other methods it's fine to not quite SIMD all of it and scalar process
     * the last 8 bytes + any tail
     */
    static final VectorSpecies<Byte> BYTES_FOR_4BYTE_SPECIES;

    static {
        var vs = OptionalInt.empty();
        try {
            vs = Stream.ofNullable(System.getProperty("tests.vectorsize"))
                .filter(Predicate.not(Set.of("", "default")::contains))
                .mapToInt(Integer::parseInt)
                .findAny();
        } catch (SecurityException ignored) {
            // ignored
        }

        // default to platform supported bitsize
        int vectorBitSize = VectorShape.preferredShape().vectorBitSize();
        // but allow easy overriding for testing
        PREFERRED_VECTOR_BITSIZE = vs.orElse(vectorBitSize);

        PREFERRED_BYTE_SPECIES = VectorSpecies.of(byte.class, VectorShape.forBitSize(PREFERRED_VECTOR_BITSIZE));
        PREFERRED_INTEGER_SPECIES = VectorSpecies.of(int.class, VectorShape.forBitSize(PREFERRED_VECTOR_BITSIZE));
        PREFERRED_FLOAT_SPECIES = VectorSpecies.of(float.class, VectorShape.forBitSize(PREFERRED_VECTOR_BITSIZE));
        PREFERRED_LONG_SPECIES = VectorSpecies.of(long.class, VectorShape.forBitSize(PREFERRED_VECTOR_BITSIZE));

        // hotspot misses some SSE intrinsics, workaround it
        // to be fair, they do document this thing only works well with AVX2/AVX3 and Neon
        boolean isAMD64withoutAVX2 = Constants.OS_ARCH.equals("amd64") && PREFERRED_VECTOR_BITSIZE < 256;
        ENABLE_INTEGER_VECTORS = (isAMD64withoutAVX2 == false) || vs.isPresent();

        int byteBitsForInt = PREFERRED_INTEGER_SPECIES.vectorBitSize() / Float.BYTES;

        VectorSpecies<Byte> byteSpecies = PREFERRED_BYTE_SPECIES; // just specify *something* to fallback on
        // int species / 4 may be too small - double the size until we get to one we can use
        while (byteBitsForInt <= 1024) { // sanity bounds check to prevent infinite loop if this isn't working as it should
            try {
                byteSpecies = VectorSpecies.of(byte.class, VectorShape.forBitSize(byteBitsForInt));
                break;
            } catch (IllegalArgumentException e) {
                byteBitsForInt *= 2;
            }
        }
        BYTES_FOR_4BYTE_SPECIES = byteSpecies;
    }

    private PanamaVectorConstants() {}
}
