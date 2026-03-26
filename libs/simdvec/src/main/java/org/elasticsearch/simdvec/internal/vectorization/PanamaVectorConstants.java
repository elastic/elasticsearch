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
    static final int PREFERRED_VECTOR_BITSIZE;

    /** Whether integer vectors can be trusted to actually be fast. */
    static final boolean ENABLE_INTEGER_VECTORS;

    static final VectorSpecies<Byte> PREFERRED_BYTE_SPECIES;
    static final VectorSpecies<Integer> PREFERRED_INTEGER_SPECIES;
    static final VectorSpecies<Float> PREFERRED_FLOAT_SPECIES;

    static {
        var vs = OptionalInt.empty();
        try {
            vs = Stream.ofNullable(System.getProperty("tests.vectorsize"))
                .filter(Predicate.not(Set.of("", "default")::contains))
                .mapToInt(Integer::parseInt)
                .findAny();
        } catch (SecurityException _) {
            // ignored
        }

        // default to platform supported bitsize
        int vectorBitSize = VectorShape.preferredShape().vectorBitSize();
        // but allow easy overriding for testing
        PREFERRED_VECTOR_BITSIZE = vs.orElse(vectorBitSize);

        PREFERRED_BYTE_SPECIES = VectorSpecies.of(byte.class, VectorShape.forBitSize(PREFERRED_VECTOR_BITSIZE));
        PREFERRED_INTEGER_SPECIES = VectorSpecies.of(int.class, VectorShape.forBitSize(PREFERRED_VECTOR_BITSIZE));
        PREFERRED_FLOAT_SPECIES = VectorSpecies.of(float.class, VectorShape.forBitSize(PREFERRED_VECTOR_BITSIZE));

        // hotspot misses some SSE intrinsics, workaround it
        // to be fair, they do document this thing only works well with AVX2/AVX3 and Neon
        boolean isAMD64withoutAVX2 = Constants.OS_ARCH.equals("amd64") && PREFERRED_VECTOR_BITSIZE < 256;
        ENABLE_INTEGER_VECTORS = (isAMD64withoutAVX2 == false) || vs.isPresent();
    }

    private PanamaVectorConstants() {}
}
