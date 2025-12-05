/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal.vectorization;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorSpecies;

import org.elasticsearch.simdvec.VectorComparisonUtils;

/**
 * Utility class providing vectorized byte comparison operations using
 * the Panama Vector API (Incubator).
 *
 * <p>This class offers methods to perform efficient byte-wise comparisons
 * on arrays using SIMD instructions. It can produce bitmasks indicating
 * which bytes in a loaded block match a given value, and can also report
 * the size of the vector block being processed.</p>
 *
 * <p>Example usage:
 * <pre>{@code
 * byte[] data = ...;
 * byte target = 0x1F;
 *
 * for (int i = 0; i < data.length; i += VectorComparisonUtils.vectorLength()) {
 *     long mask = VectorComparisonUtils.equalMask(data, i, target);
 *     // Process mask...
 * }
 * }</pre>
 */
public final class PanamaVectorComparisonUtils {

    public static VectorComparisonUtils INSTANCE;

    /** The preferred byte vector species for the current platform. */
    static final VectorSpecies<Byte> BS = PanamaVectorConstants.PREFERRED_BYTE_SPECIES;
    static final VectorSpecies<Long> LS = PanamaVectorConstants.PREFERRED_LONG_SPECIES;

    static {
        if (PanamaVectorConstants.PREFERRED_VECTOR_BITSIZE == 128) {
            INSTANCE = new Panama128VectorComparisonUtils();
        } else {
            INSTANCE = new PanamaWideVectorComparisonUtils();
        }
    }

    // Implementation used for bit-widths > 128 - uses both ByteVector and LongVector.
    static final class PanamaWideVectorComparisonUtils implements VectorComparisonUtils {

        private PanamaWideVectorComparisonUtils() {}

        @Override
        public int byteVectorLanes() {
            return BS.length();
        }

        @Override
        public long equalMask(byte[] array, int offset, byte value) {
            return ByteVector.fromArray(BS, array, offset).eq(value).toLong();
        }

        @Override
        public int longVectorLanes() {
            return LS.length();
        }

        @Override
        public long equalMask(long[] array, int offset, long value) {
            return LongVector.fromArray(LS, array, offset).eq(value).toLong();
        }
    }

    // This implementation is used on narrow bit-widths to avoid LongVector.
    // Benchmarking shows that the scalar equivalent is more performant.
    static final class Panama128VectorComparisonUtils implements VectorComparisonUtils {

        private Panama128VectorComparisonUtils() {}

        @Override
        public int byteVectorLanes() {
            return BS.length();
        }

        @Override
        public long equalMask(byte[] array, int offset, byte value) {
            return ByteVector.fromArray(BS, array, offset).eq(value).toLong();
        }

        @Override
        public int longVectorLanes() {
            return 1;
        }

        @Override
        public long equalMask(long[] array, int offset, long value) {
            return array[offset] == value ? 1L : 0L;
        }
    }
}
