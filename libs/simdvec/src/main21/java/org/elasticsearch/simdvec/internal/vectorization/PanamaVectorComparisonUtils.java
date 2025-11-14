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
public final class PanamaVectorComparisonUtils implements VectorComparisonUtils {

    public static PanamaVectorComparisonUtils INSTANCE = new PanamaVectorComparisonUtils();

    /** The preferred byte vector species for the current platform. */
    private static final VectorSpecies<Byte> BS = ByteVector.SPECIES_PREFERRED;
    private static final VectorSpecies<Long> LS = LongVector.SPECIES_PREFERRED;

    private PanamaVectorComparisonUtils() {}

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
