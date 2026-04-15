/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

public class MathUtils {
    /** Computes the squared root of x. */
    public static float sqrt(float x) {
        return (float) Math.sqrt(x);
    }

    /** Computes log2(x) using the NQT approximation. */
    public static float log2NQT(float x) {
        // Since we are applying a logarithm, the sign needs to be positive
        int bits = Float.floatToIntBits(x);
        int exponent = ((bits >> 23) & 0xFF) - 126;

        // Build the mantissa directly via bits.
        // 0x007FFFFF clears the original sign and exponent.
        // (126 << 23) forces the exponent to exactly 126, placing the value in [0.5, 1).
        int mantissaBits = (bits & 0x007FFFFF) | (126 << 23);
        float mantissa = Float.intBitsToFloat(mantissaBits);
        return 2.0f * (mantissa - 1.0f) + exponent;
    }

    /** Computes pow(2, exponent) using the NQT approximation. */
    public static float pow2NQT(float exponent) {
        int p = (int) Math.floor(exponent + 1.0f);
        p = Math.clamp(p, -30, 30);
        float m = (exponent - p) * 0.5f + 1.0f;
        // Build 2^p directly.
        float powerOf2 = Float.intBitsToFloat((p + 127) << 23);
        return Math.max(m * powerOf2, 0.0f);
    }
}
