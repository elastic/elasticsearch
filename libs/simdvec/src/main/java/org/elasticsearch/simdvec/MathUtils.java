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
    // The number of explicit bits in the mantissa (fraction) of a 32-bit float.
    public static final int MANTISSA_BITS = 23;
    // The exponent bias used to shift negative/positive exponents into unsigned integers.
    public static final int EXPONENT_BIAS = 127;
    // The exponent bias used to shift negative/positive exponents into unsigned integers.
    public static final int EXPONENT_BIAS_MINUS_ONE = 126;
    // Bitmask to isolate the 8-bit exponent field.
    public static final int EXPONENT_MASK = 0xFF;
    // Bitmask to isolate the 23-bit mantissa field. (Evaluates to 0x007FFFFF)
    public static final int MANTISSA_MASK = (1 << MANTISSA_BITS) - 1;

    /** Computes the squared root of x. */
    public static float sqrt(float x) {
        return (float) Math.sqrt(x);
    }

    /** Computes log2(x) using the NQT approximation. */
    public static float log2NQT(float x) {
        // Since we are applying a logarithm, the sign needs to be positive
        int bits = Float.floatToIntBits(x);
        int exponent = ((bits >> MANTISSA_BITS) & EXPONENT_MASK) - EXPONENT_BIAS_MINUS_ONE;

        // Build the mantissa directly via bits.
        // 0x007FFFFF clears the original sign and exponent.
        // (126 << (Float.PRECISION - 1)) forces the exponent to exactly 126, placing the value in [0.5, 1).
        int mantissaBits = (bits & MANTISSA_MASK) | (EXPONENT_BIAS_MINUS_ONE << MANTISSA_BITS);
        float mantissa = Float.intBitsToFloat(mantissaBits);
        return 2.0f * (mantissa - 1.0f) + exponent;
    }

    /** Computes pow(2, exponent) using the NQT approximation. */
    public static float pow2NQT(float exponent) {
        int p = (int) Math.floor(exponent + 1.0f);
        p = Math.clamp(p, -30, 30);
        float m = (exponent - p) * 0.5f + 1.0f;
        // Build 2^p directly.
        float powerOf2 = Float.intBitsToFloat((p + EXPONENT_BIAS) << MANTISSA_BITS);
        return Math.max(m * powerOf2, 0.0f);
    }
}
