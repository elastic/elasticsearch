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
        // 8 bits for exponent, stored in bits 23 to 30.
        // The exponent is stored in biased form, so we need to subtract the bias=127.
        // Since the format defines the mantissa in [1, 2), and we want it in [0.5, 1), we subtract 126 instead of 127.
        int exponent = ((bits >> 23) & 0xFF) - 126;
        float mantissa = x / (1 << exponent);
        return 2 * (mantissa - 1) + exponent;
    }

    /** Computes pow(2, exponent) using the NQT approximation. */
    public static float pow2NQT(float exponent) {
        int p = (int) Math.floor(exponent + 1);
        p = Math.clamp(p, -30, 30);
        float m = (exponent - p) / 2 + 1;
        float result;
        if (p >= 0) {
            result = m * (1 << p);
        } else  {
            result = m / (1 << Math.abs(p));
        }
        return Math.max(result, 0);
    }
}
