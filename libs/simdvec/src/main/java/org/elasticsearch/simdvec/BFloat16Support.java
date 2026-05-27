/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

/**
 * Basic BFloat16 conversions
 */
public class BFloat16Support {
    public static short floatToBFloat16(float f) {
        // this rounds towards even
        // zero - zero exp, zero fraction
        // denormal - zero exp, non-zero fraction
        // infinity - all-1 exp, zero fraction
        // NaN - all-1 exp, non-zero fraction

        // note that floatToIntBits doesn't maintain specific NaN values,
        // unlike floatToRawIntBits, but instead can return different NaN bit patterns.
        // this means that a NaN is unlikely to be turned into infinity by rounding

        int bits = Float.floatToIntBits(f);
        // with thanks to https://github.com/microsoft/onnxruntime Fp16Conversions
        int roundingBias = 0x7fff + ((bits >> 16) & 1);
        bits += roundingBias;
        return (short) (bits >> 16);
    }

    public static float truncateToBFloat16(float f) {
        return Float.intBitsToFloat(floatToBFloat16(f) << 16);
    }

    public static float bFloat16ToFloat(short bf) {
        return Float.intBitsToFloat(bf << 16);
    }
}
