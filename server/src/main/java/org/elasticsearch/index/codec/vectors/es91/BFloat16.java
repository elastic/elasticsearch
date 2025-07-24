/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.es91;

class BFloat16 {

    public static final int BYTES = Short.BYTES;

    public static short floatToBFloat16(float f) {
        // TODO: maintain NaN if all NaN set bits are in removed section
        return (short)(Float.floatToIntBits(f) >>> 16);
    }

    public static float bFloat16ToFloat(short bf) {
        return Float.intBitsToFloat(bf << 16);
    }

    public static short[] floatToBFloat16(float[] f) {
        short[] bf = new short[f.length];
        for (int i=0; i<f.length; i++) {
            bf[i] = floatToBFloat16(f[i]);
        }
        return bf;
    }

    public static float[] bFloat16ToFloat(short[] bf) {
        float[] f = new float[bf.length];
        for (int i=0; i<bf.length; i++) {
            f[i] = bFloat16ToFloat(bf[i]);
        }
        return f;
    }
}
