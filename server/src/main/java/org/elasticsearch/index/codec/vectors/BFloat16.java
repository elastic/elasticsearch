/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.elasticsearch.simdvec.BFloat16Support;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ShortBuffer;

public final class BFloat16 {

    public static final int BYTES = Short.BYTES;

    public static short floatToBFloat16(float f) {
        return BFloat16Support.floatToBFloat16(f);
    }

    public static float truncateToBFloat16(float f) {
        return BFloat16Support.truncateToBFloat16(f);
    }

    public static float bFloat16ToFloat(short bf) {
        return BFloat16Support.bFloat16ToFloat(bf);
    }

    public static void floatToBFloat16(float[] floats, ShortBuffer bFloats) {
        ESVectorUtil.floatToBFloat16(floats, bFloats);
    }

    public static void bFloat16ToFloat(byte[] bfBytes, float[] floats) {
        assert floats.length * 2 == bfBytes.length;
        ESVectorUtil.bFloat16ToFloat(ByteBuffer.wrap(bfBytes).order(ByteOrder.LITTLE_ENDIAN).asShortBuffer(), floats);
    }

    public static void bFloat16ToFloat(ShortBuffer bFloats, float[] floats) {
        ESVectorUtil.bFloat16ToFloat(bFloats, floats);
    }

    private BFloat16() {}
}
