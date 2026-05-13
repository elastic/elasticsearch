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

    public static void floatToBFloat16(float[] floats, byte[] bfBytes) {
        ESVectorUtil.floatToBFloat16(floats, 0, bfBytes, 0, floats.length);
    }

    /**
     * Converts {@code floatCount} floats from {@code floats}, starting at {@code floatOffset},
     * and put the bfloats in little-endian order into {@code bfBytes} starting at {@code bfOffset}.
     */
    public static void floatToBFloat16(float[] floats, int floatOffset, byte[] bfBytes, int bfOffset, int floatCount) {
        ESVectorUtil.floatToBFloat16(floats, floatOffset, bfBytes, bfOffset, floatCount);
    }

    public static void bFloat16ToFloat(byte[] bfBytes, float[] floats) {
        assert floats.length * 2 == bfBytes.length;
        ESVectorUtil.bFloat16ToFloat(ByteBuffer.wrap(bfBytes).order(ByteOrder.LITTLE_ENDIAN).asShortBuffer(), floats);
    }

    public static void bFloat16ToFloat(ShortBuffer bFloats, float[] floats) {
        ESVectorUtil.bFloat16ToFloat(bFloats, floats);
    }

    public static void swapByteOrder(byte[] array) {
        assert array.length % 2 == 0;
        for (int i = 0; i < array.length; i += 2) {
            byte b = array[i];
            array[i] = array[i + 1];
            array[i + 1] = b;
        }
    }

    private BFloat16() {}
}
