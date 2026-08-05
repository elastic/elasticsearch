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
        ESVectorUtil.floatToBFloat16(floats, 0, bfBytes, 0, floats.length, ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * Converts {@code floatCount} floats from {@code floats}, starting at {@code floatOffset},
     * and put the bfloats in {@code byteOrder} order into {@code bfBytes} starting at {@code bfOffset}.
     */
    public static void floatToBFloat16(float[] floats, int floatOffset, byte[] bfBytes, int bfOffset, int floatCount, ByteOrder byteOrder) {
        ESVectorUtil.floatToBFloat16(floats, floatOffset, bfBytes, bfOffset, floatCount, byteOrder);
    }

    public static void bFloat16ToFloat(byte[] bfBytes, float[] floats) {
        ESVectorUtil.bFloat16ToFloat(bfBytes, 0, floats, 0, floats.length, ByteOrder.LITTLE_ENDIAN);
    }

    public static void bFloat16ToFloat(ByteBuffer bfBytes, float[] floats) {
        int pos = bfBytes.position();
        bfBytes.position(pos + floats.length * 2);   // change the position now so it does a bounds check for us
        ESVectorUtil.bFloat16ToFloat(bfBytes.array(), bfBytes.arrayOffset() + pos, floats, 0, floats.length, bfBytes.order());
    }

    /**
     * Converts {@code floatCount} bfloats from {@code bfBytes} in {@code byteOrder} order, starting at {@code bfOffset},
     * and put the floats into {@code floats} starting at {@code floatOffset}.
     */
    public static void bFloat16ToFloat(byte[] bfBytes, int bfOffset, float[] floats, int floatOffset, int floatCount, ByteOrder byteOrder) {
        ESVectorUtil.bFloat16ToFloat(bfBytes, bfOffset, floats, floatOffset, floatCount, byteOrder);
    }

    private BFloat16() {}
}
