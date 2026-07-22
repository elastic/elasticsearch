/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.bytes;

import org.apache.lucene.util.BytesRef;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

/**
 * A fast 64-bit hash using xor-multiply-shift mixing - processes 8 bytes per iteration
 */
public final class MixHash64 {
    private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder());
    private static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.nativeOrder());

    private long h = 0x9E3779B97F4A7C15L;
    private final byte[] buf = new byte[8];
    private int bufLen;
    private long totalLen;

    void update(byte[] bytes, int offset, int length) {
        totalLen += length;
        int pos = offset;
        int rem = length;
        if (bufLen > 0) {
            int need = 8 - bufLen;
            if (rem < need) {
                System.arraycopy(bytes, pos, buf, bufLen, rem);
                bufLen += rem;
                return;
            }
            System.arraycopy(bytes, pos, buf, bufLen, need);
            h = (h ^ (long) LONG_HANDLE.get(buf, 0)) * 0x4cf5ad432745937fL;
            pos += need;
            rem -= need;
            bufLen = 0;
        }
        while (rem >= 8) {
            h = (h ^ (long) LONG_HANDLE.get(bytes, pos)) * 0x4cf5ad432745937fL;
            pos += 8;
            rem -= 8;
        }
        if (rem > 0) {
            System.arraycopy(bytes, pos, buf, 0, rem);
            bufLen = rem;
        }
    }

    long finish() {
        int rem = bufLen;
        if (rem >= 4) {
            long lo = Integer.toUnsignedLong((int) INT_HANDLE.get(buf, 0));
            long hi = Integer.toUnsignedLong((int) INT_HANDLE.get(buf, rem - 4));
            h = (h ^ (lo | (hi << 32))) * 0x4cf5ad432745937fL;
        } else if (rem > 0) {
            long v = Byte.toUnsignedLong(buf[0]) | (Byte.toUnsignedLong(buf[rem >> 1]) << 8) | (Byte.toUnsignedLong(buf[rem - 1]) << 16);
            h = (h ^ v) * 0x4cf5ad432745937fL;
        }
        h ^= totalLen;
        h = (h ^ (h >>> 32)) * 0x4cd6944c5cc20b6dL;
        h = (h ^ (h >>> 29)) * 0xfc12c5b19d3259e9L;
        return h ^ (h >>> 32);
    }

    public static long hash64(byte[] bytes, int offset, int length) {
        long h = 0x9E3779B97F4A7C15L;
        int pos = offset;
        int rem = length;
        while (rem >= 8) {
            h = (h ^ (long) LONG_HANDLE.get(bytes, pos)) * 0x4cf5ad432745937fL;
            pos += 8;
            rem -= 8;
        }
        if (rem >= 4) {
            long lo = Integer.toUnsignedLong((int) INT_HANDLE.get(bytes, pos));
            long hi = Integer.toUnsignedLong((int) INT_HANDLE.get(bytes, pos + rem - 4));
            h = (h ^ (lo | (hi << 32))) * 0x4cf5ad432745937fL;
        } else if (rem > 0) {
            long v = Byte.toUnsignedLong(bytes[pos]) | (Byte.toUnsignedLong(bytes[pos + (rem >> 1)]) << 8) | (Byte.toUnsignedLong(
                bytes[pos + rem - 1]
            ) << 16);
            h = (h ^ v) * 0x4cf5ad432745937fL;
        }
        h ^= length;
        h = (h ^ (h >>> 32)) * 0x4cd6944c5cc20b6dL;
        h = (h ^ (h >>> 29)) * 0xfc12c5b19d3259e9L;
        return h ^ (h >>> 32);
    }

    public static long hash64(BytesRef bytes) {
        return hash64(bytes.bytes, bytes.offset, bytes.length);
    }
}
