/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.compress.lz4;

import net.jpountz.util.Utils;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

public enum SafeUtils {
    ;

    private static final VarHandle asIntBigEndian = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);

    private static final VarHandle asIntLittleEndian = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);

    private static final VarHandle asShortLittleEndian = MethodHandles.byteArrayViewVarHandle(short[].class, ByteOrder.LITTLE_ENDIAN);

    public static void checkRange(byte[] buf, int off) {
        if (off < 0 || off >= buf.length) {
            throw new ArrayIndexOutOfBoundsException(off);
        }
    }

    public static void checkRange(byte[] buf, int off, int len) {
        checkLength(len);
        if (len > 0) {
            checkRange(buf, off);
            checkRange(buf, off + len - 1);
        }
    }

    public static void checkLength(int len) {
        if (len < 0) {
            throw new IllegalArgumentException("lengths must be >= 0");
        }
    }

    public static byte readByte(byte[] buf, int i) {
        return buf[i];
    }

    public static int readIntBE(byte[] buf, int i) {
        return (int) asIntBigEndian.get(buf, i);
    }

    public static int readIntLE(byte[] buf, int i) {
        return (int) asIntLittleEndian.get(buf, i);
    }

    public static int readInt(byte[] buf, int i) {
        if (Utils.NATIVE_BYTE_ORDER == ByteOrder.BIG_ENDIAN) {
            return readIntBE(buf, i);
        } else {
            return readIntLE(buf, i);
        }
    }

    public static long readLongLE(byte[] buf, int i) {
        return (buf[i] & 0xFFL) | ((buf[i+1] & 0xFFL) << 8) | ((buf[i+2] & 0xFFL) << 16) | ((buf[i+3] & 0xFFL) << 24)
            | ((buf[i+4] & 0xFFL) << 32) | ((buf[i+5] & 0xFFL) << 40) | ((buf[i+6] & 0xFFL) << 48) | ((buf[i+7] & 0xFFL) << 56);
    }

    public static void writeShortLE(byte[] buf, int off, int v) {
        asShortLittleEndian.set(buf, off, (short) v);
    }

    public static void writeInt(int[] buf, int off, int v) {
        buf[off] = v;
    }

    public static int readInt(int[] buf, int off) {
        return buf[off];
    }

    public static void writeByte(byte[] dest, int off, int i) {
        dest[off] = (byte) i;
    }

    public static void writeShort(short[] buf, int off, int v) {
        buf[off] = (short) v;
    }

    public static int readShortLE(byte[] buf, int i) {
        return (short) asShortLittleEndian.get(buf, i);
    }

    public static int readShort(short[] buf, int off) {
        return buf[off] & 0xFFFF;
    }
}
