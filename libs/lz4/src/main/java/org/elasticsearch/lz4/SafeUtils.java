/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.lz4;

import net.jpountz.util.Utils;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

/*
 * This file is forked from https://github.com/lz4/lz4-java, which is licensed under Apache-2 and Copyright
 * 2020 Adrien Grand and the lz4-java contributors. In particular, it forks the following file
 * net.jpountz.lz4.SafeUtils.
 *
 * It modifies the original implementation to use Java9 varhandle performance improvements. Comments
 * are included to mark the changes.
 */
public enum SafeUtils {
    ;

    // Added VarHandle
    private static final VarHandle intPlatformNative = MethodHandles.byteArrayViewVarHandle(int[].class, Utils.NATIVE_BYTE_ORDER);

    private static final VarHandle shortLittleEndian = MethodHandles.byteArrayViewVarHandle(short[].class, ByteOrder.LITTLE_ENDIAN);

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

    // Deleted unused dedicated LE/BE readInt methods

    // Modified to use VarHandle
    public static int readInt(byte[] buf, int i) {
        return (int) intPlatformNative.get(buf, i);
    }

    // Unused in forked instance, no need to optimize
    public static long readLongLE(byte[] buf, int i) {
        return (buf[i] & 0xFFL) | ((buf[i + 1] & 0xFFL) << 8) | ((buf[i + 2] & 0xFFL) << 16) | ((buf[i + 3] & 0xFFL) << 24) | ((buf[i + 4]
            & 0xFFL) << 32) | ((buf[i + 5] & 0xFFL) << 40) | ((buf[i + 6] & 0xFFL) << 48) | ((buf[i + 7] & 0xFFL) << 56);
    }

    // Modified to use VarHandle
    public static void writeShortLE(byte[] buf, int off, int v) {
        shortLittleEndian.set(buf, off, (short) v);
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

    // Modified to use VarHandle
    public static int readShortLE(byte[] buf, int i) {
        return Short.toUnsignedInt((short) shortLittleEndian.get(buf, i));
    }

    public static int readShort(short[] buf, int off) {
        return buf[off] & 0xFFFF;
    }
}
