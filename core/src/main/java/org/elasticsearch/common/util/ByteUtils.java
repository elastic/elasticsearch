/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.util;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;


/** Utility methods to do byte-level encoding. These methods are biased towards little-endian byte order because it is the most
 *  common byte order and reading several bytes at once may be optimizable in the future with the help of sun.mist.Unsafe. */
public enum ByteUtils {
    ;

    public static final int MAX_BYTES_VLONG = 9;

    /** Zig-zag decode. */
    public static long zigZagDecode(long n) {
        return ((n >>> 1) ^ -(n & 1));
    }

    /** Zig-zag encode: this helps transforming small signed numbers into small positive numbers. */
    public static long zigZagEncode(long n) {
        return (n >> 63) ^ (n << 1);
    }

    /** Write a long in little-endian format. */
    public static void writeLongLE(long l, byte[] arr, int offset) {
        for (int i = 0; i < 8; ++i) {
            arr[offset++] = (byte) l;
            l >>>= 8;
        }
        assert l == 0;
    }

    /** Write a long in little-endian format. */
    public static long readLongLE(byte[] arr, int offset) {
        long l = arr[offset++] & 0xFFL;
        for (int i = 1; i < 8; ++i) {
            l |= (arr[offset++] & 0xFFL) << (8 * i);
        }
        return l;
    }

    /** Write an int in little-endian format. */
    public static void writeIntLE(int l, byte[] arr, int offset) {
        for (int i = 0; i < 4; ++i) {
            arr[offset++] = (byte) l;
            l >>>= 8;
        }
        assert l == 0;
    }

    /** Read an int in little-endian format. */
    public static int readIntLE(byte[] arr, int offset) {
        int l = arr[offset++] & 0xFF;
        for (int i = 1; i < 4; ++i) {
            l |= (arr[offset++] & 0xFF) << (8 * i);
        }
        return l;
    }

    /** Write a double in little-endian format. */
    public static void writeDoubleLE(double d, byte[] arr, int offset) {
        writeLongLE(Double.doubleToRawLongBits(d), arr, offset);
    }

    /** Read a double in little-endian format. */
    public static double readDoubleLE(byte[] arr, int offset) {
        return Double.longBitsToDouble(readLongLE(arr, offset));
    }

    /** Write a float in little-endian format. */
    public static void writeFloatLE(float d, byte[] arr, int offset) {
        writeIntLE(Float.floatToRawIntBits(d), arr, offset);
    }

    /** Read a float in little-endian format. */
    public static float readFloatLE(byte[] arr, int offset) {
        return Float.intBitsToFloat(readIntLE(arr, offset));
    }

    /** Same as DataOutput#writeVLong but accepts negative values (written on 9 bytes). */
    public static void writeVLong(ByteArrayDataOutput out, long i) {
        for (int k = 0; k < 8 && (i & ~0x7FL) != 0L; ++k) {
            out.writeByte((byte)((i & 0x7FL) | 0x80L));
            i >>>= 7;
        }
        out.writeByte((byte)i);
    }

    /** Same as DataOutput#readVLong but can read negative values (read on 9 bytes). */
    public static long readVLong(ByteArrayDataInput in) {
        // unwinded because of hotspot bugs, see Lucene's impl
        byte b = in.readByte();
        if (b >= 0) return b;
        long i = b & 0x7FL;
        b = in.readByte();
        i |= (b & 0x7FL) << 7;
        if (b >= 0) return i;
        b = in.readByte();
        i |= (b & 0x7FL) << 14;
        if (b >= 0) return i;
        b = in.readByte();
        i |= (b & 0x7FL) << 21;
        if (b >= 0) return i;
        b = in.readByte();
        i |= (b & 0x7FL) << 28;
        if (b >= 0) return i;
        b = in.readByte();
        i |= (b & 0x7FL) << 35;
        if (b >= 0) return i;
        b = in.readByte();
        i |= (b & 0x7FL) << 42;
        if (b >= 0) return i;
        b = in.readByte();
        i |= (b & 0x7FL) << 49;
        if (b >= 0) return i;
        b = in.readByte();
        i |= (b & 0xFFL) << 56;
        return i;
    }

}
