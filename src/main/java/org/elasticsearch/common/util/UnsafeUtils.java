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

import org.apache.lucene.util.BytesRef;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.ByteOrder;

/** Utility methods that use {@link Unsafe}. */
public enum UnsafeUtils {
    ;

    private static final Unsafe UNSAFE;
    private static final long BYTE_ARRAY_OFFSET;
    private static final int BYTE_ARRAY_SCALE;

    static {
        try {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            UNSAFE = (Unsafe) theUnsafe.get(null);
            BYTE_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
            BYTE_ARRAY_SCALE = UNSAFE.arrayIndexScale(byte[].class);
        } catch (IllegalAccessException e) {
            throw new ExceptionInInitializerError("Cannot access Unsafe");
        } catch (NoSuchFieldException e) {
            throw new ExceptionInInitializerError("Cannot access Unsafe");
        } catch (SecurityException e) {
            throw new ExceptionInInitializerError("Cannot access Unsafe");
        }
    }

    // Don't expose these methods directly, they are too easy to mis-use since they depend on the byte order.
    // If you need methods to read integers, please expose a method that makes the byte order explicit such
    // as readIntLE (little endian).

    // Also, please ***NEVER*** expose any method that writes using Unsafe, this is too dangerous

    private static long readLong(byte[] src, int offset) {
        return UNSAFE.getLong(src, BYTE_ARRAY_OFFSET + offset);
    }

    private static int readInt(byte[] src, int offset) {
        return UNSAFE.getInt(src, BYTE_ARRAY_OFFSET + offset);
    }

    private static short readShort(byte[] src, int offset) {
        return UNSAFE.getShort(src, BYTE_ARRAY_OFFSET + offset);
    }

    private static byte readByte(byte[] src, int offset) {
        return UNSAFE.getByte(src, BYTE_ARRAY_OFFSET + BYTE_ARRAY_SCALE * offset);
    }

    /** Compare the two given {@link BytesRef}s for equality. */
    public static boolean equals(BytesRef b1, BytesRef b2) {
        if (b1.length != b2.length) {
            return false;
        }
        return equals(b1.bytes, b1.offset, b2.bytes, b2.offset, b1.length);
    }

    /**
     * Compare <code>b1[offset1:offset1+length)</code>against <code>b1[offset2:offset2+length)</code>.
     */
    public static boolean equals(byte[] b1, int offset1, byte[] b2, int offset2, int length) {
        int o1 = offset1;
        int o2 = offset2;
        int len = length;
        while (len >= 8) {
            if (readLong(b1, o1) != readLong(b2, o2)) {
                return false;
            }
            len -= 8;
            o1 += 8;
            o2 += 8;
        }
        if (len >= 4) {
            if (readInt(b1, o1) != readInt(b2, o2)) {
                return false;
            }
            len -= 4;
            o1 += 4;
            o2 += 4;
        }
        if (len >= 2) {
            if (readShort(b1, o1) != readShort(b2, o2)) {
                return false;
            }
            len -= 2;
            o1 += 2;
            o2 += 2;
        }
        if (len == 1) {
            if (readByte(b1, o1) != readByte(b2, o2)) {
                return false;
            }
        } else {
            assert len == 0;
        }
        return true;
    }

    /**
     * Read a long using little endian byte order.
     */
    public static long readLongLE(byte[] src, int offset) {
        long value = readLong(src, offset);
        if (ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN) {
            value = Long.reverseBytes(value);
        }
        return value;
    }

    /**
     * Read an int using little endian byte order.
     */
    public static int readIntLE(byte[] src, int offset) {
        int value = readInt(src, offset);
        if (ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN) {
            value = Integer.reverseBytes(value);
        }
        return value;
    }

}
