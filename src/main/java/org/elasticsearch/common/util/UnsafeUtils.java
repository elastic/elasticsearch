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
        int len = b1.length;
        if (b2.length != len) {
            return false;
        }
        int o1 = b1.offset, o2 = b2.offset;
        while (len >= 8) {
            if (readLong(b1.bytes, o1) != readLong(b2.bytes, o2)) {
                return false;
            }
            len -= 8;
            o1 += 8;
            o2 += 8;
        }
        if (len >= 4) {
            if (readInt(b1.bytes, o1) != readInt(b2.bytes, o2)) {
                return false;
            }
            len -= 4;
            o1 += 4;
            o2 += 4;
        }
        if (len >= 2) {
            if (readShort(b1.bytes, o1) != readShort(b2.bytes, o2)) {
                return false;
            }
            len -= 2;
            o1 += 2;
            o2 += 2;
        }
        if (len == 1) {
            if (readByte(b1.bytes, o1) != readByte(b2.bytes, o2)) {
                return false;
            }
        } else {
            assert len == 0;
        }
        return true;
    }

}
