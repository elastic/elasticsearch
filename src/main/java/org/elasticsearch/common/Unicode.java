/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.common;

import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.common.util.concurrent.ThreadLocals;

import java.util.Arrays;

/**
 *
 */
public class Unicode {

    private static ThreadLocal<ThreadLocals.CleanableValue<UnicodeUtil.UTF8Result>> cachedUtf8Result = new ThreadLocal<ThreadLocals.CleanableValue<UnicodeUtil.UTF8Result>>() {
        @Override
        protected ThreadLocals.CleanableValue<UnicodeUtil.UTF8Result> initialValue() {
            return new ThreadLocals.CleanableValue<UnicodeUtil.UTF8Result>(new UnicodeUtil.UTF8Result());
        }
    };

    private static ThreadLocal<ThreadLocals.CleanableValue<UTF16Result>> cachedUtf16Result = new ThreadLocal<ThreadLocals.CleanableValue<UTF16Result>>() {
        @Override
        protected ThreadLocals.CleanableValue<UTF16Result> initialValue() {
            return new ThreadLocals.CleanableValue<UTF16Result>(new UTF16Result());
        }
    };

    public static byte[] fromStringAsBytes(String source) {
        if (source == null) {
            return null;
        }
        UnicodeUtil.UTF8Result result = unsafeFromStringAsUtf8(source);
        return Arrays.copyOfRange(result.result, 0, result.length);
    }

    public static UnicodeUtil.UTF8Result fromStringAsUtf8(String source) {
        if (source == null) {
            return null;
        }
        UnicodeUtil.UTF8Result result = new UnicodeUtil.UTF8Result();
        UnicodeUtil.UTF16toUTF8(source, 0, source.length(), result);
        return result;
    }

    public static void fromStringAsUtf8(String source, UnicodeUtil.UTF8Result result) {
        if (source == null) {
            result.length = 0;
            return;
        }
        UnicodeUtil.UTF16toUTF8(source, 0, source.length(), result);
    }

    public static UnicodeUtil.UTF8Result unsafeFromStringAsUtf8(String source) {
        if (source == null) {
            return null;
        }
        UnicodeUtil.UTF8Result result = cachedUtf8Result.get().get();
        UnicodeUtil.UTF16toUTF8(source, 0, source.length(), result);
        return result;
    }

    public static String fromBytes(byte[] source) {
        return fromBytes(source, 0, source.length);
    }

    public static String fromBytes(byte[] source, int offset, int length) {
        if (source == null) {
            return null;
        }
        UTF16Result result = unsafeFromBytesAsUtf16(source, offset, length);
        return new String(result.result, 0, result.length);
    }

    public static UTF16Result fromBytesAsUtf16(byte[] source) {
        return fromBytesAsUtf16(source, 0, source.length);
    }

    public static UTF16Result fromBytesAsUtf16(byte[] source, int offset, int length) {
        if (source == null) {
            return null;
        }
        UTF16Result result = new UTF16Result();
        UTF8toUTF16(source, offset, length, result);
        return result;
    }

    public static UTF16Result unsafeFromBytesAsUtf16(byte[] source) {
        return unsafeFromBytesAsUtf16(source, 0, source.length);
    }

    public static UTF16Result unsafeFromBytesAsUtf16(byte[] source, int offset, int length) {
        if (source == null) {
            return null;
        }
        UTF16Result result = cachedUtf16Result.get().get();
        UTF8toUTF16(source, offset, length, result);
        return result;
    }

    // LUCENE MONITOR

    // an optimized version of UTF16Result that does not hold the offsets since we don't need them
    // they are only used with continuous writing to the same utf16 (without "clearing it")

    public static final class UTF16Result {
        public char[] result = new char[10];
        //        public int[] offsets = new int[10];
        public int length;

        public void setLength(int newLength) {
            if (result.length < newLength) {
                char[] newArray = new char[(int) (1.5 * newLength)];
                System.arraycopy(result, 0, newArray, 0, length);
                result = newArray;
            }
            length = newLength;
        }

        public void copyText(UTF16Result other) {
            setLength(other.length);
            System.arraycopy(other.result, 0, result, 0, length);
        }
    }


    /**
     * Convert UTF8 bytes into UTF16 characters.  If offset
     * is non-zero, conversion starts at that starting point
     * in utf8, re-using the results from the previous call
     * up until offset.
     */
    public static void UTF8toUTF16(final byte[] utf8, final int offset, final int length, final UTF16Result result) {

        final int end = offset + length;
        char[] out = result.result;
//        if (result.offsets.length <= end) {
//            int[] newOffsets = new int[2 * end];
//            System.arraycopy(result.offsets, 0, newOffsets, 0, result.offsets.length);
//            result.offsets = newOffsets;
//        }
//        final int[] offsets = result.offsets;

        // If incremental decoding fell in the middle of a
        // single unicode character, rollback to its start:
        int upto = offset;
//        while (offsets[upto] == -1)
//            upto--;

        int outUpto = 0; // offsets[upto];

        // Pre-allocate for worst case 1-for-1
        if (outUpto + length >= out.length) {
            char[] newOut = new char[2 * (outUpto + length)];
            System.arraycopy(out, 0, newOut, 0, outUpto);
            result.result = out = newOut;
        }

        while (upto < end) {

            final int b = utf8[upto] & 0xff;
            final int ch;

            upto += 1; // CHANGE
//            offsets[upto++] = outUpto;

            if (b < 0xc0) {
                assert b < 0x80;
                ch = b;
            } else if (b < 0xe0) {
                ch = ((b & 0x1f) << 6) + (utf8[upto] & 0x3f);
                upto += 1; // CHANGE
//                offsets[upto++] = -1;
            } else if (b < 0xf0) {
                ch = ((b & 0xf) << 12) + ((utf8[upto] & 0x3f) << 6) + (utf8[upto + 1] & 0x3f);
                upto += 2; // CHANGE
//                offsets[upto++] = -1;
//                offsets[upto++] = -1;
            } else {
                assert b < 0xf8;
                ch = ((b & 0x7) << 18) + ((utf8[upto] & 0x3f) << 12) + ((utf8[upto + 1] & 0x3f) << 6) + (utf8[upto + 2] & 0x3f);
                upto += 3; // CHANGE
//                offsets[upto++] = -1;
//                offsets[upto++] = -1;
//                offsets[upto++] = -1;
            }

            if (ch <= UNI_MAX_BMP) {
                // target is a character <= 0xFFFF
                out[outUpto++] = (char) ch;
            } else {
                // target is a character in range 0xFFFF - 0x10FFFF
                final int chHalf = ch - HALF_BASE;
                out[outUpto++] = (char) ((chHalf >> HALF_SHIFT) + UnicodeUtil.UNI_SUR_HIGH_START);
                out[outUpto++] = (char) ((chHalf & HALF_MASK) + UnicodeUtil.UNI_SUR_LOW_START);
            }
        }

//        offsets[upto] = outUpto;
        result.length = outUpto;
    }

    private static final long UNI_MAX_BMP = 0x0000FFFF;

    private static final int HALF_BASE = 0x0010000;
    private static final long HALF_SHIFT = 10;
    private static final long HALF_MASK = 0x3FFL;
}
