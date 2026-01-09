/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.lucene;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;

public class BytesRefs {

    /**
     * Converts a value to a string, taking special care if its a {@link BytesRef} to call
     * {@link org.apache.lucene.util.BytesRef#utf8ToString()}.
     */
    public static String toString(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof BytesRef bytesRef) {
            return bytesRef.utf8ToString();
        }
        return value.toString();
    }

    /**
     * Converts an object value to BytesRef.
     */
    public static BytesRef toBytesRef(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof BytesRef bytesRef) {
            return bytesRef;
        }
        return new BytesRef(value.toString());
    }

    /**
     * Checks that the input is not longer than {@link IndexWriter#MAX_TERM_LENGTH}
     * @param input a BytesRef
     * @return the same BytesRef, if no exception has been thrown
     * @throws IllegalArgumentException if the input is too long
     */
    public static BytesRef checkIndexableLength(BytesRef input) {
        if (input.length > IndexWriter.MAX_TERM_LENGTH) {
            throw new IllegalArgumentException(
                "Term is longer than maximum indexable length, term starting with [" + safeStringPrefix(input, 10)
            );
        }
        return input;
    }

    /**
     * Converts a given string to a {@link BytesRef} object with an exactly sized byte array.
     * <p>
     * This method alternative method to the standard {@link BytesRef} constructor's allocates the
     * exact byte array size needed for the string. This is done by parsing the UTF-16 string two
     * times the first to estimate the array length and the second to copy the string value inside
     * the array.
     * </p>
     *
     * @param s the input string to convert
     * @return a BytesRef object representing the input string
     */
    public static BytesRef toExactSizedBytesRef(String s) {
        int l = s.length();
        byte[] b = new byte[UnicodeUtil.calcUTF16toUTF8Length(s, 0, l)];
        UnicodeUtil.UTF16toUTF8(s, 0, l, b);
        return new BytesRef(b, 0, b.length);
    }

    /**
     * Produces a UTF-string prefix of the input BytesRef.  If the prefix cutoff would produce
     * ill-formed UTF, it falls back to the hexadecimal representation.
     * @param input an input BytesRef
     * @return a String prefix
     */
    private static String safeStringPrefix(BytesRef input, int prefixLength) {
        BytesRef prefix = new BytesRef(input.bytes, input.offset, prefixLength);
        try {
            return prefix.utf8ToString();
        } catch (Exception e) {
            return prefix.toString();
        }
    }

    /**
     * Calculate the number of code points in a utf-8 encoded string stored in a BytesRef.
     * Use SWAR techniques to processes multiple bytes at once. Rather than counting leading
     * bytes which start with 0 or 11, we count continuation bytes which start with 10. Since
     * every byte which is not a continuation byte is the start of a code point, we subtract
     * the number of continuation bytes from the number of bytes to get the number of code points.
     *
     * Lucene's UnicodeUtil.codePointCount throws an error on some invalid Unicode strings. This method
     * never throws and thus assumes that all input strings are valid Unicode.
     *
     * @param bytes a value unicode string encoded in utf-8
     * @return the number of Unicode code points in the string
     */
    public static int fastCodePointCount(BytesRef bytes) {
        int pos = bytes.offset;
        int limit = bytes.offset + bytes.length;
        int continuations = 0;

        for (; pos <= limit - 8; pos += 8) {
            long data = (long) BitUtil.VH_NATIVE_LONG.get(bytes.bytes, pos);
            long high = data & 0x8080808080808080L;
            // If all bytes start with 0, they are all ascii, so the block can be skipped.
            if (high != 0) {
                // Set the high bit in `mask` if the high bit in data is set and the second bit is not set
                long mask = high & (~data << 1);
                continuations += Long.bitCount(mask);
            }
        }

        // Last 7 or fewer bytes
        while (pos < limit) {
            continuations += (bytes.bytes[pos] & 0xC0) == 0x80 ? 1 : 0;
            pos++;
        }

        return bytes.length - continuations;
    }
}
