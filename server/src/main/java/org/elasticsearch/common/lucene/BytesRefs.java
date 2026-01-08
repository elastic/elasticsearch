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

    public static int codePointCount(BytesRef bytes) {
        int pos = bytes.offset;
        int limit = bytes.offset + bytes.length;
        int continuations = 0;

        for (; pos <= limit - 8; pos += 8) {
            long data = (long) BitUtil.VH_NATIVE_LONG.get(bytes.bytes, pos);
            long high = data & 0x8080808080808080L;
            if (high != 0) {
                // High bit is set in mask if first bit set and second bit not set
                // Matches bytes of form: 0b10......
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
