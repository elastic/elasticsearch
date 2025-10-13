/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.util;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

public final class UrlCodecUtils {

    private UrlCodecUtils() {}

    private static final char[] HEX_DIGITS = "0123456789ABCDEF".toCharArray();

    public static BytesRef urlEncode(final BytesRef val, BreakingBytesRefBuilder scratch, final boolean plusForSpace) {
        int size = computeSizeAfterEncoding(val, plusForSpace);

        if (size == -1) {
            // the input doesn't change after encoding so encoding can be skipped
            return val;
        }

        scratch.grow(size);
        scratch.clear();

        int lo = val.offset;
        int hi = val.offset + val.length;

        for (int i = lo; i < hi; ++i) {
            byte b = val.bytes[i];
            char c = (char) (b & 0xFF);

            if (plusForSpace && c == ' ') {
                scratch.append((byte) '+');
                continue;
            }

            if (isRfc3986Safe(c)) {
                scratch.append(b);
                continue;
            }

            // every encoded byte is represented by 3 chars: %XY
            scratch.append((byte) '%');

            // the X in %XY is the hex value for the high nibble
            scratch.append((byte) HEX_DIGITS[(c >> 4) & 0x0F]);

            // the Y in %XY is the hex value for the low nibble
            scratch.append((byte) HEX_DIGITS[c & 0x0F]);
        }

        return scratch.bytesRefView();
    }

    /**
     * Determines whether a character is considered unreserved (or safe) according to RFC3986. Alphanumerics along with ".-_~" are safe,
     * and therefore not percent-encoded.
     *
     * @param c A character
     * @return Boolean
     */
    public static boolean isRfc3986Safe(char c) {
        return ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || ('0' <= c && c <= '9') || c == '-' || c == '.' || c == '_' || c == '~';
    }

    /**
     * <p>Computes the size of the input if it were encoded, and tells whether any encoding is needed at all. For example, if the input only
     * contained alphanumerics and safe characters, then -1 is returned, to mean that no encoding is needed. If the input additionally
     * contained spaces which can be encoded as '+', then the new size after encoding is returned.</p>
     *
     * <p>Examples</p>
     * <ul>
     *     <li>"abc" -> -1 (no encoding needed)</li>
     *     <li>"a b" ->  3 if encoding spaces as "+". The positive value indicates encoding is needed.</li>
     *     <li>"a b" ->  5 if encoding spaces as "%20". The positive value indicates encoding is needed.</li>
     *     <li>""    -> -1 (no encoding needed)</li>
     * </ul>
     *
     * @param val
     * @param plusForSpace Whether spaces are encoded as + or %20.
     * @return The new size after encoding, or -1 if no encoding is needed.
     */
    private static int computeSizeAfterEncoding(final BytesRef val, final boolean plusForSpace) {
        int size = 0;
        boolean noEncodingNeeded = true;

        int lo = val.offset;
        int hi = val.offset + val.length;

        for (int i = lo; i < hi; ++i) {
            char c = (char) (val.bytes[i] & 0xFF);

            if (plusForSpace && c == ' ') {
                ++size;
                noEncodingNeeded = false;
            } else if (isRfc3986Safe(c)) {
                ++size;
            } else {
                size += 3;
                noEncodingNeeded = false;
            }
        }

        if (noEncodingNeeded) {
            return -1;
        }

        return size;
    }

}
