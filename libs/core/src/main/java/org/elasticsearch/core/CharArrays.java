/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.core;

import java.util.Objects;

/**
 * Helper class similar to Arrays to handle conversions for Char arrays
 */
public final class CharArrays {

    private CharArrays() {}

    public static char[] utf8BytesToChars(byte[] utf8Bytes) {
        return utf8BytesToChars(utf8Bytes, 0, utf8Bytes.length);
    }

    /**
     * Decodes the provided byte[] to a UTF-8 char[]. This is done while avoiding
     * conversions to String. The provided byte[] is not modified by this method, so
     * the caller needs to take care of clearing the value if it is sensitive.
     * <p>
     * The conversion is done manually rather than via {@code StandardCharsets.UTF_8} so that the
     * value is only ever written to the exactly sized result array, never to intermediate buffers
     * that would need to be cleared. Each malformed byte decodes to {@code U+FFFD}.
     */
    public static char[] utf8BytesToChars(byte[] utf8Bytes, int offset, int len) {
        Objects.checkFromIndexSize(offset, len, utf8Bytes.length);
        final int end = offset + len;
        int charCount = 0;
        for (int i = offset; i < end;) {
            final int cp = codePointAt(utf8Bytes, i, end);
            i += cp < 0 ? 1 : utf8Length(cp);
            charCount += cp >= Character.MIN_SUPPLEMENTARY_CODE_POINT ? 2 : 1;
        }
        final char[] chars = new char[charCount];
        for (int i = offset, ci = 0; i < end;) {
            final int cp = codePointAt(utf8Bytes, i, end);
            if (cp < 0) {
                chars[ci++] = '\uFFFD';
                i++;
            } else {
                ci += Character.toChars(cp, chars, ci);
                i += utf8Length(cp);
            }
        }
        return chars;
    }

    /**
     * Returns the code point encoded at index {@code i}, or {@code -1} if the sequence at {@code i}
     * is not well-formed UTF-8 (this includes overlong forms, encoded surrogates, code points beyond
     * {@code U+10FFFF} and truncated sequences).
     */
    private static int codePointAt(byte[] bytes, int i, int end) {
        final int b1 = bytes[i] & 0xFF;
        if (b1 < 0x80) {
            return b1;
        }
        final int seqLength = b1 < 0xC2 ? 0 : b1 < 0xE0 ? 2 : b1 < 0xF0 ? 3 : b1 < 0xF5 ? 4 : 0;
        if (seqLength == 0 || i + seqLength > end) {
            return -1;
        }
        int cp = b1 & (0xFF >> (seqLength + 1)); // the lead byte's payload bits: 0x1F, 0x0F or 0x07
        for (int j = i + 1; j < i + seqLength; j++) {
            if ((bytes[j] & 0xC0) != 0x80) {
                return -1;
            }
            cp = (cp << 6) | (bytes[j] & 0x3F);
        }
        final int minCp = seqLength == 2 ? 0x80 : seqLength == 3 ? 0x800 : Character.MIN_SUPPLEMENTARY_CODE_POINT;
        if (cp < minCp || cp > Character.MAX_CODE_POINT || isSurrogateCodePoint(cp)) {
            return -1;
        }
        return cp;
    }

    private static int utf8Length(int cp) {
        return cp < 0x80 ? 1 : cp < 0x800 ? 2 : cp < 0x10000 ? 3 : 4;
    }

    private static boolean isSurrogateCodePoint(int cp) {
        return Character.MIN_SURROGATE <= cp && cp <= Character.MAX_SURROGATE;
    }

    /**
     * Encodes the provided char[] to a UTF-8 byte[]. This is done while avoiding
     * conversions to String. The provided char[] is not modified by this method, so
     * the caller needs to take care of clearing the value if it is sensitive.
     * <p>
     * The conversion is done manually rather than via {@code StandardCharsets.UTF_8} so that the
     * value is only ever written to the exactly sized result array, never to intermediate buffers
     * that would need to be cleared. Unpaired surrogates encode to {@code '?'}, as with
     * {@code StandardCharsets.UTF_8.encode}.
     */
    public static byte[] toUtf8Bytes(char[] chars) {
        int byteCount = 0;
        for (int i = 0; i < chars.length;) {
            final int cp = Character.codePointAt(chars, i);
            i += Character.charCount(cp);
            byteCount += isSurrogateCodePoint(cp) ? 1 : utf8Length(cp);
        }
        final byte[] bytes = new byte[byteCount];
        for (int i = 0, bi = 0; i < chars.length;) {
            final int cp = Character.codePointAt(chars, i);
            i += Character.charCount(cp);
            if (isSurrogateCodePoint(cp)) {
                bytes[bi++] = '?'; // unpaired surrogate
            } else if (cp < 0x80) {
                bytes[bi++] = (byte) cp;
            } else if (cp < 0x800) {
                bytes[bi++] = (byte) (0xC0 | (cp >> 6));
                bytes[bi++] = (byte) (0x80 | (cp & 0x3F));
            } else if (cp < 0x10000) {
                bytes[bi++] = (byte) (0xE0 | (cp >> 12));
                bytes[bi++] = (byte) (0x80 | ((cp >> 6) & 0x3F));
                bytes[bi++] = (byte) (0x80 | (cp & 0x3F));
            } else {
                bytes[bi++] = (byte) (0xF0 | (cp >> 18));
                bytes[bi++] = (byte) (0x80 | ((cp >> 12) & 0x3F));
                bytes[bi++] = (byte) (0x80 | ((cp >> 6) & 0x3F));
                bytes[bi++] = (byte) (0x80 | (cp & 0x3F));
            }
        }
        return bytes;
    }

    /**
     * Tests if a char[] contains a sequence of characters that match the prefix. This is like
     * {@link String#startsWith(String)} but does not require conversion of the char[] to a string.
     */
    public static boolean charsBeginsWith(String prefix, char[] chars) {
        if (chars == null || prefix == null) {
            return false;
        }

        if (prefix.length() > chars.length) {
            return false;
        }

        for (int i = 0; i < prefix.length(); i++) {
            if (chars[i] != prefix.charAt(i)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Constant time equality check of char arrays to avoid potential timing attacks.
     */
    public static boolean constantTimeEquals(char[] a, char[] b) {
        Objects.requireNonNull(a, "char arrays must not be null for constantTimeEquals");
        Objects.requireNonNull(b, "char arrays must not be null for constantTimeEquals");
        if (a.length != b.length) {
            return false;
        }

        int equals = 0;
        for (int i = 0; i < a.length; i++) {
            equals |= a[i] ^ b[i];
        }

        return equals == 0;
    }

    /**
     * Constant time equality check of strings to avoid potential timing attacks.
     */
    public static boolean constantTimeEquals(String a, String b) {
        Objects.requireNonNull(a, "strings must not be null for constantTimeEquals");
        Objects.requireNonNull(b, "strings must not be null for constantTimeEquals");
        if (a.length() != b.length()) {
            return false;
        }

        int equals = 0;
        for (int i = 0; i < a.length(); i++) {
            equals |= a.charAt(i) ^ b.charAt(i);
        }

        return equals == 0;
    }
}
