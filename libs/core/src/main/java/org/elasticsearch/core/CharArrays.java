/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.core;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
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
     */
    public static char[] utf8BytesToChars(byte[] utf8Bytes, int offset, int len) {
        final ByteBuffer byteBuffer = ByteBuffer.wrap(utf8Bytes, offset, len);
        final CharBuffer charBuffer = StandardCharsets.UTF_8.decode(byteBuffer);
        final char[] chars;
        if (charBuffer.hasArray()) {
            // there is no guarantee that the char buffers backing array is the right size
            // so we need to make a copy
            chars = Arrays.copyOfRange(charBuffer.array(), charBuffer.position(), charBuffer.limit());
            Arrays.fill(charBuffer.array(), (char) 0); // clear sensitive data
        } else {
            final int length = charBuffer.limit() - charBuffer.position();
            chars = new char[length];
            charBuffer.get(chars);
            // if the buffer is not read only we can reset and fill with 0's
            if (charBuffer.isReadOnly() == false) {
                charBuffer.clear(); // reset
                for (int i = 0; i < charBuffer.limit(); i++) {
                    charBuffer.put((char) 0);
                }
            }
        }
        return chars;
    }

    /**
     * Encodes the provided char[] to a UTF-8 byte[]. This is done while avoiding
     * conversions to String. The provided char[] is not modified by this method, so
     * the caller needs to take care of clearing the value if it is sensitive.
     */
    public static byte[] toUtf8Bytes(char[] chars) {
        final CharBuffer charBuffer = CharBuffer.wrap(chars);
        final ByteBuffer byteBuffer = StandardCharsets.UTF_8.encode(charBuffer);
        final byte[] bytes;
        if (byteBuffer.hasArray()) {
            // there is no guarantee that the byte buffers backing array is the right size
            // so we need to make a copy
            bytes = Arrays.copyOfRange(byteBuffer.array(), byteBuffer.position(), byteBuffer.limit());
            Arrays.fill(byteBuffer.array(), (byte) 0); // clear sensitive data
        } else {
            final int length = byteBuffer.limit() - byteBuffer.position();
            bytes = new byte[length];
            byteBuffer.get(bytes);
            // if the buffer is not read only we can reset and fill with 0's
            if (byteBuffer.isReadOnly() == false) {
                byteBuffer.clear(); // reset
                for (int i = 0; i < byteBuffer.limit(); i++) {
                    byteBuffer.put((byte) 0);
                }
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
