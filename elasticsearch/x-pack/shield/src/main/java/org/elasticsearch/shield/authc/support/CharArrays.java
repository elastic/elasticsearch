/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.support;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Helper class similar to Arrays to handle conversions for Char arrays
 */
public class CharArrays {

    public static char[] utf8BytesToChars(byte[] utf8Bytes) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(utf8Bytes);
        CharBuffer charBuffer = StandardCharsets.UTF_8.decode(byteBuffer);
        char[] chars = Arrays.copyOfRange(charBuffer.array(), charBuffer.position(), charBuffer.limit());
        byteBuffer.clear();
        charBuffer.clear();
        return chars;
    }

    /**
     * Like String.indexOf for for an array of chars
     */
    static int indexOf(char[] array, char ch) {
        for (int i = 0; (i < array.length); i++) {
            if (array[i] == ch) {
                return i;
            }
        }
        return -1;
    }

    public static byte[] toUtf8Bytes(char[] chars) {
        CharBuffer charBuffer = CharBuffer.wrap(chars);
        ByteBuffer byteBuffer = StandardCharsets.UTF_8.encode(charBuffer);
        byte[] bytes = Arrays.copyOfRange(byteBuffer.array(), byteBuffer.position(), byteBuffer.limit());
        Arrays.fill(byteBuffer.array(), (byte) 0); // clear sensitive data
        return bytes;
    }

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
}
