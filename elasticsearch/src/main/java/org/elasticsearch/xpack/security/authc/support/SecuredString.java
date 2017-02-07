/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.support;

import org.elasticsearch.ElasticsearchException;

import java.nio.CharBuffer;
import java.util.Arrays;
import java.util.Objects;

/**
 * This is not a string but a CharSequence that can be cleared of its memory.  Important for handling passwords.
 *
 * Not thread safe There is a chance that the chars could be cleared while doing operations on the chars.
 * <p>
 * TODO: dot net's SecureString implementation does some obfuscation of the password to prevent gleaming passwords
 * from memory dumps.  (this is hard as dot net uses windows system crypto.  Thats probably the reason java still doesn't have it)
 */
public class SecuredString implements CharSequence, AutoCloseable {

    public static final SecuredString EMPTY = new SecuredString(new char[0]);

    private final char[] chars;
    private boolean cleared = false;

    /**
     * Creates a new SecuredString from the chars. These chars will be cleared when the SecuredString is closed so they should not be
     * used directly outside of using the SecuredString
     */
    public SecuredString(char[] chars) {
        this.chars = Objects.requireNonNull(chars, "chars must not be null!");
    }

    /**
     * This constructor is used internally for the concatenate method.
     */
    private SecuredString(char[] chars, int start, int end) {
        this.chars = new char[end - start];
        System.arraycopy(chars, start, this.chars, 0, this.chars.length);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null) return false;
        // Require password to calculate equals
        if (this == EMPTY || o == EMPTY) return false;

        if (o instanceof SecuredString) {
            SecuredString that = (SecuredString) o;

            if (cleared != that.cleared) return false;
            if (!Arrays.equals(chars, that.chars)) return false;

            return true;
        } else if (o instanceof CharSequence) {
            CharSequence that = (CharSequence) o;
            if (cleared) return false;
            if (chars.length != that.length()) return false;

            for (int i = 0; i < chars.length; i++) {
                if (chars[i] != that.charAt(i)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(chars);
        result = 31 * result + (cleared ? 1 : 0);
        return result;
    }

    /**
     * Note: This is a dangerous call that exists for performance/optimization
     * DO NOT modify the array returned by this method and DO NOT cache it (as it will be cleared).
     *
     * To clear the array call SecureString.clear().
     *
     * @return the internal characters that MUST NOT be cleared manually
     */
    public char[] internalChars() {
        throwIfCleared();
        return chars;
    }

    /**
     * @return  A copy of the internal characters. May be used for caching.
     */
    public char[] copyChars() {
        throwIfCleared();
        return Arrays.copyOf(chars, chars.length);
    }

    /**
     * @return utf8 encoded bytes
     */
    public byte[] utf8Bytes() {
        throwIfCleared();
        return CharArrays.toUtf8Bytes(chars);
    }

    @Override
    public int length() {
        throwIfCleared();
        return chars.length;
    }

    @Override
    public char charAt(int index) {
        throwIfCleared();
        return chars[index];
    }

    @Override
    public SecuredString subSequence(int start, int end) {
        throwIfCleared();
        return new SecuredString(this.chars, start, end);
    }

    /**
     * Manually clear the underlying array holding the characters
     */
    public void clear() {
        cleared = true;
        Arrays.fill(chars, (char) 0);
    }

    /**
     * @param toAppend String to combine with this SecureString
     * @return a new SecureString with toAppend concatenated
     */
    public SecuredString concat(CharSequence toAppend) {
        throwIfCleared();

        CharBuffer buffer = CharBuffer.allocate(chars.length + toAppend.length());
        buffer.put(chars);
        for (int i = 0; i < toAppend.length(); i++) {
            buffer.put(i + chars.length, toAppend.charAt(i));
        }
        return new SecuredString(buffer.array());
    }

    private void throwIfCleared() {
        if (cleared) {
            throw new ElasticsearchException("attempt to use cleared password");
        }
    }

    /**
     * This does a char by char comparison of the two Strings to provide protection against timing attacks. In other
     * words it does not exit at the first character that does not match and only exits at the end of the comparison.
     *
     * NOTE: length will cause this function to exit early, which is OK as it is not considered feasible to prevent
     * length attacks
     *
     * @param a the first string to be compared
     * @param b the second string to be compared
     * @return true if both strings match completely
     */
    public static boolean constantTimeEquals(String a, String b) {
        char[] aChars = a.toCharArray();
        char[] bChars = b.toCharArray();

        return constantTimeEquals(aChars, bChars);
    }

    /**
     * This does a char by char comparison of the two Strings to provide protection against timing attacks. In other
     * words it does not exit at the first character that does not match and only exits at the end of the comparison.
     *
     * NOTE: length will cause this function to exit early, which is OK as it is not considered feasible to prevent
     * length attacks
     *
     * @param securedString the securedstring to compare to string char by char
     * @param string the string to compare
     * @return true if both match char for char
     */
    public static boolean constantTimeEquals(SecuredString securedString, String string) {
        return constantTimeEquals(securedString.internalChars(), string.toCharArray());
    }

    /**
     * This does a char by char comparison of the two Strings to provide protection against timing attacks. In other
     * words it does not exit at the first character that does not match and only exits at the end of the comparison.
     *
     * NOTE: length will cause this function to exit early, which is OK as it is not considered feasible to prevent
     * length attacks
     *
     * @param securedString the securedstring to compare to string char by char
     * @param otherString the other securedstring to compare
     * @return true if both match char for char
     */
    public static boolean constantTimeEquals(SecuredString securedString, SecuredString otherString) {
        return constantTimeEquals(securedString.internalChars(), otherString.internalChars());
    }

    /**
     * This does a char by char comparison of the two arrays to provide protection against timing attacks. In other
     * words it does not exit at the first character that does not match and only exits at the end of the comparison.
     *
     * NOTE: length will cause this function to exit early, which is OK as it is not considered feasible to prevent
     * length attacks
     *
     * @param a the first char array
     * @param b the second char array
     * @return true if both match char for char
     */
    public static boolean constantTimeEquals(char[] a, char[] b) {
        if (a.length != b.length) {
            return false;
        }

        int equals = 0;
        for (int i = 0; i < a.length; i++) {
            equals |= a[i] ^ b[i];
        }

        return equals == 0;
    }

    @Override
    public void close() {
        clear();
    }
}
