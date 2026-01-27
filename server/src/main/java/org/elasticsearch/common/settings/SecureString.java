/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.settings;

import org.elasticsearch.core.Releasable;

import java.util.Arrays;
import java.util.Objects;

/**
 * A String implementations which allows clearing the underlying char array.
 */
public final class SecureString implements CharSequence, Releasable {

    private char[] chars;

    /**
     * Constructs a new SecureString which controls the passed in char array.
     *
     * Note: When this instance is closed, the array will be zeroed out.
     */
    public SecureString(char[] chars) {
        this.chars = Objects.requireNonNull(chars);
    }

    /**
     * Constructs a new SecureString from an existing String.
     *
     * NOTE: This is not actually secure, since the provided String cannot be deallocated, but
     * this constructor allows for easy compatibility between new and old apis.
     *
     * @deprecated Only use for compatibility between deprecated string settings and new secure strings
     */
    @Deprecated
    public SecureString(String s) {
        this(s.toCharArray());
    }

    @Override
    public synchronized boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof CharSequence cs) {
            return equals(cs);
        } else {
            return false;
        }
    }

    /** Constant time equality to avoid potential timing attacks. */
    public synchronized boolean equals(CharSequence that) {
        ensureNotClosed();
        // This is intentional to make sure the check is constant time relative to the length of the comparison string
        return that != null && compareChars(0, that, 0, that.length()) && this.length() == that.length();
    }

    public boolean startsWith(CharSequence other) {
        ensureNotClosed();
        return compareChars(0, other, 0, other.length());
    }

    /**
     * Compare the characters in this {@code SecureString} from {@code thisOffset} to {@code thisOffset + len}
     * against that characters in {@code other} from {@code otherOffset} to {@code otherOffset + len}
     * <br />
     * This operation is performed in constant time (relative to {@code len})
     */
    public synchronized boolean regionMatches(int thisOffset, CharSequence other, int otherOffset, int len) {
        ensureNotClosed();
        if (otherOffset < 0 || thisOffset < 0) {
            // cannot start from a negative value
            return false;
        }

        // Perform some calculations as long to prevent overflow
        if (otherOffset + (long) len > other.length()) {
            // cannot compare a region that runs past the end of the comparison string
            // we don't check the length of our string because we want to run in constant time
            return false;
        }

        if (len < 0) {
            throw new IllegalArgumentException("length cannot be negative");
        }
        if (len == 0) {
            // zero length regions are always equals
            return true;
        }

        return compareChars(thisOffset, other, otherOffset, len);
    }

    /**
     * Constant time comparison of a range from {@link #chars} against an identical length range from {@code other}
     */
    private boolean compareChars(int thisOffset, CharSequence other, int otherOffset, int len) {
        assert len <= other.length() : "len is longer that comparison string: " + len + " vs " + other.length();

        int equals = 0;
        for (int i = 0; i < len; i++) {
            final char o = other.charAt(otherOffset + i);
            final int t = thisOffset + i < chars.length ? chars[thisOffset + i] : (Character.MAX_VALUE + 1);
            equals |= t ^ o;
        }

        return equals == 0;
    }

    @Override
    public synchronized int hashCode() {
        return Arrays.hashCode(chars);
    }

    @Override
    public synchronized int length() {
        ensureNotClosed();
        return chars.length;
    }

    @Override
    public synchronized char charAt(int index) {
        ensureNotClosed();
        return chars[index];
    }

    @Override
    public SecureString subSequence(int start, int end) {
        throw new UnsupportedOperationException("Cannot get subsequence of SecureString");
    }

    /**
     * Convert to a {@link String}. This should only be used with APIs that do not take {@link CharSequence}.
     */
    @Override
    public synchronized String toString() {
        return new String(chars);
    }

    /**
     * Closes the string by clearing the underlying char array.
     */
    @Override
    public synchronized void close() {
        if (chars != null) {
            Arrays.fill(chars, '\0');
            chars = null;
        }
    }

    /**
     * Returns a new copy of this object that is backed by its own char array. Closing the new instance has no effect on the instance it
     * was created from. This is useful for APIs which accept a char array and you want to be safe about the API potentially modifying the
     * char array. For example:
     *
     * <pre>
     *     try (SecureString copy = secureString.clone()) {
     *         // pass thee char[] to a external API
     *         PasswordAuthentication auth = new PasswordAuthentication(username, copy.getChars());
     *         ...
     *     }
     * </pre>
     */
    @Override
    public synchronized SecureString clone() {
        ensureNotClosed();
        return new SecureString(Arrays.copyOf(chars, chars.length));
    }

    /**
     * Returns the underlying char[]. This is a dangerous operation as the array may be modified while it is being used by other threads
     * or a consumer may modify the values in the array. For safety, it is preferable to use {@link #clone()} and pass its chars to the
     * consumer when the chars are needed multiple times.
     */
    public synchronized char[] getChars() {
        ensureNotClosed();
        return chars;
    }

    /** Throw an exception if this string has been closed, indicating something is trying to access the data after being closed. */
    private void ensureNotClosed() {
        if (chars == null) {
            throw new IllegalStateException("SecureString has already been closed");
        }
    }
}
