/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.text;

/**
 * A CharSequence that limits the number of characters that can be read from it.
 * {@link LimitExceededException} is thrown when the limit is exceeded.
 */
public class ReadLimitedCharSequence implements CharSequence {
    private final CharSequence wrapped;
    protected final int readLimitFactor;
    protected final int readLimit;
    protected int currentRead;

    public ReadLimitedCharSequence(CharSequence wrapped, int readLimitFactor) {
        if (readLimitFactor <= 0) throw new IllegalArgumentException("readLimitFactor must be greater than 0");
        this.wrapped = wrapped;
        this.readLimitFactor = readLimitFactor;
        this.readLimit = wrapped.length() * readLimitFactor;
    }

    /**
     * Exception thrown when the read limit is exceeded
     */
    public static class LimitExceededException extends IllegalStateException {
        private final int readLimit;

        public LimitExceededException(int readLimit) {
            super("Read limit exceeded: " + readLimit);
            this.readLimit = readLimit;
        }

        public int readLimit() {
            return readLimit;
        }
    }

    protected RuntimeException createLimitException() {
        return new LimitExceededException(readLimit);
    }

    @Override
    public int length() {
        return wrapped.length();
    }

    @Override
    public char charAt(int index) {
        if (++currentRead > readLimit) {
            throw createLimitException();
        }
        return wrapped.charAt(index);
    }

    @Override
    public boolean isEmpty() {
        return wrapped.isEmpty();
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        return new ReadLimitedCharSequence(wrapped.subSequence(start, end), readLimitFactor);
    }

    @Override
    public String toString() {
        return wrapped.toString();
    }
}
