/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.text;

import org.elasticsearch.common.Strings;

import java.io.IOException;
import java.io.Writer;
import java.util.Objects;

/**
 * A {@link Writer} that throws an exception if the string exceeds a specified size. Rather than
 * extending {@link java.io.StringWriter} which wraps a {@link java.lang.StringBuffer}, this class implements
 * the required logic directly and wraps a {@link java.lang.StringBuilder}. This avoids synchronization overhead,
 * but also means that this class is not thread safe.
 */
public final class SizeLimitingStringWriter extends Writer {

    public static class SizeLimitExceededException extends IllegalStateException {
        public SizeLimitExceededException(String message) {
            super(message);
        }
    }

    private final int sizeLimit;
    private final StringBuilder builder = new StringBuilder();

    public SizeLimitingStringWriter(int sizeLimit) {
        this.sizeLimit = sizeLimit;
    }

    private int limitSize(int additionalChars) {
        int neededSize = builder.length() + additionalChars;
        if (neededSize > sizeLimit) {
            return additionalChars - (neededSize - sizeLimit);
        }
        return additionalChars;
    }

    private void throwSizeLimitExceeded(int limitedChars, int requestedChars) {
        assert limitedChars < requestedChars;
        int bufLen = builder.length();
        int foundSize = bufLen - limitedChars + requestedChars; // reconstitute original
        String selection = builder.substring(0, Math.min(bufLen, 20));
        throw new SizeLimitExceededException(
            Strings.format("String [%s...] has size [%d] which exceeds the size limit [%d]", selection, foundSize, sizeLimit)
        );
    }

    @Override
    public void write(int c) {
        if (limitSize(1) != 1) {
            throwSizeLimitExceeded(0, 1);
        }
        builder.append((char) c);
    }

    // write(char[]) delegates to write(char[], int, int)

    @Override
    public void write(char[] cbuf, int off, int len) {
        Objects.checkFromIndexSize(off, len, cbuf.length);
        if (len == 0) {
            return;
        }
        int limitedLen = limitSize(len);
        if (limitedLen > 0) {
            builder.append(cbuf, off, limitedLen);
        }
        if (limitedLen != len) {
            throwSizeLimitExceeded(limitedLen, len);
        }
    }

    @Override
    public void write(String str) {
        this.write(str, 0, str.length());
    }

    @Override
    public void write(String str, int off, int len) {
        Objects.checkFromIndexSize(off, len, str.length());
        if (len == 0) {
            return;
        }
        int limitedLen = limitSize(len);
        if (limitedLen > 0) {
            builder.append(str, off, off + limitedLen);
        }
        if (limitedLen != len) {
            throwSizeLimitExceeded(limitedLen, len);
        }
    }

    @Override
    public void flush() throws IOException {
        // noop
    }

    @Override
    public void close() throws IOException {
        // noop
    }

    // append(...) delegates to write(...) methods

    @Override
    public String toString() {
        return builder.toString();
    }
}
