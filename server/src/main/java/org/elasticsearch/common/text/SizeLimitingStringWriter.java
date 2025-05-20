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

import java.io.StringWriter;

/**
 * A {@link StringWriter} that throws an exception if the string exceeds a specified size.
 */
public class SizeLimitingStringWriter extends StringWriter {

    public static class SizeLimitExceededException extends IllegalStateException {
        public SizeLimitExceededException(String message) {
            super(message);
        }
    }

    private final int sizeLimit;

    public SizeLimitingStringWriter(int sizeLimit) {
        this.sizeLimit = sizeLimit;
    }

    private int limitSize(int additionalChars) {
        int neededSize = getBuffer().length() + additionalChars;
        if (neededSize > sizeLimit) {
            return additionalChars - (neededSize - sizeLimit);
        }
        return additionalChars;
    }

    private void throwSizeLimitExceeded(int limitedChars, int requestedChars) {
        assert limitedChars < requestedChars;
        int bufLen = getBuffer().length();
        int foundSize = bufLen - limitedChars + requestedChars; // reconstitute original
        String selection = getBuffer().substring(0, Math.min(bufLen, 20));
        throw new SizeLimitExceededException(
            Strings.format("String [%s...] has size [%d] which exceeds the size limit [%d]", selection, foundSize, sizeLimit)
        );
    }

    @Override
    public void write(int c) {
        if (limitSize(1) != 1) {
            throwSizeLimitExceeded(0, 1);
        }
        super.write(c);
    }

    // write(char[]) delegates to write(char[], int, int)

    @Override
    public void write(char[] cbuf, int off, int len) {
        int limitedLen = limitSize(len);
        if (limitedLen > 0) {
            super.write(cbuf, off, limitedLen);
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
        int limitedLen = limitSize(len);
        if (limitedLen > 0) {
            super.write(str, off, limitedLen);
        }
        if (limitedLen != len) {
            throwSizeLimitExceeded(limitedLen, len);
        }
    }

    // append(...) delegates to write(...) methods
}
