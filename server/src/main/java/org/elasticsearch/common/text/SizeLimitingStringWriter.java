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

    private void checkSizeLimit(int additionalChars) {
        int bufLen = getBuffer().length();
        if (bufLen + additionalChars > sizeLimit) {
            throw new SizeLimitExceededException(
                Strings.format("String [%s...] has exceeded the size limit [%s]", getBuffer().substring(0, Math.min(bufLen, 20)), sizeLimit)
            );
        }
    }

    @Override
    public void write(int c) {
        checkSizeLimit(1);
        super.write(c);
    }

    // write(char[]) delegates to write(char[], int, int)

    @Override
    public void write(char[] cbuf, int off, int len) {
        checkSizeLimit(len);
        super.write(cbuf, off, len);
    }

    @Override
    public void write(String str) {
        checkSizeLimit(str.length());
        super.write(str);
    }

    @Override
    public void write(String str, int off, int len) {
        checkSizeLimit(len);
        super.write(str, off, len);
    }

    // append(...) delegates to write(...) methods
}
