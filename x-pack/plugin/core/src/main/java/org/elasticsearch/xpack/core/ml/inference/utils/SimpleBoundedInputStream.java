/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.utils;

import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.io.InputStream;

/**
 * This is a pared down bounded input stream.
 * Only read is specifically enforced.
 */
public final class SimpleBoundedInputStream extends InputStream {

    private final InputStream in;
    private final long maxBytes;
    private long numBytes;
    private final boolean throwWhenExceeded;

    public SimpleBoundedInputStream(InputStream inputStream, long maxBytes, boolean throwWhenExceeded) {
        this.in = ExceptionsHelper.requireNonNull(inputStream, "inputStream");
        if (maxBytes < 0) {
            throw new IllegalArgumentException("[maxBytes] must be greater than or equal to 0");
        }
        this.maxBytes = maxBytes;
        this.throwWhenExceeded = throwWhenExceeded;
    }

    /**
     * A simple wrapper around the injected input stream that restricts the total number of bytes able to be read.
     * @return The byte read. -1 on internal stream completion or when maxBytes is exceeded.
     * @throws IOException on failure
     */
    @Override
    public int read() throws IOException {
        // We have reached the maximum, signal stream completion.
        if (numBytes >= maxBytes) {
            if (throwWhenExceeded) {
                throw new IOException("input stream exceeded maximum bytes of [" + maxBytes +"]");
            }
            return -1;
        }
        numBytes++;
        return in.read();
    }

    /**
     * Delegates `close` to the wrapped InputStream
     * @throws IOException on failure
     */
    @Override
    public void close() throws IOException {
        in.close();
    }
}
