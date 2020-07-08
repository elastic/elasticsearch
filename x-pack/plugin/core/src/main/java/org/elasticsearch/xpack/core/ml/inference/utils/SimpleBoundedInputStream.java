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

    public static class StreamSizeExceededException extends IOException {
        private final long maxBytes;

        public StreamSizeExceededException(String message, long maxBytes) {
            super(message);
            this.maxBytes = maxBytes;
        }

        public long getMaxBytes() {
            return maxBytes;
        }
    }

    public SimpleBoundedInputStream(InputStream inputStream, long maxBytes) {
        this.in = ExceptionsHelper.requireNonNull(inputStream, "inputStream");
        if (maxBytes < 0) {
            throw new IllegalArgumentException("[maxBytes] must be greater than or equal to 0");
        }
        this.maxBytes = maxBytes;
    }

    /**
     * A simple wrapper around the injected input stream that restricts the total number of bytes able to be read.
     * @return The byte read.
     * @throws StreamSizeExceededException when byte limit is exceeded
     * @throws IOException on failure
     */
    @Override
    public int read() throws IOException {
        // We have reached the maximum, signal stream completion.
        if (numBytes >= maxBytes) {
            throw new StreamSizeExceededException("input stream exceeded maximum bytes of [" + maxBytes + "]", maxBytes);
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
