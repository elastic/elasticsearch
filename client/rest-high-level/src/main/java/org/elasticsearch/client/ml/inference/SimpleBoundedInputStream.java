/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.inference;


import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/**
 * This is a pared down bounded input stream.
 * Only read is specifically enforced.
 */
final class SimpleBoundedInputStream extends InputStream {

    private final InputStream in;
    private final long maxBytes;
    private long numBytes;

    SimpleBoundedInputStream(InputStream inputStream, long maxBytes) {
        this.in = Objects.requireNonNull(inputStream, "inputStream");
        if (maxBytes < 0) {
            throw new IllegalArgumentException("[maxBytes] must be greater than or equal to 0");
        }
        this.maxBytes = maxBytes;
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
