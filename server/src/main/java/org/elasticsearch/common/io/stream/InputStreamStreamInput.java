/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io.stream;

import org.elasticsearch.common.io.Streams;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

public class InputStreamStreamInput extends StreamInput {

    private final InputStream is;
    private final long sizeLimit;

    /**
     * Creates a new InputStreamStreamInput with unlimited size
     * @param is the input stream to wrap
     */
    public InputStreamStreamInput(InputStream is) {
        this(is, Long.MAX_VALUE);
    }

    /**
     * Creates a new InputStreamStreamInput with a size limit
     * @param is the input stream to wrap
     * @param sizeLimit a hard limit of the number of bytes in the given input stream. This is used for internal input validation
     */
    public InputStreamStreamInput(InputStream is, long sizeLimit) {
        this.is = is;
        if (sizeLimit < 0) {
            throw new IllegalArgumentException("size limit must be positive");
        }
        this.sizeLimit = sizeLimit;

    }

    @Override
    public byte readByte() throws IOException {
        int ch = is.read();
        if (ch < 0) throw new EOFException();
        return (byte) (ch);
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        if (len < 0) throw new IndexOutOfBoundsException();
        final int read = Streams.readFully(is, b, offset, len);
        if (read != len) {
            throw new EOFException();
        }
    }

    @Override
    public void reset() throws IOException {
        is.reset();
    }

    @Override
    public boolean markSupported() {
        return is.markSupported();
    }

    @Override
    public void mark(int readlimit) {
        is.mark(readlimit);
    }

    @Override
    public void close() throws IOException {
        is.close();
    }

    @Override
    public int available() throws IOException {
        return is.available();
    }

    @Override
    public int read() throws IOException {
        return is.read();
    }

    @Override
    public int read(byte[] b) throws IOException {
        return is.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return is.read(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
        return is.skip(n);
    }

    @Override
    protected void ensureCanReadBytes(int length) throws EOFException {
        if (length > sizeLimit) {
            throw new EOFException("tried to read: " + length + " bytes but this stream is limited to: " + sizeLimit);
        }
    }
}
