/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.inference.common;

import org.elasticsearch.common.unit.ByteSizeValue;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An input stream throwing an exception when a preconfigured number of bytes is reached
 * This input stream exists to prevent reading streaming or very big requests
 *
 * This implementation does not support mark/reset to prevent complex byte counting recalculations
 */
public final class SizeLimitInputStream extends FilterInputStream {

    private final long maxByteSize;
    private final AtomicLong byteCounter = new AtomicLong(0);

    /**
     * Creates a new input stream, that throws an exception after a certain number of bytes is read
     * @param maxByteSize The maximum data to read, before throwing an exception
     * @param in The underlying input stream containing the data
     */
    public SizeLimitInputStream(ByteSizeValue maxByteSize, InputStream in) {
        super(in);
        this.maxByteSize = maxByteSize.getBytes();
    }

    @Override
    public int read() throws IOException {
        int bytesRead = super.read();

        if (bytesRead != -1) {
            byteCounter.incrementAndGet();
            checkMaximumLengthReached();
        }

        return bytesRead;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int bytesRead = super.read(b, off, len);

        if (bytesRead != -1) {
            byteCounter.addAndGet(len);
            checkMaximumLengthReached();
        }

        return bytesRead;
    }

    @Override
    public synchronized void mark(int readlimit) {
        throw new UnsupportedOperationException("mark not supported");
    }

    @Override
    public synchronized void reset() throws IOException {
        throw new IOException("reset not supported");
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    private void checkMaximumLengthReached() throws IOException {
        if (byteCounter.get() > maxByteSize) {
            throw new IOException("Maximum limit of [" + maxByteSize + "] bytes reached");
        }
    }
}
