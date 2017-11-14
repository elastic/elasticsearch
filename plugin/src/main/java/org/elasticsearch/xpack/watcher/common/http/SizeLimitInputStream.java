/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.common.http;

import org.elasticsearch.common.unit.ByteSizeValue;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An inputstream throwing an exception when a preconfigured number of bytes is reached
 * This inputstream exists to prevent reading streaming or very big requests
 *
 * This implementation does not support mark/reset to prevent complex byte counting recalculations
 */
final class SizeLimitInputStream extends FilterInputStream {

    private final int maxByteSize;
    private final AtomicInteger byteCounter = new AtomicInteger(0);

    /**
     * Creates a new input stream, that throws an exception when a certain number of bytes is read
     * @param maxByteSize The maximum data to read, before throwing an exception
     * @param in          The underlying inputstream containing the data
     */
    SizeLimitInputStream(ByteSizeValue maxByteSize, InputStream in) {
        super(in);
        this.maxByteSize = maxByteSize.bytesAsInt();
    }

    @Override
    public int read() throws IOException {
        byteCounter.incrementAndGet();
        checkMaximumLengthReached();
        return super.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        byteCounter.addAndGet(len);
        checkMaximumLengthReached();
        return super.read(b, off, len);
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
