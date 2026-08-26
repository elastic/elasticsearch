/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Wraps an {@link InputStream} and counts the bytes successfully read. Used by the line-oriented
 * text-format readers to publish a {@code sizeInBytes} statistic for stream-only sources (bzip2,
 * zstd-streamed) whose {@link org.elasticsearch.xpack.esql.datasources.spi.StorageObject#length()}
 * throws {@code UnsupportedOperationException}.
 * <p>
 * The count reflects bytes consumed from the underlying stream, including bytes pulled into a
 * downstream buffer (e.g. {@code BufferedReader}'s 8 KiB block) regardless of whether the reader
 * subsequently propagates them. For a clean whole-file drain this equals the decompressed file
 * size; for partial drains (the cache gate refuses the write in that case) it is an over-estimate.
 */
public final class CountingInputStream extends FilterInputStream {

    private long bytesRead;
    private long markBytesRead;

    public CountingInputStream(InputStream in) {
        super(in);
    }

    @Override
    public int read() throws IOException {
        int b = super.read();
        if (b >= 0) {
            bytesRead++;
        }
        return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int n = super.read(b, off, len);
        if (n > 0) {
            bytesRead += n;
        }
        return n;
    }

    @Override
    public long skip(long n) throws IOException {
        long skipped = super.skip(n);
        if (skipped > 0) {
            bytesRead += skipped;
        }
        return skipped;
    }

    @Override
    public synchronized void mark(int readlimit) {
        super.mark(readlimit);
        markBytesRead = bytesRead;
    }

    @Override
    public synchronized void reset() throws IOException {
        super.reset();
        bytesRead = markBytesRead;
    }

    public long getBytesRead() {
        return bytesRead;
    }
}
