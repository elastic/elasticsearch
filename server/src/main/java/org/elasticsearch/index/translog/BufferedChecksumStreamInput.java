/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.translog;

import org.apache.lucene.store.BufferedChecksum;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.io.stream.FilterStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/**
 * Similar to Lucene's BufferedChecksumIndexInput, however this wraps a
 * {@link StreamInput} so anything read will update the checksum
 */
public final class BufferedChecksumStreamInput extends FilterStreamInput {
    private static final int SKIP_BUFFER_SIZE = 1024;
    private byte[] skipBuffer;
    private final Checksum digest;
    private final String source;

    public BufferedChecksumStreamInput(StreamInput in, String source, BufferedChecksumStreamInput reuse) {
        super(in);
        this.source = source;
        if (reuse == null ) {
            this.digest = new BufferedChecksum(new CRC32());
        } else {
            this.digest = reuse.digest;
            digest.reset();
            this.skipBuffer = reuse.skipBuffer;
        }
    }

    public BufferedChecksumStreamInput(StreamInput in, String source) {
        this(in, source, null);
    }

    public long getChecksum() {
        return this.digest.getValue();
    }

    @Override
    public byte readByte() throws IOException {
        final byte b = delegate.readByte();
        digest.update(b);
        return b;
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        delegate.readBytes(b, offset, len);
        digest.update(b, offset, len);
    }

    private static final ThreadLocal<byte[]> buffer = ThreadLocal.withInitial(() -> new byte[8]);

    @Override
    public short readShort() throws IOException {
        final byte[] buf = buffer.get();
        readBytes(buf, 0, 2);
        return Numbers.bytesToShort(buf, 0);
    }

    @Override
    public int readInt() throws IOException {
        final byte[] buf = buffer.get();
        readBytes(buf, 0, 4);
        return Numbers.bytesToInt(buf, 0);
    }

    @Override
    public long readLong() throws IOException {
        final byte[] buf = buffer.get();
        readBytes(buf, 0, 8);
        return Numbers.bytesToLong(buf, 0);
    }

    @Override
    public int readVInt() throws IOException {
        return readVIntSlow();
    }

    @Override
    public long readVLong() throws IOException {
        return readVLongSlow();
    }

    @Override
    public void reset() throws IOException {
        delegate.reset();
        digest.reset();
    }

    @Override
    public int read() throws IOException {
        return readByte() & 0xFF;
    }

    @Override
    public boolean markSupported() {
        return delegate.markSupported();
    }

    @Override
    public long skip(long numBytes) throws IOException {
        if (numBytes < 0) {
            throw new IllegalArgumentException("numBytes must be >= 0, got " + numBytes);
        }
        if (skipBuffer == null) {
            skipBuffer = new byte[SKIP_BUFFER_SIZE];
        }
        assert skipBuffer.length == SKIP_BUFFER_SIZE;
        long skipped = 0;
        for (; skipped < numBytes; ) {
            final int step = (int) Math.min(SKIP_BUFFER_SIZE, numBytes - skipped);
            readBytes(skipBuffer, 0, step);
            skipped += step;
        }
        return skipped;
    }

    @Override
    public synchronized void mark(int readlimit) {
        delegate.mark(readlimit);
    }

    public void resetDigest() {
        digest.reset();
    }

    public String getSource(){
        return source;
    }
}
