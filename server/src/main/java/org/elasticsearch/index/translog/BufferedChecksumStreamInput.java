/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.translog;

import org.apache.lucene.store.BufferedChecksum;
import org.elasticsearch.common.io.stream.FilterStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.internal.io.Streams;

import java.io.IOException;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/**
 * Similar to Lucene's BufferedChecksumIndexInput, however this wraps a
 * {@link StreamInput} so anything read will update the checksum
 */
public final class BufferedChecksumStreamInput extends FilterStreamInput {
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

    @Override
    public short readShort() throws IOException {
        final byte[] buf = Streams.getTemporaryBuffer();
        readBytes(buf, 0, 2);
        return (short) (((buf[0] & 0xFF) << 8) | (buf[1] & 0xFF));
    }

    @Override
    public int readInt() throws IOException {
        final byte[] buf = Streams.getTemporaryBuffer();
        readBytes(buf, 0, 4);
        return ((buf[0] & 0xFF) << 24) | ((buf[1] & 0xFF) << 16) | ((buf[2] & 0xFF) << 8) | (buf[3] & 0xFF);
    }

    @Override
    public long readLong() throws IOException {
        final byte[] buf = Streams.getTemporaryBuffer();
        readBytes(buf, 0, 8);
        return (((long) (((buf[0] & 0xFF) << 24) | ((buf[1] & 0xFF) << 16) | ((buf[2] & 0xFF) << 8) | (buf[3] & 0xFF))) << 32)
            | ((((buf[4] & 0xFF) << 24) | ((buf[5] & 0xFF) << 16) | ((buf[6] & 0xFF) << 8) | (buf[7] & 0xFF)) & 0xFFFFFFFFL);
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
        long skipped = 0;
        final byte[] buf = Streams.getTemporaryBuffer();
        for (; skipped < numBytes; ) {
            final int step = (int) Math.min(buf.length, numBytes - skipped);
            readBytes(buf, 0, step);
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
