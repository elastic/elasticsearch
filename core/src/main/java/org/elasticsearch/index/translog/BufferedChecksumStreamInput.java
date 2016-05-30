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
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/**
 * Similar to Lucene's BufferedChecksumIndexInput, however this wraps a
 * {@link StreamInput} so anything read will update the checksum
 */
public final class BufferedChecksumStreamInput extends StreamInput {
    private static final int SKIP_BUFFER_SIZE = 1024;
    private byte[] skipBuffer;
    private final StreamInput in;
    private final Checksum digest;

    public BufferedChecksumStreamInput(StreamInput in) {
        this.in = in;
        this.digest = new BufferedChecksum(new CRC32());
    }

    public BufferedChecksumStreamInput(StreamInput in, BufferedChecksumStreamInput reuse) {
        this.in = in;
        if (reuse == null ) {
            this.digest = new BufferedChecksum(new CRC32());
        } else {
            this.digest = reuse.digest;
            digest.reset();
            this.skipBuffer = reuse.skipBuffer;
        }
    }

    public long getChecksum() {
        return this.digest.getValue();
    }

    @Override
    public byte readByte() throws IOException {
        final byte b = in.readByte();
        digest.update(b);
        return b;
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        in.readBytes(b, offset, len);
        digest.update(b, offset, len);
    }

    @Override
    public void reset() throws IOException {
        in.reset();
        digest.reset();
    }

    @Override
    public int read() throws IOException {
        return readByte() & 0xFF;
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    @Override
    public boolean markSupported() {
        return in.markSupported();
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
    public int available() throws IOException {
        return in.available();
    }

    @Override
    public synchronized void mark(int readlimit) {
        in.mark(readlimit);
    }

    public void resetDigest() {
        digest.reset();
    }
}
