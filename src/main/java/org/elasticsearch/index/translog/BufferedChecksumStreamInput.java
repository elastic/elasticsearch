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
    private final StreamInput in;
    private final Checksum digest;

    public BufferedChecksumStreamInput(StreamInput in) {
        this.in = in;
        this.digest = new BufferedChecksum(new CRC32());
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
}
