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
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/**
 * Similar to Lucene's BufferedChecksumIndexOutput, however this wraps a
 * {@link StreamOutput} so anything written will update the checksum
 */
public final class BufferedChecksumStreamOutput extends StreamOutput {
    private final StreamOutput out;
    private final Checksum digest;

    public BufferedChecksumStreamOutput(StreamOutput out) {
        this.out = out;
        this.digest = new BufferedChecksum(new CRC32());
    }

    public long getChecksum() {
        return this.digest.getValue();
    }

    @Override
    public void writeByte(byte b) throws IOException {
        out.writeByte(b);
        digest.update(b);
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
        out.writeBytes(b, offset, length);
        digest.update(b, offset, length);
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }

    @Override
    public void close() throws IOException {
        out.close();
    }

    @Override
    public void reset() throws IOException {
        out.reset();
        digest.reset();
    }

    public void resetDigest() {
        digest.reset();
    }
}
