/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
