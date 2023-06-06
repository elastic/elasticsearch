/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io.stream;

import org.apache.lucene.util.BytesRef;

import java.io.EOFException;
import java.io.IOException;

/**
 * Resettable {@link StreamInput} that wraps a byte array. It is heavily inspired in Lucene's
 * {@link org.apache.lucene.store.ByteArrayDataInput}.
 */
public class ByteArrayStreamInput extends StreamInput {

    private byte[] bytes;
    private int pos;
    private int limit;

    public ByteArrayStreamInput() {
        reset(BytesRef.EMPTY_BYTES);
    }

    public ByteArrayStreamInput(byte[] bytes) {
        reset(bytes);
    }

    @Override
    public int read() throws IOException {
        if (limit - pos <= 0) {
            return -1;
        }
        return readByte() & 0xFF;
    }

    public void reset(byte[] bytes) {
        reset(bytes, 0, bytes.length);
    }

    public int length() {
        return limit;
    }

    public int getPosition() {
        return pos;
    }

    public void setPosition(int pos) {
        this.pos = pos;
    }

    public void reset(byte[] bytes, int offset, int len) {
        this.bytes = bytes;
        pos = offset;
        limit = offset + len;
    }

    public void skipBytes(long count) {
        pos += (int) count;
    }

    @Override
    public void close() {
        // No-op
    }

    @Override
    public int available() {
        return limit - pos;
    }

    @Override
    protected void ensureCanReadBytes(int length) throws EOFException {
        final int available = limit - pos;
        if (length > available) {
            throwEOF(length, available);
        }
    }

    @Override
    public byte readByte() {
        return bytes[pos++];
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) {
        System.arraycopy(bytes, pos, b, offset, len);
        pos += len;
    }
}
