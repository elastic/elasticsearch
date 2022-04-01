/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.io.stream;

import java.io.EOFException;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

public class ByteBufferStreamInput extends StreamInput {

    private final ByteBuffer buffer;

    public ByteBufferStreamInput(ByteBuffer buffer) {
        this.buffer = buffer.mark();
    }

    @Override
    public int read() throws IOException {
        if (buffer.hasRemaining() == false) {
            return -1;
        }
        return buffer.get() & 0xFF;
    }

    @Override
    public byte readByte() throws IOException {
        try {
            return buffer.get();
        } catch (BufferUnderflowException ex) {
            throw newEOFException(ex);
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (buffer.hasRemaining() == false) {
            return -1;
        }

        len = Math.min(len, buffer.remaining());
        buffer.get(b, off, len);
        return len;
    }

    @Override
    public long skip(long n) throws IOException {
        int remaining = buffer.remaining();
        if (n > remaining) {
            buffer.position(buffer.limit());
            return remaining;
        }
        buffer.position((int) (buffer.position() + n));
        return n;
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        try {
            buffer.get(b, offset, len);
        } catch (BufferUnderflowException ex) {
            throw newEOFException(ex);
        }
    }

    @Override
    public short readShort() throws IOException {
        try {
            return buffer.getShort();
        } catch (BufferUnderflowException ex) {
            throw newEOFException(ex);
        }
    }

    @Override
    public int readInt() throws IOException {
        try {
            return buffer.getInt();
        } catch (BufferUnderflowException ex) {
            throw newEOFException(ex);
        }
    }

    @Override
    public long readLong() throws IOException {
        try {
            return buffer.getLong();
        } catch (BufferUnderflowException ex) {
            throw newEOFException(ex);
        }
    }

    private static EOFException newEOFException(RuntimeException ex) {
        EOFException eofException = new EOFException();
        eofException.initCause(ex);
        return eofException;
    }

    @Override
    public void reset() throws IOException {
        buffer.reset();
    }

    @Override
    public int available() throws IOException {
        return buffer.remaining();
    }

    @Override
    protected void ensureCanReadBytes(int length) throws EOFException {
        if (buffer.remaining() < length) {
            throw new EOFException("tried to read: " + length + " bytes but only " + buffer.remaining() + " remaining");
        }
    }

    @Override
    public void mark(int readlimit) {
        buffer.mark();
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public void close() throws IOException {}
}
