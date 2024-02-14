/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.io.stream;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;

import java.io.EOFException;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

public class ByteBufferStreamInput extends StreamInput {

    private final ByteBuffer buffer;

    public ByteBufferStreamInput(ByteBuffer buffer) {
        this.buffer = buffer.mark();
    }

    /**
     * Read a vInt encoded in the format written by {@link StreamOutput#writeVInt} from a {@link ByteBuffer}.
     * The buffer is assumed to contain enough bytes to fully read the value and its position is moved by this method.
     * @param buffer buffer to read from
     * @return value read from the buffer
     * @throws IOException if buffer does not contain a valid vInt starting from the current position
     */
    public static int readVInt(ByteBuffer buffer) throws IOException {
        byte b = buffer.get();
        if (b >= 0) {
            return b;
        }
        int i = b & 0x7F;
        b = buffer.get();
        i |= (b & 0x7F) << 7;
        if (b >= 0) {
            return i;
        }
        b = buffer.get();
        i |= (b & 0x7F) << 14;
        if (b >= 0) {
            return i;
        }
        b = buffer.get();
        i |= (b & 0x7F) << 21;
        if (b >= 0) {
            return i;
        }
        b = buffer.get();
        i |= (b & 0x0F) << 28;
        if ((b & 0xF0) != 0) {
            throwOnBrokenVInt(b, i);
        }
        return i;
    }

    /**
     * Read a vLong encoded in the format written by {@link StreamOutput#writeVLong(long)} from a {@link ByteBuffer}.
     * The buffer is assumed to contain enough bytes to fully read the value and its position is moved by this method.
     * @param buffer buffer to read from
     * @return value read from the buffer
     * @throws IOException if buffer does not contain a valid vLong starting from the current position
     */
    public static long readVLong(ByteBuffer buffer) throws IOException {
        byte b = buffer.get();
        long i = b & 0x7FL;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = buffer.get();
        i |= (b & 0x7FL) << 7;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = buffer.get();
        i |= (b & 0x7FL) << 14;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = buffer.get();
        i |= (b & 0x7FL) << 21;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = buffer.get();
        i |= (b & 0x7FL) << 28;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = buffer.get();
        i |= (b & 0x7FL) << 35;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = buffer.get();
        i |= (b & 0x7FL) << 42;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = buffer.get();
        i |= (b & 0x7FL) << 49;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = buffer.get();
        i |= ((b & 0x7FL) << 56);
        if ((b & 0x80) == 0) {
            return i;
        }
        b = buffer.get();
        if (b != 0 && b != 1) {
            throwOnBrokenVLong(b, i);
        }
        i |= ((long) b) << 63;
        return i;
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
    public int readVInt() throws IOException {
        try {
            return readVInt(buffer);
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

    @Override
    public long readVLong() throws IOException {
        try {
            return readVLong(buffer);
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
        final int available = buffer.remaining();
        if (length > available) {
            throwEOF(length, available);
        }
    }

    @Override
    public BytesReference readSlicedBytesReference() throws IOException {
        if (buffer.hasArray()) {
            int len = readVInt();
            var res = new BytesArray(buffer.array(), buffer.arrayOffset() + buffer.position(), len);
            skip(len);
            return res;
        }
        return super.readSlicedBytesReference();
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
