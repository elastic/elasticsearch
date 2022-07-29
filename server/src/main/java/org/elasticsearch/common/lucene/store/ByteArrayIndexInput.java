/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.lucene.store;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BitUtil;

import java.io.EOFException;
import java.io.IOException;

/**
 * Wraps array of bytes into IndexInput
 */
public class ByteArrayIndexInput extends IndexInput {
    private final byte[] bytes;

    private int pos;

    private final int offset;

    private final int length;

    public ByteArrayIndexInput(String resourceDesc, byte[] bytes) {
        this(resourceDesc, bytes, 0, bytes.length);
    }

    public ByteArrayIndexInput(String resourceDesc, byte[] bytes, int offset, int length) {
        super(resourceDesc);
        this.bytes = bytes;
        this.offset = offset;
        this.length = length;
    }

    @Override
    public void close() throws IOException {}

    @Override
    public long getFilePointer() {
        return pos;
    }

    @Override
    public void seek(long l) throws IOException {
        if (l < 0) {
            throw new IllegalArgumentException("Seeking to negative position: " + pos);
        } else if (l > length) {
            throw new EOFException("seek past EOF");
        }
        pos = (int) l;
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        if (offset >= 0L && length >= 0L && offset + length <= this.length) {
            return new ByteArrayIndexInput(sliceDescription, bytes, this.offset + (int) offset, (int) length);
        } else {
            throw new IllegalArgumentException(
                "slice() "
                    + sliceDescription
                    + " out of bounds: offset="
                    + offset
                    + ",length="
                    + length
                    + ",fileLength="
                    + this.length
                    + ": "
                    + this
            );
        }
    }

    @Override
    public byte readByte() throws IOException {
        if (pos >= offset + length) {
            throw new EOFException("seek past EOF");
        }
        return bytes[offset + pos++];
    }

    @Override
    public void readBytes(final byte[] b, final int offset, int len) throws IOException {
        if (pos + len > this.offset + length) {
            throw new EOFException("seek past EOF");
        }
        System.arraycopy(bytes, this.offset + pos, b, offset, len);
        pos += len;
    }

    @Override
    public short readShort() throws IOException {
        try {
            return (short) BitUtil.VH_LE_SHORT.get(bytes, pos);
        } finally {
            pos += Short.BYTES;
        }
    }

    @Override
    public int readInt() throws IOException {
        try {
            return (int) BitUtil.VH_LE_INT.get(bytes, pos);
        } finally {
            pos += Integer.BYTES;
        }
    }

    @Override
    public long readLong() throws IOException {
        try {
            return (long) BitUtil.VH_LE_LONG.get(bytes, pos);
        } finally {
            pos += Long.BYTES;
        }
    }
}
