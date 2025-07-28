/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.common.lucene.store;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.BitUtil;
import org.elasticsearch.common.Strings;

import java.io.EOFException;
import java.io.IOException;

/**
 * Wraps array of bytes into IndexInput
 */
public class ByteArrayIndexInput extends IndexInput implements RandomAccessInput {
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
        this.pos = offset;
        this.length = length;
    }

    @Override
    public void close() throws IOException {}

    @Override
    public long getFilePointer() {
        return pos - offset;
    }

    @Override
    public void seek(long l) throws IOException {
        pos = position(l);
    }

    private int position(long p) throws EOFException {
        if (p < 0) {
            throw new IllegalArgumentException("Seeking to negative position: " + p);
        } else if (p > length) {
            throw new EOFException("seek past EOF");
        }
        return (int) p + offset;
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public byte readByte(long pos) throws IOException {
        return bytes[position(pos)];
    }

    @Override
    public short readShort(long pos) throws IOException {
        return (short) BitUtil.VH_LE_SHORT.get(bytes, position(pos));
    }

    @Override
    public int readInt(long pos) throws IOException {
        return (int) BitUtil.VH_LE_INT.get(bytes, position(pos));
    }

    @Override
    public long readLong(long pos) throws IOException {
        return (long) BitUtil.VH_LE_LONG.get(bytes, position(pos));
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        if (offset >= 0L && length >= 0L && offset + length <= this.length) {
            return new ByteArrayIndexInput(sliceDescription, bytes, this.offset + (int) offset, (int) length);
        } else {
            throw new IllegalArgumentException(
                Strings.format(
                    "slice() %s out of bounds: offset=%d,length=%d,fileLength=%d: %s",
                    sliceDescription,
                    offset,
                    length,
                    this.length,
                    this
                )
            );
        }
    }

    @Override
    public byte readByte() throws IOException {
        if (pos >= offset + length) {
            throw new EOFException("seek past EOF");
        }
        return bytes[pos++];
    }

    @Override
    public void readBytes(final byte[] b, final int offset, int len) throws IOException {
        if (pos + len > this.offset + length) {
            throw new EOFException("seek past EOF");
        }
        System.arraycopy(bytes, pos, b, offset, len);
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
