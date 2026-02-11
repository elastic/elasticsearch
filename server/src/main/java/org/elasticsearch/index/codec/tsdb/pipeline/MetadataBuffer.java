/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.ArrayUtil;

import java.io.IOException;

public final class MetadataBuffer implements MetadataWriter {

    public static final int DEFAULT_CAPACITY = 64;

    private byte[] data;
    private int dataSize;

    public MetadataBuffer() {
        this(DEFAULT_CAPACITY);
    }

    public MetadataBuffer(int initialCapacity) {
        this.data = new byte[initialCapacity];
        this.dataSize = 0;
    }

    public int size() {
        return dataSize;
    }

    public void writeTo(final DataOutput out, int offset, int length) throws IOException {
        if (length > 0) {
            out.writeBytes(data, offset, length);
        }
    }

    public void clear() {
        dataSize = 0;
    }

    private void ensureCapacity(int additional) {
        if (data.length < dataSize + additional) {
            data = ArrayUtil.grow(data, dataSize + additional);
        }
    }

    @Override
    public MetadataWriter writeByte(byte value) {
        ensureCapacity(Byte.BYTES);
        data[dataSize++] = value;
        return this;
    }

    @Override
    public MetadataWriter writeZInt(int value) {
        return writeVInt((value >> 31) ^ (value << 1));
    }

    @Override
    public MetadataWriter writeZLong(long value) {
        return writeVLong((value >> 63) ^ (value << 1));
    }

    @Override
    public MetadataWriter writeLong(long value) {
        // Match Lucene's DataOutput.writeLong format for compatibility with DataInput.readLong
        // Lucene writes: [low int little-endian] [high int little-endian]
        ensureCapacity(Long.BYTES);
        int lo = (int) value;
        int hi = (int) (value >> 32);
        // Write low int first (little-endian)
        data[dataSize++] = (byte) lo;
        data[dataSize++] = (byte) (lo >> 8);
        data[dataSize++] = (byte) (lo >> 16);
        data[dataSize++] = (byte) (lo >> 24);
        // Write high int second (little-endian)
        data[dataSize++] = (byte) hi;
        data[dataSize++] = (byte) (hi >> 8);
        data[dataSize++] = (byte) (hi >> 16);
        data[dataSize++] = (byte) (hi >> 24);
        return this;
    }

    @Override
    public MetadataWriter writeVInt(int value) {
        while ((value & ~0x7F) != 0) {
            ensureCapacity(1);
            data[dataSize++] = (byte) ((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        ensureCapacity(1);
        data[dataSize++] = (byte) value;
        return this;
    }

    @Override
    public MetadataWriter writeVLong(long value) {
        for (int i = 0; i < 9 && (value & ~0x7FL) != 0; i++) {
            ensureCapacity(1);
            data[dataSize++] = (byte) ((value & 0x7FL) | 0x80L);
            value >>>= 7;
        }
        ensureCapacity(1);
        data[dataSize++] = (byte) value;
        return this;
    }

    @Override
    public MetadataWriter writeBytes(final byte[] bytes, int offset, int length) {
        ensureCapacity(length);
        System.arraycopy(bytes, offset, data, dataSize, length);
        dataSize += length;
        return this;
    }
}
