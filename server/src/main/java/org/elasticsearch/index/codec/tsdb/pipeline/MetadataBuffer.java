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

    private static final int DEFAULT_DATA_CAPACITY = 64;

    private byte[] data;
    private short dataSize;

    public MetadataBuffer() {
        this.data = new byte[DEFAULT_DATA_CAPACITY];
        this.dataSize = 0;
    }

    public short size() {
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
        dataSize = (short) (dataSize + length);
        return this;
    }
}
