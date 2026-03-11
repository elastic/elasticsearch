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

/**
 * In-memory auto-growing buffer for stage metadata. Implements {@link MetadataWriter}
 * for use during encoding, and provides {@link #writeTo} for flushing to a
 * {@link DataOutput}.
 *
 * <p>The byte layout for {@link #writeLong} matches Lucene's {@code DataOutput.writeLong}
 * format (two little-endian ints, low int first) for compatibility with
 * {@code DataInput.readLong}.
 */
final class MetadataBuffer implements MetadataWriter {

    private static final int DEFAULT_CAPACITY = 64;
    private static final int MAX_VINT_BYTES = 5;
    private static final int MAX_VLONG_BYTES = 10;

    private byte[] data;
    private int dataSize;

    MetadataBuffer() {
        this(DEFAULT_CAPACITY);
    }

    /**
     * Creates a metadata buffer with the specified initial capacity.
     *
     * @param initialCapacity the initial buffer capacity in bytes
     */
    MetadataBuffer(int initialCapacity) {
        this.data = new byte[initialCapacity];
        this.dataSize = 0;
    }

    /**
     * Returns the number of bytes currently stored.
     *
     * @return the buffer size in bytes
     */
    public int size() {
        return dataSize;
    }

    /**
     * Writes a slice of the buffer to the given output.
     *
     * @param out the output to write to
     * @param offset the start offset within this buffer
     * @param length the number of bytes to write
     */
    public void writeTo(final DataOutput out, int offset, int length) throws IOException {
        assert offset >= 0 && length >= 0 && offset + length <= dataSize
            : "Invalid slice [offset=" + offset + ", length=" + length + ", size=" + dataSize + "]";
        if (length > 0) {
            out.writeBytes(data, offset, length);
        }
    }

    /** Resets the buffer for reuse without releasing the backing array. */
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
        return encodeVInt((value >> 31) ^ (value << 1));
    }

    @Override
    public MetadataWriter writeZLong(long value) {
        return encodeVLong((value >> 63) ^ (value << 1));
    }

    @Override
    public MetadataWriter writeLong(long value) {
        ensureCapacity(Long.BYTES);
        final int lo = (int) value;
        final int hi = (int) (value >> 32);
        data[dataSize++] = (byte) lo;
        data[dataSize++] = (byte) (lo >> 8);
        data[dataSize++] = (byte) (lo >> 16);
        data[dataSize++] = (byte) (lo >> 24);
        data[dataSize++] = (byte) hi;
        data[dataSize++] = (byte) (hi >> 8);
        data[dataSize++] = (byte) (hi >> 16);
        data[dataSize++] = (byte) (hi >> 24);
        return this;
    }

    @Override
    public MetadataWriter writeInt(int value) {
        ensureCapacity(Integer.BYTES);
        data[dataSize++] = (byte) value;
        data[dataSize++] = (byte) (value >> 8);
        data[dataSize++] = (byte) (value >> 16);
        data[dataSize++] = (byte) (value >> 24);
        return this;
    }

    @Override
    public MetadataWriter writeVInt(int value) {
        assert value >= 0 : "writeVInt requires non-negative value, got: " + value + ". Use writeZInt for signed values";
        return encodeVInt(value);
    }

    @Override
    public MetadataWriter writeVLong(long value) {
        assert value >= 0 : "writeVLong requires non-negative value, got: " + value + ". Use writeZLong for signed values";
        return encodeVLong(value);
    }

    private MetadataWriter encodeVInt(int value) {
        ensureCapacity(MAX_VINT_BYTES);
        while ((value & ~0x7F) != 0) {
            data[dataSize++] = (byte) ((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        data[dataSize++] = (byte) value;
        return this;
    }

    private MetadataWriter encodeVLong(long value) {
        ensureCapacity(MAX_VLONG_BYTES);
        for (int i = 0; i < 9 && (value & ~0x7FL) != 0; i++) {
            data[dataSize++] = (byte) ((value & 0x7FL) | 0x80L);
            value >>>= 7;
        }
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
