/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

import org.elasticsearch.common.util.ByteUtils;

import java.util.Arrays;
import java.util.Objects;

/**
 * A reusable byte buffer for writing stage metadata during encoding.
 * <p>
 * This buffer accumulates metadata (stage IDs and parameters) during the encoding
 * pipeline before writing to the final output. It supports variable-length encoding
 * of integers and longs using the same conventions as Lucene's DataOutput.
 * <p>
 * The buffer automatically grows when capacity is exceeded and retains its grown
 * size after {@link #clear()} for zero-allocation steady-state operation. This
 * avoids repeated allocations during encoding hot paths, reducing GC pressure.
 * <p>
 * This buffer is write-only. For reading metadata during decoding, use
 * {@link MetadataReader} implementations that wrap Lucene's DataInput directly.
 * <p>
 * This class is NOT thread-safe and should be used by a single thread.
 */
public final class MetadataBuffer implements MetadataWriter {

    private static final int DEFAULT_CAPACITY_BYTES = 64;

    private static final int MAX_VINT_BYTES = Integer.BYTES + 1;
    private static final int MAX_VLONG_BYTES = Long.BYTES + 2;

    private byte[] buffer;
    private int size;

    /**
     * Creates a new MetadataBuffer with default initial capacity (64 bytes).
     */
    public MetadataBuffer() {
        this.buffer = new byte[DEFAULT_CAPACITY_BYTES];
        this.size = 0;
    }

    /**
     * Writes a single byte to the buffer.
     *
     * @param b the byte to write
     */
    @Override
    public void writeByte(final byte b) {
        ensureCapacity(1);
        buffer[size++] = b;
    }

    /**
     * Writes a non-negative integer in variable-length format.
     * Uses 1-5 bytes depending on the value magnitude.
     *
     * @param value the non-negative integer to write
     * @throws IllegalArgumentException if the value is negative
     */
    @Override
    public void writeVInt(final int value) {
        if (value < 0) {
            throw new IllegalArgumentException("writeVInt does not support negative values: " + value);
        }
        ensureCapacity(MAX_VINT_BYTES);
        writeVarLong(value & 0xFFFFFFFFL);
    }

    /**
     * Writes a non-negative long in variable-length format.
     * Uses 1-9 bytes depending on the value magnitude.
     *
     * @param value the non-negative long to write
     * @throws IllegalArgumentException if the value is negative
     */
    @Override
    public void writeVLong(final long value) {
        if (value < 0) {
            throw new IllegalArgumentException("writeVLong does not support negative values: " + value);
        }
        ensureCapacity(MAX_VLONG_BYTES);
        writeVarLong(value);
    }

    /**
     * Writes a signed integer using zig-zag encoding followed by variable-length encoding.
     *
     * @param value the signed integer to write
     */
    @Override
    public void writeZInt(final int value) {
        ensureCapacity(MAX_VINT_BYTES);
        writeVarLong(ByteUtils.zigZagEncode(value) & 0xFFFFFFFFL);
    }

    /**
     * Writes a signed long using zig-zag encoding followed by variable-length encoding.
     *
     * @param value the signed long to write
     */
    @Override
    public void writeZLong(final long value) {
        ensureCapacity(MAX_VLONG_BYTES);
        writeVarLong(ByteUtils.zigZagEncode(value));
    }

    private void writeVarLong(long value) {
        while ((value & ~0x7FL) != 0) {
            buffer[size++] = (byte) ((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        buffer[size++] = (byte) value;
    }

    /**
     * Clears the buffer for reuse, resetting size to zero.
     * The underlying buffer capacity is retained to avoid reallocation.
     */
    @Override
    public void clear() {
        size = 0;
    }

    /**
     * Returns the number of bytes written to the buffer.
     *
     * @return the size in bytes
     */
    @Override
    public int size() {
        return size;
    }

    /**
     * Returns a copy of the written bytes.
     *
     * @return a new byte array containing exactly {@link #size()} bytes
     */
    @Override
    public byte[] toByteArray() {
        return Arrays.copyOf(buffer, size);
    }

    /**
     * Writes the buffer contents to the destination array.
     *
     * @param dest the destination array
     * @param offset the offset in the destination array
     * @return the number of bytes written
     * @throws NullPointerException if dest is null
     * @throws IllegalArgumentException if offset is negative or destination has insufficient space
     */
    public int writeTo(final byte[] dest, int offset) {
        Objects.requireNonNull(dest, "dest");
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be non-negative: " + offset);
        }
        if (dest.length - offset < size) {
            throw new IllegalArgumentException(
                "destination array has insufficient space: need "
                    + size
                    + " bytes at offset "
                    + offset
                    + ", but array length is "
                    + dest.length
            );
        }
        System.arraycopy(buffer, 0, dest, offset, size);
        return size;
    }

    private void ensureCapacity(final int additional) {
        if (size + additional > buffer.length) {
            grow(additional);
        }
    }

    private void grow(final int additional) {
        final int newCapacity = Math.max(buffer.length * 2, size + additional);
        buffer = Arrays.copyOf(buffer, newCapacity);
    }
}
