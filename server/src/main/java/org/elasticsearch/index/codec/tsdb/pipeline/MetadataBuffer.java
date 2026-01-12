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

/**
 * A reusable byte buffer for reading and writing stage metadata.
 * <p>
 * This buffer supports variable-length encoding of integers and longs using the same
 * conventions as Lucene's DataOutput/DataInput. The buffer automatically grows when
 * capacity is exceeded and retains its grown size after reset for zero-allocation
 * steady-state operation.
 * <p>
 * This class is NOT thread-safe and should be used by a single thread.
 */
public final class MetadataBuffer implements MetadataWriter, MetadataReader {

    private static final int DEFAULT_CAPACITY_BYTES = 64;

    private static final int SINGLE_BYTE = 1;
    private static final int MAX_VINT_BYTES = 5;
    private static final int MAX_VLONG_BYTES = 9;
    private static final int MAX_ZLONG_BYTES = 10;

    private byte[] buffer;
    private int position;
    private int size;

    /**
     * Creates a new MetadataBuffer with default initial capacity.
     */
    public MetadataBuffer() {
        this.buffer = new byte[DEFAULT_CAPACITY_BYTES];
        this.position = 0;
        this.size = 0;
    }

    /**
     * Writes a single byte to the buffer.
     *
     * @param b the byte to write
     */
    @Override
    public void writeByte(final byte b) {
        if (position >= buffer.length) {
            grow(SINGLE_BYTE);
        }
        buffer[position++] = b;
        size = position;
        assert position <= buffer.length : "position exceeds buffer length";
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
        if (position + MAX_VINT_BYTES > buffer.length) {
            grow(MAX_VINT_BYTES);
        }
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
        if (position + MAX_VLONG_BYTES > buffer.length) {
            grow(MAX_VLONG_BYTES);
        }
        writeVarLong(value);
    }

    /**
     * Writes a signed long using zig-zag encoding followed by variable-length encoding.
     * Efficient for values with small absolute magnitude.
     *
     * @param value the signed long to write
     */
    @Override
    public void writeZLong(final long value) {
        if (position + MAX_ZLONG_BYTES > buffer.length) {
            grow(MAX_ZLONG_BYTES);
        }
        writeVarLong(ByteUtils.zigZagEncode(value));
    }

    private void writeVarLong(long value) {
        while ((value & ~0x7FL) != 0) {
            buffer[position++] = (byte) ((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        buffer[position++] = (byte) value;
        size = position;
        assert position <= buffer.length : "position exceeds buffer length";
    }

    /**
     * Reads a single byte from the buffer.
     *
     * @return the byte value
     * @throws IllegalStateException if no more bytes are available
     */
    @Override
    public byte readByte() {
        if (position >= size) {
            throw new IllegalStateException("Attempt to read past end of buffer: position=" + position + ", size=" + size);
        }
        final byte result = buffer[position++];
        assert position <= size : "position exceeds size after read";
        return result;
    }

    /**
     * Reads a non-negative integer in variable-length format.
     *
     * @return the integer value
     * @throws IllegalStateException if no more bytes are available or encoding is invalid
     */
    @Override
    public int readVInt() {
        byte b = readByte();
        if (b >= 0) {
            return b;
        }
        int result = b & 0x7F;
        b = readByte();
        result |= (b & 0x7F) << 7;
        if (b >= 0) {
            return result;
        }
        b = readByte();
        result |= (b & 0x7F) << 14;
        if (b >= 0) {
            return result;
        }
        b = readByte();
        result |= (b & 0x7F) << 21;
        if (b >= 0) {
            return result;
        }
        b = readByte();
        result |= (b & 0x0F) << 28;
        if ((b & 0xF0) != 0) {
            throw new IllegalStateException("Invalid VInt encoding");
        }
        return result;
    }

    /**
     * Reads a non-negative long in variable-length format.
     *
     * @return the long value
     * @throws IllegalStateException if no more bytes are available or encoding is invalid
     */
    @Override
    public long readVLong() {
        byte b = readByte();
        if (b >= 0) {
            return b;
        }
        long result = b & 0x7FL;
        b = readByte();
        result |= (b & 0x7FL) << 7;
        if (b >= 0) {
            return result;
        }
        b = readByte();
        result |= (b & 0x7FL) << 14;
        if (b >= 0) {
            return result;
        }
        b = readByte();
        result |= (b & 0x7FL) << 21;
        if (b >= 0) {
            return result;
        }
        b = readByte();
        result |= (b & 0x7FL) << 28;
        if (b >= 0) {
            return result;
        }
        b = readByte();
        result |= (b & 0x7FL) << 35;
        if (b >= 0) {
            return result;
        }
        b = readByte();
        result |= (b & 0x7FL) << 42;
        if (b >= 0) {
            return result;
        }
        b = readByte();
        result |= (b & 0x7FL) << 49;
        if (b >= 0) {
            return result;
        }
        b = readByte();
        if (b < 0) {
            throw new IllegalStateException("Invalid VLong encoding");
        }
        result |= (b & 0xFFL) << 56;
        return result;
    }

    /**
     * Reads a signed long that was written using zig-zag encoding.
     *
     * @return the signed long value
     * @throws IllegalStateException if no more bytes are available or encoding is invalid
     */
    @Override
    public long readZLong() {
        byte b = readByte();
        if (b >= 0) {
            return ByteUtils.zigZagDecode(b);
        }
        long result = b & 0x7FL;
        b = readByte();
        result |= (b & 0x7FL) << 7;
        if (b >= 0) {
            return ByteUtils.zigZagDecode(result);
        }
        b = readByte();
        result |= (b & 0x7FL) << 14;
        if (b >= 0) {
            return ByteUtils.zigZagDecode(result);
        }
        b = readByte();
        result |= (b & 0x7FL) << 21;
        if (b >= 0) {
            return ByteUtils.zigZagDecode(result);
        }
        b = readByte();
        result |= (b & 0x7FL) << 28;
        if (b >= 0) {
            return ByteUtils.zigZagDecode(result);
        }
        b = readByte();
        result |= (b & 0x7FL) << 35;
        if (b >= 0) {
            return ByteUtils.zigZagDecode(result);
        }
        b = readByte();
        result |= (b & 0x7FL) << 42;
        if (b >= 0) {
            return ByteUtils.zigZagDecode(result);
        }
        b = readByte();
        result |= (b & 0x7FL) << 49;
        if (b >= 0) {
            return ByteUtils.zigZagDecode(result);
        }
        b = readByte();
        result |= (b & 0x7FL) << 56;
        if (b >= 0) {
            return ByteUtils.zigZagDecode(result);
        }
        b = readByte();
        if ((b & 0xFE) != 0) {
            throw new IllegalStateException("Invalid ZLong encoding");
        }
        result |= (b & 0x01L) << 63;
        return ByteUtils.zigZagDecode(result);
    }

    /**
     * Resets the buffer position and size for reuse.
     * The underlying buffer capacity is retained to avoid reallocation.
     */
    @Override
    public void reset() {
        position = 0;
        size = 0;
    }

    /**
     * Returns the current read/write position.
     *
     * @return the current position
     */
    @Override
    public int position() {
        return position;
    }

    /**
     * Sets the read position. Used when switching from write to read mode.
     *
     * @param newPosition the new position
     * @throws IllegalArgumentException if position is negative or exceeds size
     */
    @Override
    public void setPosition(final int newPosition) {
        if (newPosition < 0 || newPosition > size) {
            throw new IllegalArgumentException("Invalid position: " + newPosition + ", size=" + size);
        }
        this.position = newPosition;
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
     * Returns the current capacity of the underlying buffer.
     *
     * @return the capacity in bytes
     */
    public int capacity() {
        return buffer.length;
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

    private void grow(final int additional) {
        final int newCapacity = Math.max(buffer.length * 2, position + additional);
        buffer = Arrays.copyOf(buffer, newCapacity);
    }
}
