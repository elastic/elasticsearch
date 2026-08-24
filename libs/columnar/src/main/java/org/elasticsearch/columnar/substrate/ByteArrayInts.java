/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.substrate;

import java.io.IOException;

/**
 * Integer encodings over {@code byte[]}, in the two forms this codec writes into buffers it owns:
 * <b>variable-length</b> (a vint or vlong, seven bits per byte, high bit continuing) and <b>fixed-width</b>
 * little-endian at one, two or four bytes.
 *
 * <p>These exist because {@link org.apache.lucene.store.DataOutput} and
 * {@link org.apache.lucene.store.DataInput} are stream shapes: they carry a position, so a caller holding a
 * byte array and an index of its own cannot use them without wrapping the array per call. That left the same
 * seven-bit loop hand-written wherever a buffer needed one, which is a format detail worth having in exactly
 * one place — every byte written here is read back by Lucene's {@code readVInt}/{@code readVLong} somewhere,
 * so a divergence would be a silent corruption rather than a compile error.
 *
 * <p>Callers size their buffers with {@link #MAX_VINT_BYTES}, {@link #MAX_VLONG_BYTES} or
 * {@link #vIntLength}. Reads throw {@link IOException} on a malformed vint rather than running off the
 * array, consistent with Lucene's own behaviour; truncated or otherwise corrupt data is also caught by the
 * segment checksum before it reaches these methods.
 */
public final class ByteArrayInts {

    /** Bytes a vint can take at most, so a buffer can be sized before the value is known. */
    public static final int MAX_VINT_BYTES = 5;

    /** Bytes a vlong can take at most. Ten rather than nine, because a negative value uses the full width. */
    public static final int MAX_VLONG_BYTES = 10;

    private ByteArrayInts() {}

    /**
     * Writes {@code value} as a vint at {@code offset}, byte for byte as Lucene's {@code writeVInt} would.
     *
     * @return the number of bytes written, at most {@link #MAX_VINT_BYTES}
     */
    public static int writeVInt(int value, byte[] dst, int offset) {
        int at = offset;
        while ((value & ~0x7F) != 0) {
            dst[at++] = (byte) ((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        dst[at++] = (byte) value;
        return at - offset;
    }

    /**
     * Writes {@code value} as a vlong at {@code offset}, byte for byte as Lucene's {@code writeVLong} would.
     *
     * @return the number of bytes written, at most {@link #MAX_VLONG_BYTES}
     */
    public static int writeVLong(long value, byte[] dst, int offset) {
        int at = offset;
        while ((value & ~0x7FL) != 0) {
            dst[at++] = (byte) ((value & 0x7FL) | 0x80L);
            value >>>= 7;
        }
        dst[at++] = (byte) value;
        return at - offset;
    }

    /**
     * Reads the vint at {@code offset}. Throws {@link IOException} on a value that is too wide to be a valid
     * vint rather than running off the array.
     *
     * <p>Prefer {@link #readVInt(byte[], int[])} when the caller also needs to advance past the vint, to
     * avoid a separate {@link #vIntLength} call that re-derives what the read already had in hand.
     */
    public static int readVInt(byte[] src, int offset) throws IOException {
        int value = 0, shift = 0;
        for (int i = 0; i < MAX_VINT_BYTES; i++) {
            final byte b = src[offset++];
            value |= (b & 0x7F) << shift;
            shift += 7;
            if ((b & 0x80) == 0) {
                return value;
            }
        }
        throw new IOException("Invalid vInt detected (too many bytes)");
    }

    /**
     * Reads the vint at {@code pos[0]} and advances {@code pos[0]} past it, so the caller does not need a
     * separate {@link #vIntLength} call. The cursor is typically a single-element array held as a field and
     * reused across calls to avoid allocation in hot decode loops.
     */
    public static int readVInt(byte[] src, int[] pos) throws IOException {
        int offset = pos[0], value = 0, shift = 0;
        for (int i = 0; i < MAX_VINT_BYTES; i++) {
            final byte b = src[offset++];
            value |= (b & 0x7F) << shift;
            shift += 7;
            if ((b & 0x80) == 0) {
                pos[0] = offset;
                return value;
            }
        }
        throw new IOException("Invalid vInt detected (too many bytes)");
    }

    /**
     * The bytes {@link #writeVInt} takes for {@code value}, which is also the width of the encoding
     * {@link #readVInt} just read — the encoding being minimal, the value determines its own length.
     */
    public static int vIntLength(int value) {
        final int bits = Integer.SIZE - Integer.numberOfLeadingZeros(value);
        // Seven bits per byte, and zero still occupies one.
        return bits == 0 ? 1 : (bits + 6) / 7;
    }

    /** The narrowest of {@code 1}, {@code 2} and {@code 4} bytes that holds every value up to {@code max}. */
    public static int widthFor(int max) {
        return max < 0x100 ? 1 : (max < 0x10000 ? 2 : 4);
    }

    /** Writes the low {@code width} bytes of {@code value} little-endian at {@code offset}. */
    public static void writeIntLE(int value, int width, byte[] dst, int offset) {
        for (int b = 0; b < width; b++) {
            dst[offset + b] = (byte) (value >>> (8 * b));
        }
    }

    /** Reads a {@code width}-byte little-endian value at {@code offset}, as written by {@link #writeIntLE}. */
    public static int readIntLE(byte[] src, int offset, int width) {
        int value = 0;
        for (int b = 0; b < width; b++) {
            value |= (src[offset + b] & 0xFF) << (8 * b);
        }
        return value;
    }
}
