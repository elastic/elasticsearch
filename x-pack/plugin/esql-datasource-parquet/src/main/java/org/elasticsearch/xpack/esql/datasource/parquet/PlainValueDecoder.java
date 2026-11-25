/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.UninitializedArrays;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Bulk PLAIN decoder over a little-endian value {@link ByteBuffer} for page-level Parquet reads.
 * Fixed-width types (INT32, INT64, FLOAT, DOUBLE) are read contiguously; BOOLEAN is bit-packed
 * (1 bit per value); BINARY is length-prefixed; FIXED_LEN_BYTE_ARRAY uses a fixed stride.
 */
final class PlainValueDecoder {

    private ByteBuffer buffer;

    /**
     * Bit offset within the current byte for boolean reads/skips. Parquet PLAIN booleans are
     * bit-packed (8 values per byte) and interleaved skip/read calls (e.g. during sparse reads)
     * can consume a non-multiple-of-8 count, leaving a partial byte. This field tracks how many
     * bits of the current byte have already been consumed (range 0..7). Reset on {@link #init}.
     */
    private int boolBitOffset;

    /**
     * Initializes (or re-initializes) the decoder with a new value buffer. Must be called before
     * each Parquet data page to reset internal state (including the boolean bit offset).
     */
    void init(ByteBuffer valueBytes) {
        this.buffer = valueBytes.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        this.boolBitOffset = 0;
    }

    void readInts(int[] values, int offset, int count) {
        buffer.asIntBuffer().get(values, offset, count);
        buffer.position(buffer.position() + (count << 2));
    }

    void readLongs(long[] values, int offset, int count) {
        buffer.asLongBuffer().get(values, offset, count);
        buffer.position(buffer.position() + (count << 3));
    }

    // TODO: consider adding a reusable float[] to DecodeBuffers to avoid per-batch allocation
    void readFloats(double[] values, int offset, int count) {
        float[] tmp = UninitializedArrays.newFloatArray(count);
        buffer.asFloatBuffer().get(tmp, 0, count);
        buffer.position(buffer.position() + (count << 2));
        for (int i = 0; i < count; i++) {
            values[offset + i] = tmp[i];
        }
    }

    void readDoubles(double[] values, int offset, int count) {
        buffer.asDoubleBuffer().get(values, offset, count);
        buffer.position(buffer.position() + (count << 3));
    }

    void readBooleans(boolean[] values, int offset, int count) {
        int basePos = buffer.position();
        for (int i = 0; i < count; i++) {
            int absoluteBit = boolBitOffset + i;
            values[offset + i] = ((buffer.get(basePos + absoluteBit / 8) >>> (absoluteBit % 8)) & 1) != 0;
        }
        int totalBits = boolBitOffset + count;
        buffer.position(basePos + totalBits / 8);
        boolBitOffset = totalBits % 8;
    }

    /** Returned BytesRef instances share storage with the value buffer and must be copied before the buffer is reused. */
    void readBinaries(BytesRef[] values, int offset, int count) {
        if (buffer.hasArray()) {
            byte[] backing = buffer.array();
            int baseOffset = buffer.arrayOffset();
            for (int i = 0; i < count; i++) {
                int length = buffer.getInt();
                int pos = buffer.position();
                values[offset + i] = new BytesRef(backing, baseOffset + pos, length);
                buffer.position(pos + length);
            }
        } else {
            for (int i = 0; i < count; i++) {
                int length = buffer.getInt();
                byte[] copy = UninitializedArrays.newByteArray(length);
                buffer.get(copy);
                values[offset + i] = new BytesRef(copy);
            }
        }
    }

    /**
     * Exact number of payload bytes (length prefixes excluded) that {@link #copyPlainPayloads} writes
     * for the next {@code count} PLAIN BINARY values. Callers size the per-batch destination from this
     * rather than from {@link ByteBuffer#remaining()}: a batch is a small slice of a page, so a
     * page-remainder bound makes each batch allocate close to a whole page, which is quadratic per page.
     * <p>
     * Walks the {@code [int32 LE length][bytes]} prefixes with absolute reads, so the buffer position
     * is left unchanged. A truncated length prefix, or a payload running past the buffer limit, throws
     * {@link BufferUnderflowException} as the relative reads in {@link #copyPlainPayloads} do. A negative
     * length, which only a corrupt page holds, counts as an empty value exactly as in
     * {@link #copyPlainPayloads}: it contributes no payload bytes and only its 4-byte prefix is consumed,
     * so the two methods always agree. Bounds are checked as {@code length > limit - pos}, so neither
     * the position nor the total can overflow: the total never exceeds the buffer limit.
     */
    int plainPayloadBytes(int count) {
        int pos = buffer.position();
        int limit = buffer.limit();
        int total = 0;
        for (int i = 0; i < count; i++) {
            if (limit - pos < Integer.BYTES) {
                throw new BufferUnderflowException();
            }
            int length = buffer.getInt(pos);
            pos += Integer.BYTES;
            if (length <= 0) {
                continue;
            }
            if (length > limit - pos) {
                throw new BufferUnderflowException();
            }
            pos += length;
            total += length;
        }
        return total;
    }

    /**
     * Copies {@code count} PLAIN BINARY payloads (no length prefixes) into {@code dest} starting
     * at {@code destOff}. {@code exclusiveEnds[i]} is the exclusive dest offset of value {@code i}
     * relative to {@code destOff}. Returns packed length.
     */
    int copyPlainPayloads(byte[] dest, int destOff, int[] exclusiveEnds, int count) {
        int d = destOff;
        if (buffer.hasArray()) {
            byte[] backing = buffer.array();
            int base = buffer.arrayOffset();
            for (int i = 0; i < count; i++) {
                int length = buffer.getInt();
                if (length > 0) {
                    int pos = buffer.position();
                    System.arraycopy(backing, base + pos, dest, d, length);
                    buffer.position(pos + length);
                    d += length;
                }
                exclusiveEnds[i] = d - destOff;
            }
        } else {
            for (int i = 0; i < count; i++) {
                int length = buffer.getInt();
                if (length > 0) {
                    buffer.get(dest, d, length);
                    d += length;
                }
                exclusiveEnds[i] = d - destOff;
            }
        }
        return d - destOff;
    }

    /** Returned BytesRef instances share storage with the value buffer and must be copied before the buffer is reused. */
    void readFixedBinaries(BytesRef[] values, int offset, int count, int fixedLength) {
        if (buffer.hasArray()) {
            byte[] backing = buffer.array();
            int baseOffset = buffer.arrayOffset();
            for (int i = 0; i < count; i++) {
                int pos = buffer.position();
                values[offset + i] = new BytesRef(backing, baseOffset + pos, fixedLength);
                buffer.position(pos + fixedLength);
            }
        } else {
            for (int i = 0; i < count; i++) {
                byte[] copy = UninitializedArrays.newByteArray(fixedLength);
                buffer.get(copy);
                values[offset + i] = new BytesRef(copy);
            }
        }
    }

    double readOneDouble() {
        return buffer.getDouble();
    }

    float readOneFloat() {
        return buffer.getFloat();
    }

    long readOneLong() {
        return buffer.getLong();
    }

    int readOneInt() {
        return buffer.getInt();
    }

    void skipInts(int count) {
        buffer.position(buffer.position() + (count << 2));
    }

    void skipLongs(int count) {
        buffer.position(buffer.position() + (count << 3));
    }

    void skipFloats(int count) {
        buffer.position(buffer.position() + (count << 2));
    }

    void skipDoubles(int count) {
        buffer.position(buffer.position() + (count << 3));
    }

    void skipBooleans(int count) {
        int totalBits = boolBitOffset + count;
        buffer.position(buffer.position() + totalBits / 8);
        boolBitOffset = totalBits % 8;
    }

    void skipBinaries(int count) {
        for (int i = 0; i < count; i++) {
            int length = buffer.getInt();
            buffer.position(buffer.position() + length);
        }
    }

    void skipFixedBinaries(int count, int fixedLength) {
        buffer.position(buffer.position() + count * fixedLength);
    }
}
