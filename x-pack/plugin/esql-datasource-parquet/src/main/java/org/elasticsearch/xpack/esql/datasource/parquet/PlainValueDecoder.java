/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;

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
        float[] tmp = new float[count];
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
                byte[] copy = new byte[length];
                buffer.get(copy);
                values[offset + i] = new BytesRef(copy);
            }
        }
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
                byte[] copy = new byte[fixedLength];
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
