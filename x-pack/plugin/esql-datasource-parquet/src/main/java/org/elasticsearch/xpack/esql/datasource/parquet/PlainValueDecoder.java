/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.compute.data.UninitializedArrays;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Bulk PLAIN decoder over a little-endian value {@link ByteBuffer} for page-level Parquet reads.
 * Fixed-width types (INT32, INT64, FLOAT, DOUBLE) are read contiguously; BOOLEAN is bit-packed
 * (1 bit per value); BINARY is length-prefixed; FIXED_LEN_BYTE_ARRAY uses a fixed stride.
 */
final class PlainValueDecoder {

    private ByteBuffer buffer;

    /**
     * Heap PLAIN BINARY layout recorded by {@link #readBinaries} so UTF-8 region accept does not
     * walk the {@link BytesRef} array again. Invalid after {@link #init} and on the direct-buffer
     * copy path.
     */
    private final HeapBinaryLayout binaryLayout = new HeapBinaryLayout();

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
        binaryLayout.reset();
    }

    HeapBinaryLayout binaryLayout() {
        return binaryLayout;
    }

    void invalidateBinaryLayout() {
        binaryLayout.reset();
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
        binaryLayout.reset();
        if (buffer.hasArray()) {
            byte[] backing = buffer.array();
            int baseOffset = buffer.arrayOffset();
            // Layout run indices are 0-based in this read. Production always passes offset 0;
            // a non-zero offset still slices BytesRefs but skips recording.
            boolean recording = offset == 0 && count > 0;
            int expectedAbs = 0;
            int runFrom = 0;
            int runOff = 0;
            // Inline the ASCII-prefix mask: a nested-class static call showed up as 28% self in JFR.
            final int asciiPrefixMask = HeapBinaryLayout.ASCII_LENGTH_PREFIX_MASK;
            int i = 0;
            if (count > 0) {
                int length = buffer.getInt();
                int pos = buffer.position();
                int abs = baseOffset + pos;
                values[offset] = new BytesRef(backing, abs, length);
                if (recording) {
                    if (abs < Integer.BYTES) {
                        recording = false;
                        binaryLayout.reset();
                    } else {
                        runFrom = 0;
                        runOff = (length & asciiPrefixMask) == 0 ? abs - Integer.BYTES : abs;
                        expectedAbs = abs + length + Integer.BYTES;
                    }
                }
                buffer.position(pos + length);
                i = 1;
            }
            for (; i < count; i++) {
                int length = buffer.getInt();
                int pos = buffer.position();
                int abs = baseOffset + pos;
                values[offset + i] = new BytesRef(backing, abs, length);
                if (recording) {
                    if (abs != expectedAbs) {
                        recording = false;
                        binaryLayout.reset();
                    } else if ((length & asciiPrefixMask) != 0) {
                        binaryLayout.addRun(runFrom, i, runOff, abs - Integer.BYTES);
                        runFrom = i;
                        runOff = abs;
                    }
                    expectedAbs = abs + length + Integer.BYTES;
                }
                buffer.position(pos + length);
            }
            if (recording) {
                BytesRef last = values[count - 1];
                int lastEnd = last.offset + last.length;
                if (lastEnd >= last.offset && lastEnd <= backing.length) {
                    binaryLayout.addRun(runFrom, count, runOff, lastEnd);
                    binaryLayout.backing = backing;
                    binaryLayout.valueCount = count;
                    binaryLayout.valid = true;
                }
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

    /**
     * Contiguous heap PLAIN BINARY layout: one backing array and ASCII-prefix runs. Built while
     * {@link #readBinaries} already has each length in hand, or reconstructed once from sliced
     * {@link BytesRef}s in tests.
     */
    static final class HeapBinaryLayout {
        /**
         * A Parquet BINARY length prefix is four ASCII bytes iff no byte has the high bit set.
         * {@code length < 128} is sufficient but not necessary: 256..383 also qualify, while 128
         * does not ({@code 0x80} looks like a UTF-8 continuation).
         */
        static final int ASCII_LENGTH_PREFIX_MASK = 0x80808080;

        boolean valid;
        byte[] backing;
        int valueCount;
        int runCount;
        int[] runFrom;
        int[] runTo;
        int[] runOff;
        int[] runEnd;

        void reset() {
            valid = false;
            backing = null;
            valueCount = 0;
            runCount = 0;
        }

        void addRun(int from, int to, int off, int end) {
            if (from >= to) {
                return;
            }
            ensureCapacity();
            runFrom[runCount] = from;
            runTo[runCount] = to;
            runOff[runCount] = off;
            runEnd[runCount] = end;
            runCount++;
        }

        static boolean isAsciiLengthPrefix(int length) {
            return (length & ASCII_LENGTH_PREFIX_MASK) == 0;
        }

        /**
         * One walk over {@code vals} for tests that do not go through {@link #readBinaries}.
         * Production must use the decoder-recorded layout (zero extra {@link BytesRef} walks).
         */
        static HeapBinaryLayout fromValues(BytesRef[] vals, int count) {
            HeapBinaryLayout layout = new HeapBinaryLayout();
            if (count == 0) {
                return layout;
            }
            BytesRef first = vals[0];
            if (first.offset < Integer.BYTES) {
                return layout;
            }
            byte[] backing = first.bytes;
            int expectedOff = first.offset;
            int runFrom = 0;
            int regionOff = isAsciiLengthPrefix(first.length) ? first.offset - Integer.BYTES : first.offset;
            for (int i = 0; i < count; i++) {
                BytesRef v = vals[i];
                if (v.bytes != backing || v.offset != expectedOff || v.offset < Integer.BYTES) {
                    layout.reset();
                    return layout;
                }
                if (ByteUtils.readIntLE(backing, v.offset - Integer.BYTES) != v.length) {
                    layout.reset();
                    return layout;
                }
                if (i > 0 && isAsciiLengthPrefix(v.length) == false) {
                    layout.addRun(runFrom, i, regionOff, v.offset - Integer.BYTES);
                    runFrom = i;
                    regionOff = v.offset;
                }
                expectedOff = v.offset + v.length + Integer.BYTES;
            }
            BytesRef last = vals[count - 1];
            int lastEnd = last.offset + last.length;
            if (lastEnd < last.offset || lastEnd > backing.length) {
                layout.reset();
                return layout;
            }
            layout.addRun(runFrom, count, regionOff, lastEnd);
            layout.backing = backing;
            layout.valueCount = count;
            layout.valid = true;
            return layout;
        }

        private void ensureCapacity() {
            if (runFrom == null) {
                runFrom = new int[4];
                runTo = new int[4];
                runOff = new int[4];
                runEnd = new int[4];
                return;
            }
            if (runCount == runFrom.length) {
                int n = runFrom.length * 2;
                runFrom = Arrays.copyOf(runFrom, n);
                runTo = Arrays.copyOf(runTo, n);
                runOff = Arrays.copyOf(runOff, n);
                runEnd = Arrays.copyOf(runEnd, n);
            }
        }
    }
}
