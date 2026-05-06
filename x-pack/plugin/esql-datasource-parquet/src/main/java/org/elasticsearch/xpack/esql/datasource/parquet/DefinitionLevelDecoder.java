/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.elasticsearch.xpack.esql.core.util.Check;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Bulk decoder for Parquet definition levels, producing a {@link WordMask} of null positions
 * for nullable flat columns. Implements the Parquet RLE/BitPacked hybrid format directly so
 * runs of all-null or all-present values can be applied to the output bitmask in O(1) per run
 * instead of O(N) per value.
 *
 * <p>For non-nullable columns ({@code maxDefLevel == 0}), no def-level section exists in the
 * page and all operations are no-ops.
 *
 * <p>The decoder is specialized for the overwhelmingly common case of {@code maxDefLevel == 1}
 * (a nullable flat column), where each def level is a single bit (0 = null, 1 = present) and
 * a bit-packed group of 8 def levels is exactly one byte. The general path handles
 * {@code maxDefLevel > 1} (e.g. nested types reaching this code via the flat-column read path).
 */
final class DefinitionLevelDecoder {

    private boolean nonNullable;
    private int maxDefLevel;
    private int bitWidth;
    private ByteBuffer in;

    // RLE/BitPacked hybrid state across calls (a run can span several readBatch invocations).
    private boolean rleMode;
    private int rleLeft;
    private int rleValue;
    private byte[] packedBytes;
    private int packedByteOffset;
    private int packedRemaining;
    // Decoded values for the currently-loaded bit-packed group (only used when bitWidth > 1).
    private final int[] groupValues = new int[8];
    private int groupNext = 8;

    DefinitionLevelDecoder() {}

    void init(ByteBuffer defLevelBytes, int maxDefLevel, boolean hasLengthPrefix) {
        this.maxDefLevel = maxDefLevel;
        if (maxDefLevel <= 0) {
            this.nonNullable = true;
            this.in = null;
            return;
        }
        this.nonNullable = false;
        this.bitWidth = 32 - Integer.numberOfLeadingZeros(maxDefLevel);
        ByteBuffer source = defLevelBytes.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        if (hasLengthPrefix) {
            int payloadLen = source.getInt();
            Check.isTrue(source.remaining() >= payloadLen, "Truncated definition level payload");
            ByteBuffer payload = source.slice().order(ByteOrder.LITTLE_ENDIAN);
            payload.limit(payloadLen);
            this.in = payload;
        } else {
            this.in = source.slice().order(ByteOrder.LITTLE_ENDIAN);
        }
        this.rleMode = true;
        this.rleLeft = 0;
        this.rleValue = 0;
        this.packedRemaining = 0;
        this.packedByteOffset = 0;
        this.groupNext = 8;
    }

    /**
     * Decodes the next {@code count} definition levels, setting null positions in {@code nulls}
     * starting at {@code offset}. Returns the number of non-null values.
     */
    int readBatch(int count, WordMask nulls, int offset) {
        if (nonNullable) {
            return count;
        }
        if (count == 0) {
            return 0;
        }
        int produced = 0;
        int nullCount = 0;
        while (produced < count) {
            if (rleMode) {
                if (rleLeft == 0) {
                    startNextRun();
                    continue;
                }
                int take = Math.min(rleLeft, count - produced);
                if (rleValue < maxDefLevel) {
                    nulls.setRange(offset + produced, offset + produced + take);
                    nullCount += take;
                }
                produced += take;
                rleLeft -= take;
            } else {
                if (packedRemaining == 0) {
                    startNextRun();
                    continue;
                }
                int take = Math.min(packedRemaining, count - produced);
                nullCount += unpackInto(nulls, offset + produced, take);
                produced += take;
            }
        }
        return count - nullCount;
    }

    /**
     * Skips {@code count} definition levels without materializing values.
     * Returns the number of non-null values that were skipped.
     */
    int skip(int count) {
        if (nonNullable || count == 0) {
            return count;
        }
        int skipped = 0;
        int nullCount = 0;
        while (skipped < count) {
            if (rleMode) {
                if (rleLeft == 0) {
                    startNextRun();
                    continue;
                }
                int take = Math.min(rleLeft, count - skipped);
                if (rleValue < maxDefLevel) {
                    nullCount += take;
                }
                skipped += take;
                rleLeft -= take;
            } else {
                if (packedRemaining == 0) {
                    startNextRun();
                    continue;
                }
                int take = Math.min(packedRemaining, count - skipped);
                nullCount += skipPackedNulls(take);
                skipped += take;
            }
        }
        return count - nullCount;
    }

    /**
     * Consumes {@code count} bit-packed def levels (which must already be loaded into the
     * current group) and writes nulls into {@code mask} starting at {@code outOffset}.
     * Returns the number of nulls written.
     *
     * <p>For {@code bitWidth == 1} this consumes raw bytes 8 levels at a time without
     * staging a {@code groupValues[]} array; each bit in the packed byte directly maps
     * to a not-null bit, so the null bits are obtained by inverting the packed byte.
     */
    private int unpackInto(WordMask mask, int outOffset, int count) {
        int nullCount = 0;
        int remaining = count;
        if (bitWidth == 1) {
            // Fast path: drain in-flight partial group bit-by-bit (rare: only when readBatch
            // finished mid-group on a prior call), then process whole bytes (8 levels each),
            // and finally a tail byte for the last <8 levels.
            //
            // Invariant: when groupNext < 8, the in-flight byte is packedBytes[packedByteOffset - 1].
            // groupNext drops below 8 only after a tail-byte read below (which incremented
            // packedByteOffset before setting groupNext = remaining), so packedByteOffset >= 1
            // is guaranteed in this branch.
            while (groupNext < 8 && remaining > 0) {
                int packed = packedBytes[packedByteOffset - 1] & 0xFF;
                if (((packed >>> groupNext) & 1) == 0) {
                    mask.set(outOffset);
                    nullCount++;
                }
                groupNext++;
                outOffset++;
                remaining--;
                packedRemaining--;
            }
            // Bulk path: read 8 bytes (= 64 def levels) at a time and OR the inverted bits
            // straight into the WordMask's underlying long[]. Parquet packs def levels LSB-first
            // within each byte, and reading 8 bytes as little-endian gives a long whose bit i
            // matches def level i in the 64-level group — same convention as WordMask.set, so
            // the inverted long can be OR-ed at any bit position via WordMask.orLongAt.
            while (remaining >= 64) {
                long packedLong = readLittleEndianLong(packedBytes, packedByteOffset);
                packedByteOffset += 8;
                long nullLong = ~packedLong;
                if (nullLong != 0) {
                    nullCount += 64 - Long.bitCount(packedLong);
                    mask.orLongAt(outOffset, nullLong);
                }
                outOffset += 64;
                remaining -= 64;
                packedRemaining -= 64;
            }
            // Per-byte path: drains the [0, 64) byte tail of the bulk loop above.
            while (remaining >= 8) {
                int packed = packedBytes[packedByteOffset++] & 0xFF;
                int nullByte = ~packed & 0xFF;
                // Single popcount + iterate-set-bits is faster than testing 8 bits unconditionally
                // because realistic flat-column null patterns produce mostly-empty inverted bytes
                // (nulls are sparse) — Integer.bitCount maps to a single CPU popcnt instruction.
                if (nullByte != 0) {
                    nullCount += Integer.bitCount(nullByte);
                    int b = nullByte;
                    do {
                        int bit = Integer.numberOfTrailingZeros(b);
                        mask.set(outOffset + bit);
                        b &= b - 1;
                    } while (b != 0);
                }
                outOffset += 8;
                remaining -= 8;
                packedRemaining -= 8;
            }
            if (remaining > 0) {
                int packed = packedBytes[packedByteOffset++] & 0xFF;
                for (int i = 0; i < remaining; i++) {
                    if (((packed >>> i) & 1) == 0) {
                        mask.set(outOffset + i);
                        nullCount++;
                    }
                }
                groupNext = remaining;
                packedRemaining -= remaining;
            }
            return nullCount;
        }
        // General path (bitWidth > 1): unpack 8 values at a time into groupValues[].
        while (remaining > 0) {
            if (groupNext >= 8) {
                loadPackedGroup();
            }
            int inGroup = Math.min(8 - groupNext, remaining);
            for (int i = 0; i < inGroup; i++) {
                if (groupValues[groupNext + i] < maxDefLevel) {
                    mask.set(outOffset + i);
                    nullCount++;
                }
            }
            groupNext += inGroup;
            outOffset += inGroup;
            remaining -= inGroup;
            packedRemaining -= inGroup;
        }
        return nullCount;
    }

    /**
     * Skip variant of {@link #unpackInto}: consumes {@code count} bit-packed def levels
     * without writing to a mask, returning the number of nulls encountered.
     */
    private int skipPackedNulls(int count) {
        int nullCount = 0;
        int remaining = count;
        if (bitWidth == 1) {
            while (groupNext < 8 && remaining > 0) {
                int packed = packedBytes[packedByteOffset - 1] & 0xFF;
                if (((packed >>> groupNext) & 1) == 0) {
                    nullCount++;
                }
                groupNext++;
                remaining--;
                packedRemaining--;
            }
            // Bulk path: pop 64 def levels per iteration (popcount on the inverted long).
            while (remaining >= 64) {
                long packedLong = readLittleEndianLong(packedBytes, packedByteOffset);
                packedByteOffset += 8;
                nullCount += 64 - Long.bitCount(packedLong);
                remaining -= 64;
                packedRemaining -= 64;
            }
            while (remaining >= 8) {
                int packed = packedBytes[packedByteOffset++] & 0xFF;
                nullCount += Integer.bitCount(~packed & 0xFF);
                remaining -= 8;
                packedRemaining -= 8;
            }
            if (remaining > 0) {
                int packed = packedBytes[packedByteOffset++] & 0xFF;
                for (int i = 0; i < remaining; i++) {
                    if (((packed >>> i) & 1) == 0) {
                        nullCount++;
                    }
                }
                groupNext = remaining;
                packedRemaining -= remaining;
            }
            return nullCount;
        }
        while (remaining > 0) {
            if (groupNext >= 8) {
                loadPackedGroup();
            }
            int inGroup = Math.min(8 - groupNext, remaining);
            for (int i = 0; i < inGroup; i++) {
                if (groupValues[groupNext + i] < maxDefLevel) {
                    nullCount++;
                }
            }
            groupNext += inGroup;
            remaining -= inGroup;
            packedRemaining -= inGroup;
        }
        return nullCount;
    }

    private void startNextRun() {
        Check.isTrue(in.hasRemaining(), "Truncated definition level stream");
        int header = readUnsignedVarInt();
        if ((header & 1) == 0) {
            rleMode = true;
            rleLeft = header >>> 1;
            rleValue = readIntLittleEndianPadded();
            packedRemaining = 0;
            groupNext = 8;
        } else {
            rleMode = false;
            int numGroups = header >>> 1;
            packedRemaining = numGroups * 8;
            int byteLen = numGroups * bitWidth;
            Check.isTrue(in.remaining() >= byteLen, "Truncated bit-packed definition level data");
            if (packedBytes == null || packedBytes.length < byteLen) {
                packedBytes = new byte[byteLen];
            }
            in.get(packedBytes, 0, byteLen);
            packedByteOffset = 0;
            groupNext = 8;
            rleLeft = 0;
        }
    }

    private void loadPackedGroup() {
        // bitWidth is always >= 1 here: init short-circuits to nonNullable=true when maxDefLevel<=0,
        // and unpackInto / skipPackedNulls use a bitWidth==1 fast path that never enters this method.
        unpack8Values(packedBytes, packedByteOffset, groupValues, bitWidth);
        packedByteOffset += bitWidth;
        groupNext = 0;
    }

    private int readUnsignedVarInt() {
        int shift = 0;
        int result = 0;
        while (true) {
            Check.isTrue(in.hasRemaining(), "Truncated varint in definition level stream");
            int b = in.get() & 0xFF;
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
            shift += 7;
            Check.isTrue(shift <= 35, "Varint overflow in definition level stream");
        }
    }

    /**
     * Reads 8 bytes from {@code data} starting at {@code offset} as a little-endian {@code long}.
     * The caller must ensure {@code offset + 8 <= data.length}.
     */
    private static long readLittleEndianLong(byte[] data, int offset) {
        return (data[offset] & 0xFFL) | ((data[offset + 1] & 0xFFL) << 8) | ((data[offset + 2] & 0xFFL) << 16) | ((data[offset + 3] & 0xFFL)
            << 24) | ((data[offset + 4] & 0xFFL) << 32) | ((data[offset + 5] & 0xFFL) << 40) | ((data[offset + 6] & 0xFFL) << 48)
            | ((data[offset + 7] & 0xFFL) << 56);
    }

    private int readIntLittleEndianPadded() {
        int n = (bitWidth + 7) >>> 3;
        if (n == 0) {
            return 0;
        }
        Check.isTrue(in.remaining() >= n, "Truncated padded definition level value");
        int v = 0;
        for (int i = 0; i < n; i++) {
            v |= (in.get() & 0xFF) << (8 * i);
        }
        return v;
    }

    /**
     * Unpacks 8 def-level values from the bit-packed stream. Called only from
     * {@link #loadPackedGroup}, which is itself only reachable for {@code bitWidth >= 2}
     * (the {@code bitWidth == 1} fast path bypasses {@code groupValues[]} entirely).
     */
    private static void unpack8Values(byte[] packed, int byteOffset, int[] out, int bitWidth) {
        switch (bitWidth) {
            case 2 -> {
                int word = (packed[byteOffset] & 0xFF) | ((packed[byteOffset + 1] & 0xFF) << 8);
                for (int i = 0; i < 8; i++) {
                    out[i] = (word >>> (i * 2)) & 0x3;
                }
            }
            case 4 -> {
                int word = (packed[byteOffset] & 0xFF) | ((packed[byteOffset + 1] & 0xFF) << 8) | ((packed[byteOffset + 2] & 0xFF) << 16)
                    | ((packed[byteOffset + 3] & 0xFF) << 24);
                for (int i = 0; i < 8; i++) {
                    out[i] = (word >>> (i * 4)) & 0xF;
                }
            }
            case 8 -> {
                for (int i = 0; i < 8; i++) {
                    out[i] = packed[byteOffset + i] & 0xFF;
                }
            }
            default -> {
                int bitBase = byteOffset * 8;
                for (int i = 0; i < 8; i++) {
                    out[i] = readBitsLE(packed, bitBase + i * bitWidth, bitWidth);
                }
            }
        }
    }

    private static int readBitsLE(byte[] data, int bitOffset, int width) {
        int byteIdx = bitOffset >>> 3;
        int bitInByte = bitOffset & 7;
        long word = 0;
        int bytesNeeded = ((bitInByte + width) + 7) >>> 3;
        for (int i = 0; i < bytesNeeded && (byteIdx + i) < data.length; i++) {
            word |= (long) (data[byteIdx + i] & 0xFF) << (i * 8);
        }
        return (int) ((word >>> bitInByte) & (width == 32 ? 0xFFFFFFFFL : ((1L << width) - 1)));
    }
}
