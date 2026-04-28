/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.bzip2;

import org.elasticsearch.common.unit.ByteSizeValue;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Scans raw bzip2 compressed bytes for block boundaries by searching for the
 * 48-bit block magic {@code 0x314159265359}. The magic can appear at any bit
 * position (bzip2 uses bit-level packing), so the scanner checks every bit
 * offset, not just byte boundaries.
 *
 * <p>Returns byte offsets suitable for range reads. Each offset points to the
 * byte containing the first bit of the block magic. The decompressor must be
 * given the stream from that byte onward (with a synthetic header prepended).
 */
final class Bzip2BlockScanner {

    static final long BLOCK_MAGIC = 0x314159265359L;
    static final long MAGIC_MASK = 0xFFFFFFFFFFFFL;

    static final int BZIP2_HEADER_SIZE = 4;

    /** Buffered read size for sequential scans (micro-optimization: fewer read syscalls). */
    private static final int SCAN_BUFFER_SIZE = (int) ByteSizeValue.ofKb(128).getBytes();

    private Bzip2BlockScanner() {}

    /**
     * Scans the given input stream for bzip2 block boundaries using bit-level
     * scanning. Returns byte offsets (relative to the start of the stream) where
     * each block's magic marker begins. Scans at most {@code rangeLength} bytes.
     *
     * <p>Each returned offset is the byte position of the byte that contains the
     * first bit of the 48-bit block magic. The {@code bitOffset} within that byte
     * is also tracked internally for correct synthetic-header construction.
     *
     * <p>Uses a fixed-size buffer and preserves the sliding bit window across buffer
     * boundaries so block markers are not missed at read boundaries.
     */
    static long[] scanBlockOffsets(InputStream raw, long rangeLength) throws IOException {
        if (rangeLength <= 0) {
            return new long[0];
        }
        List<Long> offsets = new ArrayList<>();
        byte[] buf = new byte[SCAN_BUFFER_SIZE];
        long streamBytesSeen = 0;
        long window = 0;
        int bitsInWindow = 0;
        while (streamBytesSeen < rangeLength) {
            int toRead = (int) Math.min(buf.length, rangeLength - streamBytesSeen);
            int n = readFully(raw, buf, 0, toRead);
            if (n <= 0) {
                break;
            }
            for (int i = 0; i < n; i++) {
                int b = buf[i] & 0xFF;
                long bytesRead = streamBytesSeen + i + 1;
                long packed = appendByteBits(b, bytesRead, window, bitsInWindow, offsets);
                window = unpackWindowAfterByte(packed);
                bitsInWindow = unpackBitsInWindowAfterByte(packed);
            }
            streamBytesSeen += n;
        }
        long[] result = new long[offsets.size()];
        for (int i = 0; i < offsets.size(); i++) {
            result[i] = offsets.get(i);
        }
        return result;
    }

    /**
     * Shifts one byte (MSB-first, matching bzip2 bit order) into the 48-bit window, records block
     * magic hits, and returns packed state for {@link #unpackWindowAfterByte} and
     * {@link #unpackBitsInWindowAfterByte} so the hot path does not re-shift the same byte.
     */
    private static long appendByteBits(long b, long bytesRead, long window, int bitsInWindow, List<Long> offsets) {
        // Bit order matches the original per-bit loop: bit 7 down to 0.
        window = ((window << 1) | ((b >> 7) & 1)) & MAGIC_MASK;
        bitsInWindow = bitsInWindow < 48 ? bitsInWindow + 1 : 48;
        if (bitsInWindow == 48) {
            maybeRecordMagic(window, bytesRead, 7, offsets);
        }
        window = ((window << 1) | ((b >> 6) & 1)) & MAGIC_MASK;
        bitsInWindow = bitsInWindow < 48 ? bitsInWindow + 1 : 48;
        if (bitsInWindow == 48) {
            maybeRecordMagic(window, bytesRead, 6, offsets);
        }
        window = ((window << 1) | ((b >> 5) & 1)) & MAGIC_MASK;
        bitsInWindow = bitsInWindow < 48 ? bitsInWindow + 1 : 48;
        if (bitsInWindow == 48) {
            maybeRecordMagic(window, bytesRead, 5, offsets);
        }
        window = ((window << 1) | ((b >> 4) & 1)) & MAGIC_MASK;
        bitsInWindow = bitsInWindow < 48 ? bitsInWindow + 1 : 48;
        if (bitsInWindow == 48) {
            maybeRecordMagic(window, bytesRead, 4, offsets);
        }
        window = ((window << 1) | ((b >> 3) & 1)) & MAGIC_MASK;
        bitsInWindow = bitsInWindow < 48 ? bitsInWindow + 1 : 48;
        if (bitsInWindow == 48) {
            maybeRecordMagic(window, bytesRead, 3, offsets);
        }
        window = ((window << 1) | ((b >> 2) & 1)) & MAGIC_MASK;
        bitsInWindow = bitsInWindow < 48 ? bitsInWindow + 1 : 48;
        if (bitsInWindow == 48) {
            maybeRecordMagic(window, bytesRead, 2, offsets);
        }
        window = ((window << 1) | ((b >> 1) & 1)) & MAGIC_MASK;
        bitsInWindow = bitsInWindow < 48 ? bitsInWindow + 1 : 48;
        if (bitsInWindow == 48) {
            maybeRecordMagic(window, bytesRead, 1, offsets);
        }
        window = ((window << 1) | (b & 1)) & MAGIC_MASK;
        bitsInWindow = bitsInWindow < 48 ? bitsInWindow + 1 : 48;
        if (bitsInWindow == 48) {
            maybeRecordMagic(window, bytesRead, 0, offsets);
        }
        return ((window & MAGIC_MASK) << 8) | (bitsInWindow & 0xFFL);
    }

    private static long unpackWindowAfterByte(long packed) {
        return (packed >>> 8) & MAGIC_MASK;
    }

    private static int unpackBitsInWindowAfterByte(long packed) {
        return (int) (packed & 0xFFL);
    }

    private static void maybeRecordMagic(long window, long bytesRead, int bitInByte, List<Long> offsets) {
        if (window == BLOCK_MAGIC) {
            long currentBitPos = (bytesRead - 1) * 8L + (7 - bitInByte);
            long magicStartBit = currentBitPos - 47;
            long byteOffset = magicStartBit / 8;
            if (byteOffset >= 0) {
                offsets.add(byteOffset);
            }
        }
    }

    private static int readFully(InputStream raw, byte[] buf, int off, int len) throws IOException {
        int total = 0;
        while (total < len) {
            int n = raw.read(buf, off + total, len - total);
            if (n < 0) {
                return total;
            }
            total += n;
        }
        return total;
    }

    /**
     * Merges sorted per-chunk offset arrays (each chunk's offsets are sorted) into one
     * sorted array with duplicates removed (e.g. from overlapped parallel scans).
     */
    static long[] mergeSortedUnique(long[][] parts) {
        int total = 0;
        for (long[] p : parts) {
            total += p.length;
        }
        if (total == 0) {
            return new long[0];
        }
        long[] merged = new long[total];
        int pos = 0;
        for (long[] p : parts) {
            System.arraycopy(p, 0, merged, pos, p.length);
            pos += p.length;
        }
        Arrays.sort(merged);
        int w = 0;
        long prev = Long.MIN_VALUE;
        for (long v : merged) {
            if (w == 0 || v != prev) {
                merged[w++] = v;
                prev = v;
            }
        }
        return Arrays.copyOf(merged, w);
    }

    /**
     * Reads the bzip2 block size digit from the file header. The header is
     * 4 bytes: {@code 'B'}, {@code 'Z'}, {@code 'h'}, and a digit {@code '1'..'9'}
     * indicating the block size in units of 100k.
     */
    static char readBlockSizeDigit(byte[] headerBytes) {
        if (headerBytes.length < BZIP2_HEADER_SIZE) {
            throw new IllegalArgumentException("Bzip2 header too short: " + headerBytes.length + " bytes");
        }
        if (headerBytes[0] != 'B' || headerBytes[1] != 'Z' || headerBytes[2] != 'h') {
            throw new IllegalArgumentException("Not a bzip2 file: invalid magic bytes");
        }
        char digit = (char) headerBytes[3];
        if (digit < '1' || digit > '9') {
            throw new IllegalArgumentException("Invalid bzip2 block size digit: " + digit);
        }
        return digit;
    }
}
