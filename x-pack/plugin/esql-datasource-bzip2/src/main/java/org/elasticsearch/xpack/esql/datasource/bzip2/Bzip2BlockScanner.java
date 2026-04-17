/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.bzip2;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
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
    static final long EOS_MAGIC = 0x177245385090L;
    static final long MAGIC_MASK = 0xFFFFFFFFFFFFL;

    static final int BZIP2_HEADER_SIZE = 4;

    private Bzip2BlockScanner() {}

    /**
     * Scans the given input stream for bzip2 block boundaries using bit-level
     * scanning. Returns byte offsets (relative to the start of the stream) where
     * each block's magic marker begins. Scans at most {@code rangeLength} bytes.
     *
     * <p>Each returned offset is the byte position of the byte that contains the
     * first bit of the 48-bit block magic. The {@code bitOffset} within that byte
     * is also tracked internally for correct synthetic-header construction.
     */
    static long[] scanBlockOffsets(InputStream raw, long rangeLength) throws IOException {
        List<Long> offsets = new ArrayList<>();
        long window = 0;
        long bytesRead = 0;
        int bitsInWindow = 0;

        int b;
        while (bytesRead < rangeLength && (b = raw.read()) != -1) {
            bytesRead++;
            // Shift in 8 new bits, one at a time, checking for magic at each bit position
            for (int bit = 7; bit >= 0; bit--) {
                int bitVal = (b >> bit) & 1;
                window = ((window << 1) | bitVal) & MAGIC_MASK;
                bitsInWindow = Math.min(bitsInWindow + 1, 48);

                if (bitsInWindow < 48) {
                    continue;
                }

                if (window == BLOCK_MAGIC) {
                    // The magic's first bit was 47 bits ago from the current bit position.
                    // Current bit position: (bytesRead - 1) * 8 + (7 - bit)
                    // Magic start bit: currentBitPos - 47
                    long currentBitPos = (bytesRead - 1) * 8 + (7 - bit);
                    long magicStartBit = currentBitPos - 47;
                    long byteOffset = magicStartBit / 8;
                    if (byteOffset >= 0) {
                        offsets.add(byteOffset);
                    }
                }
            }
        }

        long[] result = new long[offsets.size()];
        for (int i = 0; i < offsets.size(); i++) {
            result[i] = offsets.get(i);
        }
        return result;
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
