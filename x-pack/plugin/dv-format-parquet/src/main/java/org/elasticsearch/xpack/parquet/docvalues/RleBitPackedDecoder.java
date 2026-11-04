/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import java.io.IOException;

/**
 * Decoder of the hybrid encoding that Parquet uses for dictionary indices and for levels: a sequence of runs, each of which is
 * either a repeated value or a group of bit-packed values.
 *
 * <p>A run starts with a varint. When its lowest bit is {@code 0}, the run repeats one value {@code header >> 1} times, and the value
 * follows in {@code ceil(bitWidth / 8)} bytes, little-endian. When the bit is {@code 1}, {@code header >> 1} groups of 8 values follow,
 * packed with {@code bitWidth} bits for each value, starting at the lowest bit of the first byte.
 */
final class RleBitPackedDecoder {

    private RleBitPackedDecoder() {}

    /**
     * Decodes {@code count} values from {@code input[offset, end)} into {@code output}.
     *
     * @throws IOException if the input ends before {@code count} values, or a run does not fit the bit width
     */
    static void decode(byte[] input, int offset, int end, int bitWidth, int[] output, int count) throws IOException {
        if (bitWidth < 0 || bitWidth > 32) {
            throw new IOException("invalid bit width [" + bitWidth + "]");
        }
        int position = offset;
        int produced = 0;
        while (produced < count) {
            if (position >= end) {
                throw new IOException("the encoded values end after [" + produced + "] of [" + count + "] values");
            }
            // the varint header of the run
            int header = 0;
            int shift = 0;
            while (true) {
                if (position >= end) {
                    throw new IOException("a run header is cut off");
                }
                int b = input[position++] & 0xFF;
                header |= (b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    break;
                }
                shift += 7;
                if (shift > 28) {
                    throw new IOException("a run header is too long");
                }
            }
            if ((header & 1) == 0) {
                int runLength = header >>> 1;
                int valueBytes = (bitWidth + 7) / 8;
                if (position + valueBytes > end) {
                    throw new IOException("a repeated value is cut off");
                }
                int value = 0;
                for (int i = 0; i < valueBytes; i++) {
                    value |= (input[position + i] & 0xFF) << (8 * i);
                }
                position += valueBytes;
                int take = Math.min(runLength, count - produced);
                for (int i = 0; i < take; i++) {
                    output[produced++] = value;
                }
            } else {
                int groups = header >>> 1;
                long packedValues = (long) groups * 8;
                long packedBytes = (long) groups * bitWidth;
                if (position + packedBytes > end) {
                    throw new IOException("a bit-packed group is cut off");
                }
                long accumulator = 0;
                int bitsAvailable = 0;
                int mask = bitWidth == 32 ? -1 : (1 << bitWidth) - 1;
                long taken = 0;
                int bytePosition = position;
                while (taken < packedValues && produced < count) {
                    while (bitsAvailable < bitWidth) {
                        accumulator |= (long) (input[bytePosition++] & 0xFF) << bitsAvailable;
                        bitsAvailable += 8;
                    }
                    output[produced++] = bitWidth == 0 ? 0 : (int) (accumulator & mask);
                    accumulator >>>= bitWidth;
                    bitsAvailable -= bitWidth;
                    taken++;
                }
                position += (int) packedBytes;
            }
        }
    }
}
