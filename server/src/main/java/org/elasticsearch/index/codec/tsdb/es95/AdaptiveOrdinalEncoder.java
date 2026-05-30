/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.elasticsearch.index.codec.tsdb.DocValuesForUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;

/**
 * Encodes a fixed-size block of ordinals using one of four per-block modes:
 * CONST, RLE, BITPACK_LOCAL, or LEGACY. The chosen mode minimizes serialized
 * bytes for the block. Wire format: 1 mode byte + mode-specific payload.
 *
 * <p>This commit adds BITPACK_LOCAL (pack at bits for `max - min`, subtracting
 * `min` first) on top of CONST and LEGACY. RLE lands in a subsequent commit.
 */
final class AdaptiveOrdinalEncoder {

    static final byte MODE_CONST = 0;
    static final byte MODE_RLE = 1;
    static final byte MODE_BITPACK_LOCAL = 2;
    static final byte MODE_LEGACY = 3;

    private final DocValuesForUtil forUtil;
    private final long[] scratch;

    AdaptiveOrdinalEncoder(int blockSize) {
        this.forUtil = new DocValuesForUtil(blockSize);
        this.scratch = new long[blockSize];
    }

    /**
     * Encodes one block of ordinals to {@code out}. May mutate {@code in} for
     * certain {@code bitsPerOrd} branches inside {@link DocValuesForUtil#encode};
     * callers that need the original must copy beforehand. {@code in} must have
     * length equal to the encoder block size; empty arrays are not supported.
     */
    void encodeOrdinals(long[] in, DataOutput out, int bitsPerOrd) throws IOException {
        long first = in[0];
        long min = first;
        long max = first;
        boolean allSame = true;
        for (int i = 1; i < in.length; i++) {
            long v = in[i];
            if (v != first) {
                allSame = false;
            }
            if (v < min) {
                min = v;
            }
            if (v > max) {
                max = v;
            }
        }
        if (allSame) {
            out.writeByte(MODE_CONST);
            out.writeVLong(first);
            return;
        }

        int localBits = bitsRequired(max - min);
        int roundedLocalBits = DocValuesForUtil.roundBits(localBits);
        int roundedSegmentBits = DocValuesForUtil.roundBits(bitsPerOrd);
        long bytesLegacy = 1L + ((long) in.length * roundedSegmentBits + 7) / 8;
        long bytesLocal = 1L + vLongSize(min) + 1L + ((long) in.length * roundedLocalBits + 7) / 8;

        if (bytesLocal < bytesLegacy) {
            out.writeByte(MODE_BITPACK_LOCAL);
            out.writeVLong(min);
            out.writeByte((byte) localBits);
            for (int i = 0; i < in.length; i++) {
                scratch[i] = in[i] - min;
            }
            forUtil.encode(scratch, localBits, out);
            return;
        }

        out.writeByte(MODE_LEGACY);
        forUtil.encode(in, bitsPerOrd, out);
    }

    void decodeOrdinals(DataInput in, long[] out, int bitsPerOrd) throws IOException {
        byte mode = in.readByte();
        switch (mode) {
            case MODE_CONST: {
                long constValue = in.readVLong();
                Arrays.fill(out, constValue);
                return;
            }
            case MODE_BITPACK_LOCAL: {
                long minVal = in.readVLong();
                int bits = in.readByte() & 0xff;
                if (bits > 63) {
                    throw new CorruptIndexException(String.format(Locale.ROOT, "invalid BITPACK_LOCAL bits %d", bits), in);
                }
                forUtil.decode(bits, in, out);
                for (int i = 0; i < out.length; i++) {
                    out[i] += minVal;
                }
                return;
            }
            case MODE_LEGACY:
                forUtil.decode(bitsPerOrd, in, out);
                return;
            default:
                throw new CorruptIndexException(String.format(Locale.ROOT, "unknown adaptive ordinal block mode 0x%02x", mode & 0xff), in);
        }
    }

    private static int bitsRequired(long range) {
        return range == 0 ? 0 : 64 - Long.numberOfLeadingZeros(range);
    }

    private static int vLongSize(long value) {
        // NOTE: Lucene VLong is 1 byte per 7 bits, plus a final byte. Cap at 9 for max long.
        int bytes = 1;
        long unsigned = value;
        while ((unsigned & ~0x7FL) != 0) {
            bytes++;
            unsigned >>>= 7;
        }
        return bytes;
    }
}
