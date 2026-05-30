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
 * <p>This commit adds RLE on top of CONST, BITPACK_LOCAL, and LEGACY. RLE
 * encodes blocks composed of at most {@link #RLE_MAX_RUNS} runs as
 * {@code [mode][n_runs][(ord, run_length) * n_runs]}; it wins on tsid-boundary
 * blocks where a small number of distinct ordinals dominate.
 */
final class AdaptiveOrdinalEncoder {

    static final byte MODE_CONST = 0;
    static final byte MODE_RLE = 1;
    static final byte MODE_BITPACK_LOCAL = 2;
    static final byte MODE_LEGACY = 3;

    static final int RLE_MAX_RUNS = 16;

    private final DocValuesForUtil forUtil;
    private final long[] scratch;
    private final long[] scratchRunOrds = new long[RLE_MAX_RUNS + 1];
    private final int[] scratchRunLens = new int[RLE_MAX_RUNS + 1];

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
        int nRuns = 1;
        scratchRunOrds[0] = first;
        scratchRunLens[0] = 1;
        for (int i = 1; i < in.length; i++) {
            long v = in[i];
            if (v < min) {
                min = v;
            }
            if (v > max) {
                max = v;
            }
            if (nRuns <= RLE_MAX_RUNS) {
                if (v == in[i - 1]) {
                    scratchRunLens[nRuns - 1]++;
                } else if (nRuns < RLE_MAX_RUNS + 1) {
                    scratchRunOrds[nRuns] = v;
                    scratchRunLens[nRuns] = 1;
                    nRuns++;
                }
                if (nRuns > RLE_MAX_RUNS) {
                    // NOTE: cap tracking; remaining scan only updates min/max.
                    nRuns = RLE_MAX_RUNS + 1;
                }
            }
        }

        if (nRuns == 1) {
            out.writeByte(MODE_CONST);
            out.writeVLong(first);
            return;
        }

        int localBits = bitsRequired(max - min);
        int roundedLocalBits = DocValuesForUtil.roundBits(localBits);
        int roundedSegmentBits = DocValuesForUtil.roundBits(bitsPerOrd);
        long bytesLegacy = 1L + ((long) in.length * roundedSegmentBits + 7) / 8;
        long bytesLocal = 1L + vLongSize(min) + 1L + ((long) in.length * roundedLocalBits + 7) / 8;
        long bytesRle = Long.MAX_VALUE;
        if (nRuns <= RLE_MAX_RUNS) {
            bytesRle = 1L + vIntSize(nRuns);
            for (int r = 0; r < nRuns; r++) {
                bytesRle += vLongSize(scratchRunOrds[r]) + vIntSize(scratchRunLens[r]);
            }
        }

        // NOTE: tie-break order CONST > RLE > BITPACK_LOCAL > LEGACY (smaller mode wins).
        if (bytesRle <= bytesLocal && bytesRle <= bytesLegacy) {
            out.writeByte(MODE_RLE);
            out.writeVInt(nRuns);
            for (int r = 0; r < nRuns; r++) {
                out.writeVLong(scratchRunOrds[r]);
                out.writeVInt(scratchRunLens[r]);
            }
            return;
        }

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
            case MODE_RLE: {
                int n = in.readVInt();
                if (n < 1 || n > RLE_MAX_RUNS) {
                    throw new CorruptIndexException(String.format(Locale.ROOT, "invalid RLE run count %d", n), in);
                }
                int pos = 0;
                for (int r = 0; r < n; r++) {
                    long ord = in.readVLong();
                    int run = in.readVInt();
                    if (run < 1 || pos + run > out.length) {
                        throw new CorruptIndexException(String.format(Locale.ROOT, "invalid RLE run length %d at pos %d", run, pos), in);
                    }
                    Arrays.fill(out, pos, pos + run, ord);
                    pos += run;
                }
                if (pos != out.length) {
                    throw new CorruptIndexException(String.format(Locale.ROOT, "RLE runs sum to %d (expected %d)", pos, out.length), in);
                }
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

    private static int vIntSize(int value) {
        // NOTE: Lucene VInt is 1 byte per 7 bits, plus a final byte. Cap at 5 for max int.
        int bytes = 1;
        int unsigned = value;
        while ((unsigned & ~0x7F) != 0) {
            bytes++;
            unsigned >>>= 7;
        }
        return bytes;
    }
}
