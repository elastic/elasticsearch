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

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;

/**
 * Run-length encoded block codec (encoding 3, sub-mode {@link #SUB_MODE}). Applies when
 * the block resolves to between 3 and {@link BlockStats#MAX_TRACKED_RUNS} runs; the
 * 1-run and 2-run cases are handled by {@link ConstantCodec} and {@link TwoRunCodec} at
 * zero header overhead.
 *
 * <p>Wire format after the encoding-3 header and sub-mode byte:
 * <pre>
 *   vint nRuns
 *   vlong ord_0
 *   vint runLen_0
 *   zlong delta_1, vint runLen_1
 *   ...
 *   zlong delta_{n-1}, vint runLen_{n-1}
 * </pre>
 *
 * <p>The first run carries the absolute ord; each subsequent run encodes the
 * zigzag-encoded signed delta from the previous run's ord, exploiting the fact that
 * neighboring runs from the same routing-path partition tend to sit within a small ord
 * distance of each other.
 */
final class RleCodec implements BlockModeCodec {

    static final int ENCODING = 3;
    static final byte SUB_MODE = 0;
    static final RleCodec INSTANCE = new RleCodec();

    private RleCodec() {}

    @Override
    public int encoding() {
        return ENCODING;
    }

    @Override
    public long estimateSize(final long[] in, final BlockStats stats, int bitsPerOrd) {
        if (stats.nRuns < 3 || stats.nRuns > BlockStats.MAX_TRACKED_RUNS) {
            return Long.MAX_VALUE;
        }
        long size = 1L + 1L + vIntSize(stats.nRuns);
        size += vLongSize(stats.runOrds[0]) + vIntSize(stats.runLens[0]);
        for (int r = 1; r < stats.nRuns; r++) {
            size += vLongSize(zigzagEncode(stats.runOrds[r] - stats.runOrds[r - 1])) + vIntSize(stats.runLens[r]);
        }
        return size;
    }

    @Override
    public void encodePayload(final long[] in, final BlockStats stats, final CodecContext ctx, final DataOutput out, int bitsPerOrd)
        throws IOException {
        out.writeVLong(0b111);
        out.writeByte(SUB_MODE);
        out.writeVInt(stats.nRuns);
        out.writeVLong(stats.runOrds[0]);
        out.writeVInt(stats.runLens[0]);
        for (int r = 1; r < stats.nRuns; r++) {
            out.writeVLong(zigzagEncode(stats.runOrds[r] - stats.runOrds[r - 1]));
            out.writeVInt(stats.runLens[r]);
        }
    }

    @Override
    public void decodePayload(final CodecContext ctx, final DataInput in, final long[] out, int bitsPerOrd, long leadingVLong)
        throws IOException {
        final int n = in.readVInt();
        if (n < 1 || n > BlockStats.MAX_TRACKED_RUNS) {
            throw new CorruptIndexException(String.format(Locale.ROOT, "invalid RLE run count %d", n), in);
        }
        long ord = in.readVLong();
        int run = in.readVInt();
        if (run < 1 || run > out.length) {
            throw new CorruptIndexException(String.format(Locale.ROOT, "invalid RLE run length %d at run 0", run), in);
        }
        Arrays.fill(out, 0, run, ord);
        int pos = run;
        for (int r = 1; r < n; r++) {
            ord += zigzagDecode(in.readVLong());
            run = in.readVInt();
            if (run < 1 || pos + run > out.length) {
                throw new CorruptIndexException(String.format(Locale.ROOT, "invalid RLE run length %d at run %d", run, r), in);
            }
            Arrays.fill(out, pos, pos + run, ord);
            pos += run;
        }
        if (pos != out.length) {
            throw new CorruptIndexException(String.format(Locale.ROOT, "RLE runs sum to %d (expected %d)", pos, out.length), in);
        }
    }

    private static long zigzagEncode(long value) {
        return (value << 1) ^ (value >> 63);
    }

    private static long zigzagDecode(long value) {
        return (value >>> 1) ^ -(value & 1);
    }

    private static int vIntSize(int value) {
        int bytes = 1;
        int unsigned = value;
        while ((unsigned & ~0x7F) != 0) {
            bytes++;
            unsigned >>>= 7;
        }
        return bytes;
    }

    private static int vLongSize(long value) {
        int bytes = 1;
        long unsigned = value;
        while ((unsigned & ~0x7FL) != 0) {
            bytes++;
            unsigned >>>= 7;
        }
        return bytes;
    }
}
