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
 * Run-length encoded block codec. Applies when the block resolves to at
 * most {@link BlockStats#MAX_TRACKED_RUNS} runs. Payload is
 * {@code [n_runs:vint][(ord:vlong, run:vint) * n_runs]}. Stateless;
 * access via {@link #INSTANCE}.
 */
final class RleCodec implements BlockModeCodec {

    static final byte MODE = 2;
    static final RleCodec INSTANCE = new RleCodec();

    private RleCodec() {}

    @Override
    public byte mode() {
        return MODE;
    }

    @Override
    public long estimateSize(final long[] in, final BlockStats stats, int bitsPerOrd) {
        if (stats.nRuns > BlockStats.MAX_TRACKED_RUNS) {
            return Long.MAX_VALUE;
        }
        long size = vIntSize(stats.nRuns);
        for (int r = 0; r < stats.nRuns; r++) {
            size += vLongSize(stats.runOrds[r]) + vIntSize(stats.runLens[r]);
        }
        return size;
    }

    @Override
    public void encodePayload(final long[] in, final BlockStats stats, final DataOutput out, int bitsPerOrd) throws IOException {
        out.writeVInt(stats.nRuns);
        for (int r = 0; r < stats.nRuns; r++) {
            out.writeVLong(stats.runOrds[r]);
            out.writeVInt(stats.runLens[r]);
        }
    }

    @Override
    public void decodePayload(final DataInput in, final long[] out, int bitsPerOrd) throws IOException {
        int n = in.readVInt();
        if (n < 1 || n > BlockStats.MAX_TRACKED_RUNS) {
            throw new CorruptIndexException(String.format(Locale.ROOT, "invalid RLE run count %d", n), in);
        }
        int pos = 0;
        for (int r = 0; r < n; r++) {
            long ord = in.readVLong();
            int run = in.readVInt();
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
