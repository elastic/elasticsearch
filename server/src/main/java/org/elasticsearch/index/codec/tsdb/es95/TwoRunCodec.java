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
 * Two-run block codec (encoding 1). Writes {@code (firstOrd << 2) | 0b01} as
 * the header, followed by the first run length (vint) and the delta from the
 * first to the second ordinal (zlong). Byte-for-byte identical to the legacy
 * two-runs encoding so single-valued fields with a per-block boundary
 * transition carry zero overhead. Stateless; access via {@link #INSTANCE}.
 */
final class TwoRunCodec implements BlockModeCodec {

    static final int ENCODING = 1;
    static final TwoRunCodec INSTANCE = new TwoRunCodec();

    private TwoRunCodec() {}

    @Override
    public int encoding() {
        return ENCODING;
    }

    @Override
    public long estimateSize(final long[] in, final BlockStats stats, int bitsPerOrd) {
        if (stats.nRuns != 2 || bitsPerOrd >= 62) {
            return Long.MAX_VALUE;
        }
        long first = stats.runOrds[0];
        long second = stats.runOrds[1];
        return vLongSize((first << 2) | 0b01) + vIntSize(stats.runLens[0]) + zLongSize(second - first);
    }

    @Override
    public void encodePayload(final long[] in, final BlockStats stats, final CodecContext ctx, final DataOutput out, int bitsPerOrd)
        throws IOException {
        long first = stats.runOrds[0];
        long second = stats.runOrds[1];
        out.writeVLong((first << 2) | 0b01);
        out.writeVInt(stats.runLens[0]);
        out.writeZLong(second - first);
    }

    @Override
    public void decodePayload(final CodecContext ctx, final DataInput in, final long[] out, int bitsPerOrd, long leadingVLong)
        throws IOException {
        long first = leadingVLong >>> 2;
        int runLen = in.readVInt();
        if (runLen < 1 || runLen >= out.length) {
            throw new CorruptIndexException(String.format(Locale.ROOT, "invalid TWO_RUN first-run length %d", runLen), in);
        }
        long second = first + in.readZLong();
        Arrays.fill(out, 0, runLen, first);
        Arrays.fill(out, runLen, out.length, second);
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

    private static int zLongSize(long signed) {
        long zig = (signed << 1) ^ (signed >> 63);
        return vLongSize(zig);
    }
}
