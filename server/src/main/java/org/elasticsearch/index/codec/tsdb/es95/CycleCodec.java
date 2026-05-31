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
import java.util.Locale;

/**
 * Compact cycle block codec (encoding 3, sub-mode {@link #SUB_MODE}). Encodes a block whose
 * flat ord stream repeats with period {@code p} in {@code [2, blockSize / MAX_CYCLE_DIVISOR]}
 * as {@code (period, value_0, value_1, ..., value_{p-1})}. Targets the dominant SORTED_SET
 * mid-tsid block shape where every doc in a {@code _tsid} run emits the same K-ord tuple
 * (e.g. {@code host.ip}, {@code host.mac}).
 *
 * <p>The wire format has a small constant framing cost (3 bytes), much less than
 * {@link TupleRunCodec}'s 7-byte framing, so for pure-cycle blocks the codec is the
 * cheapest candidate by a few bytes per block. For blocks that are not pure cycles
 * (boundary, varying-K, partial-edge), this codec declines and {@link TupleRunCodec}
 * picks them up via its per-doc structure.
 *
 * <p>Wire format after the encoding-3 header and sub-mode byte:
 * <pre>
 *   vint period
 *   vlong first_ord
 *   vlong delta_1, ..., delta_{period - 1}    # delta_i = ord_i - ord_{i-1} - 1
 * </pre>
 *
 * <p>Stateless; access via {@link #INSTANCE}.
 */
public final class CycleCodec implements BlockModeCodec {

    /** Trailing-one-bits count for the ADAPTIVE_EXTRA dispatch. */
    public static final int ENCODING = 3;

    /** Sub-mode byte inside the ADAPTIVE_EXTRA dispatch. */
    public static final byte SUB_MODE = 3;

    public static final CycleCodec INSTANCE = new CycleCodec();

    private CycleCodec() {}

    @Override
    public int encoding() {
        return ENCODING;
    }

    @Override
    public long estimateSize(final long[] in, final BlockStats stats, int bitsPerOrd) {
        final int period = stats.cycleLength;
        if (period < 2) {
            return Long.MAX_VALUE;
        }
        long size = 1L + 1L + vIntSize(period) + vLongSize(in[0]);
        for (int k = 1; k < period; k++) {
            size += vLongSize(in[k] - in[k - 1] - 1L);
        }
        return size;
    }

    @Override
    public void encodePayload(final long[] in, final BlockStats stats, final CodecContext ctx, final DataOutput out, int bitsPerOrd)
        throws IOException {
        final int period = stats.cycleLength;
        out.writeVLong(0b111);
        out.writeByte(SUB_MODE);
        out.writeVInt(period);
        out.writeVLong(in[0]);
        for (int k = 1; k < period; k++) {
            out.writeVLong(in[k] - in[k - 1] - 1L);
        }
    }

    @Override
    public void decodePayload(final CodecContext ctx, final DataInput in, final long[] out, int bitsPerOrd, long leadingVLong)
        throws IOException {
        final int period = in.readVInt();
        if (period < 2 || period > out.length / BlockStats.MAX_CYCLE_DIVISOR) {
            throw new CorruptIndexException(String.format(Locale.ROOT, "invalid cycle period %d", period), in);
        }
        final long[] tuple = ctx.scratch;
        tuple[0] = in.readVLong();
        for (int k = 1; k < period; k++) {
            tuple[k] = tuple[k - 1] + in.readVLong() + 1L;
        }
        cyclicFill(out, tuple, period);
    }

    // NOTE: explicit small-K specializations let the JIT unroll the inner loop and
    // auto-vectorize the long writes. Without specialization the period stays a runtime
    // value and the `out[i] = tuple[i % period]` form blocks vectorization.
    private static void cyclicFill(final long[] out, final long[] tuple, int period) {
        final int n = out.length;
        if (period == 2) {
            final long t0 = tuple[0];
            final long t1 = tuple[1];
            for (int i = 0; i + 2 <= n; i += 2) {
                out[i] = t0;
                out[i + 1] = t1;
            }
            return;
        }
        if (period == 3) {
            final long t0 = tuple[0];
            final long t1 = tuple[1];
            final long t2 = tuple[2];
            int i = 0;
            final int limit = n - n % 3;
            for (; i < limit; i += 3) {
                out[i] = t0;
                out[i + 1] = t1;
                out[i + 2] = t2;
            }
            if (i < n) out[i++] = t0;
            if (i < n) out[i] = t1;
            return;
        }
        if (period == 4) {
            final long t0 = tuple[0];
            final long t1 = tuple[1];
            final long t2 = tuple[2];
            final long t3 = tuple[3];
            for (int i = 0; i + 4 <= n; i += 4) {
                out[i] = t0;
                out[i + 1] = t1;
                out[i + 2] = t2;
                out[i + 3] = t3;
            }
            return;
        }
        // NOTE: K >= 5 - lay down one tuple at position 0, then arraycopy it forward
        // in doubling chunks. arraycopy on a long[] is intrinsified to SIMD on the JVM.
        for (int k = 0; k < period; k++) {
            out[k] = tuple[k];
        }
        int filled = period;
        while (filled + filled <= n) {
            System.arraycopy(out, 0, out, filled, filled);
            filled <<= 1;
        }
        if (filled < n) {
            System.arraycopy(out, 0, out, filled, n - filled);
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
