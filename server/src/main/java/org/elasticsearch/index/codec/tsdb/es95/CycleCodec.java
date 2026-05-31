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
 * Block codec for ord streams that repeat with a period in
 * {@code [2, blockSize / MAX_CYCLE_DIVISOR]}. Targets the multi-valued SORTED_SET mid-tsid
 * shape where every doc in a {@code _tsid} run emits the same K-ord tuple
 * (e.g. {@code host.ip}, {@code host.mac}).
 *
 * <p>Wire format: the leading vlong packs the period in bits 5 and above with low 5 bits
 * fixed at {@code 01111}. The 4 trailing one-bits identify the encoding; the bit above
 * them terminates the trailing-ones run. The header is followed by K vlongs: the first
 * absolute, the remaining K-1 delta-encoded.
 * <pre>
 *   vlong header   (period shifted left 5 bits, low 5 bits are 01111)
 *   vlong first_ord
 *   vlong delta_1, ..., delta_(period - 1)     # delta_i = ord_i - ord_(i-1) - 1
 * </pre>
 *
 * <p>Stateless; access via {@link #INSTANCE}.
 */
public final class CycleCodec implements BlockModeCodec {

    /** Trailing-one-bits count selecting the CYCLE_COMPACT encoding. */
    public static final int ENCODING = 4;

    /** Low 5 bits of the header vlong: 4 trailing ones then a 0 terminator. */
    public static final long ENCODING_MARKER = 0b01111L;

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
        long size = vLongSize(((long) period << 5) | ENCODING_MARKER);
        size += vLongSize(in[0]);
        for (int k = 1; k < period; k++) {
            size += vLongSize(in[k] - in[k - 1] - 1L);
        }
        return size;
    }

    @Override
    public void encodePayload(final long[] in, final BlockStats stats, final CodecContext ctx, final DataOutput out, int bitsPerOrd)
        throws IOException {
        final int period = stats.cycleLength;
        out.writeVLong(((long) period << 5) | ENCODING_MARKER);
        out.writeVLong(in[0]);
        for (int k = 1; k < period; k++) {
            out.writeVLong(in[k] - in[k - 1] - 1L);
        }
    }

    @Override
    public void decodePayload(final CodecContext ctx, final DataInput in, final long[] out, int bitsPerOrd, long leadingVLong)
        throws IOException {
        final int period = (int) (leadingVLong >>> 5);
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

    // NOTE: small-K specializations make period a compile-time constant for the JIT, which
    // unrolls and vectorizes the inner loop. The generic `out[i] = tuple[i % period]` form
    // blocks vectorization because the modulo depends on a runtime value.
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
            if (i < n) {
                out[i++] = t0;
            }
            if (i < n) {
                out[i] = t1;
            }
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
        // NOTE: System.arraycopy on long[] is intrinsified to SIMD by the JVM. Doubling the
        // already-filled prefix amortizes the copy over log2(blockSize / period) iterations.
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
