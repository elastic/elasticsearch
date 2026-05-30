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
 * Cycle-encoded block codec (encoding 3, sub-mode {@link #SUB_MODE}). Applies when the
 * block's flat ord stream repeats with a period {@code p} in
 * {@code [2, blockSize / MAX_CYCLE_DIVISOR]} and the pattern survives both passes of
 * {@link BlockStats#recomputeWithCycle}.
 *
 * <p>Targets the multi-valued SORTED_SET case where every doc in a tsid run emits the
 * same K-ord set in the same order, so the flat ord stream looks like
 * {@code [v0, v1, ..., v(K-1), v0, v1, ...]} regardless of how the K values sort in the
 * term dictionary. {@link BitPackedCodec} and {@link BitpackCodec} would otherwise pay
 * the full bitsPerOrd per value because the K cycle values are typically scattered
 * across the term-dictionary bit range.
 *
 * <p>Payload after the encoding-3 header and sub-mode byte is
 * {@code [period:vint][value_0:vlong, value_1:vlong, ..., value_(period - 1):vlong]}.
 * The decoder reads the period and the period distinct values into a scratch slice and
 * fills {@code out[i] = cycleValues[i % period]} for the whole block. Stateless; access
 * via {@link #INSTANCE}.
 */
final class CycleCodec implements BlockModeCodec {

    static final int ENCODING = 3;
    static final byte SUB_MODE = 2;
    static final CycleCodec INSTANCE = new CycleCodec();

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
        // NOTE: 1-byte header (vlong 0b111) + 1-byte sub-mode + period (vint) + period values
        long size = 1L + 1L + vIntSize(period);
        for (int i = 0; i < period; i++) {
            size += vLongSize(in[i]);
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
        for (int i = 0; i < period; i++) {
            out.writeVLong(in[i]);
        }
    }

    @Override
    public void decodePayload(final CodecContext ctx, final DataInput in, final long[] out, int bitsPerOrd, long leadingVLong)
        throws IOException {
        final int period = in.readVInt();
        if (period < 2 || period > out.length) {
            throw new CorruptIndexException(String.format(Locale.ROOT, "invalid cycle period %d", period), in);
        }
        final long[] cycle = ctx.scratch;
        for (int i = 0; i < period; i++) {
            cycle[i] = in.readVLong();
        }
        for (int i = 0; i < out.length; i++) {
            out[i] = cycle[i % period];
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
