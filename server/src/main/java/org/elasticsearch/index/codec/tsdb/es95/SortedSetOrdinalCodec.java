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
 * Per-block ordinal codec for SORTED_SET doc values (multiple values per document).
 * Mirrors {@link SortedOrdinalCodec} but runs {@link BlockStats#recomputeWithCycle}
 * to enable {@link CycleCodec} as a sixth candidate.
 *
 * <p>The cycle case is the multi-valued pattern where every doc in a tsid run emits
 * the same K-ord set in the same order (e.g. {@code host.ip}, {@code host.mac}). The
 * flat ord stream then repeats with period K; without {@link CycleCodec} the block
 * falls through to {@link BitPackedCodec} and pays the full bitsPerOrd per value
 * because the K cycle values are typically scattered across the term-dictionary bit
 * range. SORTED blocks never see this pattern, which is why
 * {@link SortedOrdinalCodec} skips cycle detection entirely.
 *
 * <p>Wire format is byte-for-byte compatible with {@link SortedOrdinalCodec} for
 * encodings 0, 1, and 2, and shares the encoding-3 ADAPTIVE_EXTRA dispatch with
 * sub-modes 0 ({@link RleCodec}), 1 ({@link BitpackCodec}), and additionally 2
 * ({@link CycleCodec}).
 *
 * <p>The per-mode codec instances are supplied via constructor injection and held
 * as {@code final} fields. A convenience constructor delegates to the full one
 * using the package's singletons.
 */
public final class SortedSetOrdinalCodec {

    private final ConstantCodec constantCodec;
    private final TwoRunCodec twoRunCodec;
    private final BitPackedCodec bitPackedCodec;
    private final RleCodec rleCodec;
    private final BitpackCodec bitpackCodec;
    private final CycleCodec cycleCodec;
    private final CodecContext ctx;
    private final BlockStats stats;

    public SortedSetOrdinalCodec(int blockSize) {
        this(
            blockSize,
            ConstantCodec.INSTANCE,
            TwoRunCodec.INSTANCE,
            BitPackedCodec.INSTANCE,
            RleCodec.INSTANCE,
            BitpackCodec.INSTANCE,
            CycleCodec.INSTANCE
        );
    }

    SortedSetOrdinalCodec(
        int blockSize,
        final ConstantCodec constantCodec,
        final TwoRunCodec twoRunCodec,
        final BitPackedCodec bitPackedCodec,
        final RleCodec rleCodec,
        final BitpackCodec bitpackCodec,
        final CycleCodec cycleCodec
    ) {
        this.constantCodec = constantCodec;
        this.twoRunCodec = twoRunCodec;
        this.bitPackedCodec = bitPackedCodec;
        this.rleCodec = rleCodec;
        this.bitpackCodec = bitpackCodec;
        this.cycleCodec = cycleCodec;
        this.ctx = new CodecContext(blockSize);
        this.stats = new BlockStats();
    }

    public void encodeOrdinals(final long[] in, final DataOutput out, int bitsPerOrd) throws IOException {
        stats.recomputeWithCycle(in);

        BlockModeCodec winner = bitPackedCodec;
        long winnerSize = bitPackedCodec.estimateSize(in, stats, bitsPerOrd);

        long constSize = constantCodec.estimateSize(in, stats, bitsPerOrd);
        if (constSize < winnerSize) {
            winner = constantCodec;
            winnerSize = constSize;
        }

        long twoRunSize = twoRunCodec.estimateSize(in, stats, bitsPerOrd);
        if (twoRunSize < winnerSize) {
            winner = twoRunCodec;
            winnerSize = twoRunSize;
        }

        long rleSize = rleCodec.estimateSize(in, stats, bitsPerOrd);
        if (rleSize < winnerSize) {
            winner = rleCodec;
            winnerSize = rleSize;
        }

        long bitpackSize = bitpackCodec.estimateSize(in, stats, bitsPerOrd);
        if (bitpackSize < winnerSize) {
            winner = bitpackCodec;
            winnerSize = bitpackSize;
        }

        long cycleSize = cycleCodec.estimateSize(in, stats, bitsPerOrd);
        if (cycleSize < winnerSize) {
            winner = cycleCodec;
        }

        winner.encodePayload(in, stats, ctx, out, bitsPerOrd);
    }

    public void decodeOrdinals(final DataInput in, final long[] out, int bitsPerOrd) throws IOException {
        long v1 = in.readVLong();
        int encoding = Long.numberOfTrailingZeros(~v1);
        if (encoding == ConstantCodec.ENCODING) {
            constantCodec.decodePayload(ctx, in, out, bitsPerOrd, v1);
        } else if (encoding == TwoRunCodec.ENCODING) {
            twoRunCodec.decodePayload(ctx, in, out, bitsPerOrd, v1);
        } else if (encoding == BitPackedCodec.ENCODING) {
            bitPackedCodec.decodePayload(ctx, in, out, bitsPerOrd, v1);
        } else if (encoding == ADAPTIVE_EXTRA_ENCODING) {
            byte subMode = in.readByte();
            if (subMode == RleCodec.SUB_MODE) {
                rleCodec.decodePayload(ctx, in, out, bitsPerOrd, v1);
            } else if (subMode == BitpackCodec.SUB_MODE) {
                bitpackCodec.decodePayload(ctx, in, out, bitsPerOrd, v1);
            } else if (subMode == CycleCodec.SUB_MODE) {
                cycleCodec.decodePayload(ctx, in, out, bitsPerOrd, v1);
            } else {
                throw new CorruptIndexException(String.format(Locale.ROOT, "unknown ADAPTIVE_EXTRA sub-mode 0x%02x", subMode & 0xff), in);
            }
        } else {
            throw new CorruptIndexException(String.format(Locale.ROOT, "unknown sorted-set ordinal encoding %d", encoding), in);
        }
    }

    /** Trailing-one-bits count for the ADAPTIVE_EXTRA dispatch (shared by {@link RleCodec}, {@link BitpackCodec}, {@link CycleCodec}). */
    static final int ADAPTIVE_EXTRA_ENCODING = 3;
}
