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
 * Per-block ordinal codec for SORTED doc values (single value per document). For each
 * 128-value block, runs a single statistical pass and picks the cheapest of five
 * candidates by exact byte count: {@link ConstantCodec} (encoding 0),
 * {@link TwoRunCodec} (encoding 1), {@link BitPackedCodec} (encoding 2), and the
 * ADAPTIVE_EXTRA family (encoding 3) of {@link RleCodec} (sub-mode 0) and
 * {@link BitpackCodec} (sub-mode 1). The encoder writes the codec's full payload,
 * including the vlong header whose trailing one-bits count selects the encoding.
 *
 * <p>The per-mode codec instances are supplied via constructor injection and held as
 * {@code final} fields. A convenience constructor delegates to the full one using the
 * package's singletons.
 */
public final class SortedOrdinalCodec {

    private final ConstantCodec constantCodec;
    private final TwoRunCodec twoRunCodec;
    private final BitPackedCodec bitPackedCodec;
    private final RleCodec rleCodec;
    private final BitpackCodec bitpackCodec;
    private final CodecContext ctx;
    private final BlockStats stats;

    public SortedOrdinalCodec(int blockSize) {
        this(blockSize, ConstantCodec.INSTANCE, TwoRunCodec.INSTANCE, BitPackedCodec.INSTANCE, RleCodec.INSTANCE, BitpackCodec.INSTANCE);
    }

    SortedOrdinalCodec(
        int blockSize,
        final ConstantCodec constantCodec,
        final TwoRunCodec twoRunCodec,
        final BitPackedCodec bitPackedCodec,
        final RleCodec rleCodec,
        final BitpackCodec bitpackCodec
    ) {
        this.constantCodec = constantCodec;
        this.twoRunCodec = twoRunCodec;
        this.bitPackedCodec = bitPackedCodec;
        this.rleCodec = rleCodec;
        this.bitpackCodec = bitpackCodec;
        this.ctx = new CodecContext(blockSize);
        this.stats = new BlockStats();
    }

    public void encodeOrdinals(final long[] in, final DataOutput out, int bitsPerOrd) throws IOException {
        stats.recompute(in);
        // NOTE: CONST and TWO_RUN are exact winners on their respective shapes; the
        // short-circuits skip the per-candidate estimate calls on the dominant blocks.
        if (stats.allSame) {
            constantCodec.encodePayload(in, stats, ctx, out, bitsPerOrd);
            return;
        }
        if (stats.nRuns == 2) {
            twoRunCodec.encodePayload(in, stats, ctx, out, bitsPerOrd);
            return;
        }

        final long bitPackedSize = bitPackedCodec.estimateSize(in, stats, bitsPerOrd);
        final long rleSize = rleCodec.estimateSize(in, stats, bitsPerOrd);
        final long bitpackSize = bitpackCodec.estimateSize(in, stats, bitsPerOrd);

        // NOTE: tracking the winner by integer index plus a switch on typed fields keeps
        // each dispatch site monomorphic. A BlockModeCodec interface variable here would
        // see 3+ receiver types and go megamorphic, blocking JIT inlining.
        int winner = WINNER_BIT_PACKED;
        long winnerSize = bitPackedSize;
        if (rleSize < winnerSize) {
            winner = WINNER_RLE;
            winnerSize = rleSize;
        }
        if (bitpackSize < winnerSize) {
            winner = WINNER_BITPACK_LOCAL;
        }

        switch (winner) {
            case WINNER_BIT_PACKED -> bitPackedCodec.encodePayload(in, stats, ctx, out, bitsPerOrd);
            case WINNER_RLE -> rleCodec.encodePayload(in, stats, ctx, out, bitsPerOrd);
            case WINNER_BITPACK_LOCAL -> bitpackCodec.encodePayload(in, stats, ctx, out, bitsPerOrd);
            default -> throw new AssertionError("unexpected winner: " + winner);
        }
    }

    private static final int WINNER_BIT_PACKED = 0;
    private static final int WINNER_RLE = 1;
    private static final int WINNER_BITPACK_LOCAL = 2;

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
            } else {
                throw new CorruptIndexException(String.format(Locale.ROOT, "unknown ADAPTIVE_EXTRA sub-mode 0x%02x", subMode & 0xff), in);
            }
        } else {
            throw new CorruptIndexException(String.format(Locale.ROOT, "unknown adaptive ordinal encoding %d", encoding), in);
        }
    }

    /** Trailing-one-bits count for the ADAPTIVE_EXTRA dispatch (shared by {@link RleCodec} and {@link BitpackCodec}). */
    static final int ADAPTIVE_EXTRA_ENCODING = 3;
}
