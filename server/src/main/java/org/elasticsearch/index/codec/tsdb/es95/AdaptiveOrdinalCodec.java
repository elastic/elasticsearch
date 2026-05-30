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
 * Adaptive per-block ordinal codec. For each 128-value block, runs a single
 * statistical pass and picks the cheapest of five per-mode codecs by exact
 * byte count. The encoder writes the codec's full payload, including the
 * vlong header whose trailing one-bits count selects the encoding.
 *
 * <p>Encodings 0, 1, and 2 ({@link ConstantCodec}, {@link TwoRunCodec},
 * {@link BitPackedCodec}) are byte-for-byte identical to the legacy
 * {@code TSDBDocValuesEncoder.encodeOrdinals} format. Encoding 3
 * ({@code ADAPTIVE_EXTRA}) carries a one-byte sub-mode selector and
 * dispatches between {@link RleCodec} (RLE_N, sub-mode 0) and
 * {@link BitpackCodec} (BITPACK_LOCAL, sub-mode 1).
 *
 * <p>The per-mode codec instances are supplied via constructor injection and
 * held as {@code final} fields. A convenience constructor delegates to the
 * full one using the package's singletons.
 */
public final class AdaptiveOrdinalCodec {

    private final ConstantCodec constantCodec;
    private final TwoRunCodec twoRunCodec;
    private final BitPackedCodec bitPackedCodec;
    private final RleCodec rleCodec;
    private final BitpackCodec bitpackCodec;
    private final CodecContext ctx;
    private final BlockStats stats;

    public AdaptiveOrdinalCodec(int blockSize) {
        this(blockSize, ConstantCodec.INSTANCE, TwoRunCodec.INSTANCE, BitPackedCodec.INSTANCE, RleCodec.INSTANCE, BitpackCodec.INSTANCE);
    }

    AdaptiveOrdinalCodec(
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
