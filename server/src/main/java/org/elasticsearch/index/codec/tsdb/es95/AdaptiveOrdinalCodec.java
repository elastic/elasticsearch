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
 * statistical pass and picks the cheapest of four per-mode codecs:
 * {@link LegacyCodec}, {@link ConstantCodec}, {@link RleCodec},
 * {@link BitpackCodec}. The selected mode is written as a one-byte header
 * followed by the codec's payload.
 *
 * <p>Wire format per block: {@code [mode:1][payload]}. Mode byte values are
 * defined as {@code static final byte MODE} on each codec class.
 *
 * <p>The four per-mode codec instances are supplied via constructor
 * injection and held as {@code final} fields. A convenience constructor
 * delegates to the full one using the package's singletons.
 */
public final class AdaptiveOrdinalCodec {

    private final LegacyCodec legacyCodec;
    private final ConstantCodec constantCodec;
    private final RleCodec rleCodec;
    private final BitpackCodec bitpackCodec;
    private final CodecContext ctx;
    private final BlockStats stats;

    public AdaptiveOrdinalCodec(int blockSize) {
        this(blockSize, LegacyCodec.INSTANCE, ConstantCodec.INSTANCE, RleCodec.INSTANCE, BitpackCodec.INSTANCE);
    }

    AdaptiveOrdinalCodec(
        int blockSize,
        final LegacyCodec legacyCodec,
        final ConstantCodec constantCodec,
        final RleCodec rleCodec,
        final BitpackCodec bitpackCodec
    ) {
        this.legacyCodec = legacyCodec;
        this.constantCodec = constantCodec;
        this.rleCodec = rleCodec;
        this.bitpackCodec = bitpackCodec;
        this.ctx = new CodecContext(blockSize);
        this.stats = new BlockStats();
    }

    public void encodeOrdinals(final long[] in, final DataOutput out, int bitsPerOrd) throws IOException {
        stats.recompute(in);

        BlockModeCodec winner = legacyCodec;
        long winnerSize = sizeWithHeader(legacyCodec.estimateSize(in, stats, bitsPerOrd));

        long constSize = sizeWithHeader(constantCodec.estimateSize(in, stats, bitsPerOrd));
        if (constSize < winnerSize) {
            winner = constantCodec;
            winnerSize = constSize;
        }

        long rleSize = sizeWithHeader(rleCodec.estimateSize(in, stats, bitsPerOrd));
        if (rleSize < winnerSize) {
            winner = rleCodec;
            winnerSize = rleSize;
        }

        long bitpackSize = sizeWithHeader(bitpackCodec.estimateSize(in, stats, bitsPerOrd));
        if (bitpackSize < winnerSize) {
            winner = bitpackCodec;
        }

        out.writeByte(winner.mode());
        winner.encodePayload(in, stats, ctx, out, bitsPerOrd);
    }

    public void decodeOrdinals(final DataInput in, final long[] out, int bitsPerOrd) throws IOException {
        byte mode = in.readByte();
        switch (mode) {
            case LegacyCodec.MODE:
                legacyCodec.decodePayload(ctx, in, out, bitsPerOrd);
                return;
            case ConstantCodec.MODE:
                constantCodec.decodePayload(ctx, in, out, bitsPerOrd);
                return;
            case RleCodec.MODE:
                rleCodec.decodePayload(ctx, in, out, bitsPerOrd);
                return;
            case BitpackCodec.MODE:
                bitpackCodec.decodePayload(ctx, in, out, bitsPerOrd);
                return;
            default:
                throw new CorruptIndexException(String.format(Locale.ROOT, "unknown adaptive ordinal block mode 0x%02x", mode & 0xff), in);
        }
    }

    private static long sizeWithHeader(long payloadSize) {
        return payloadSize == Long.MAX_VALUE ? Long.MAX_VALUE : 1L + payloadSize;
    }
}
