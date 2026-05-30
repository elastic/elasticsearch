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
 */
final class AdaptiveOrdinalCodec {

    private final LegacyCodec legacyCodec;
    private final BitpackCodec bitpackCodec;
    private final BlockStats stats;

    AdaptiveOrdinalCodec(int blockSize) {
        this.legacyCodec = new LegacyCodec(blockSize);
        this.bitpackCodec = new BitpackCodec(blockSize);
        this.stats = new BlockStats();
    }

    void encodeOrdinals(final long[] in, final DataOutput out, int bitsPerOrd) throws IOException {
        stats.recompute(in);

        BlockModeCodec winner = legacyCodec;
        long winnerSize = sizeWithHeader(legacyCodec.estimateSize(in, stats, bitsPerOrd));

        long constSize = sizeWithHeader(ConstantCodec.INSTANCE.estimateSize(in, stats, bitsPerOrd));
        if (constSize < winnerSize) {
            winner = ConstantCodec.INSTANCE;
            winnerSize = constSize;
        }

        long rleSize = sizeWithHeader(RleCodec.INSTANCE.estimateSize(in, stats, bitsPerOrd));
        if (rleSize < winnerSize) {
            winner = RleCodec.INSTANCE;
            winnerSize = rleSize;
        }

        long bitpackSize = sizeWithHeader(bitpackCodec.estimateSize(in, stats, bitsPerOrd));
        if (bitpackSize < winnerSize) {
            winner = bitpackCodec;
        }

        out.writeByte(winner.mode());
        winner.encodePayload(in, stats, out, bitsPerOrd);
    }

    void decodeOrdinals(final DataInput in, final long[] out, int bitsPerOrd) throws IOException {
        byte mode = in.readByte();
        switch (mode) {
            case LegacyCodec.MODE:
                legacyCodec.decodePayload(in, out, bitsPerOrd);
                return;
            case ConstantCodec.MODE:
                ConstantCodec.INSTANCE.decodePayload(in, out, bitsPerOrd);
                return;
            case RleCodec.MODE:
                RleCodec.INSTANCE.decodePayload(in, out, bitsPerOrd);
                return;
            case BitpackCodec.MODE:
                bitpackCodec.decodePayload(in, out, bitsPerOrd);
                return;
            default:
                throw new CorruptIndexException(String.format(Locale.ROOT, "unknown adaptive ordinal block mode 0x%02x", mode & 0xff), in);
        }
    }

    private static long sizeWithHeader(long payloadSize) {
        return payloadSize == Long.MAX_VALUE ? Long.MAX_VALUE : 1L + payloadSize;
    }
}
