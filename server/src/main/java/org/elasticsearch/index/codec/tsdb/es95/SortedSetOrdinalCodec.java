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
 * Mirrors {@link SortedOrdinalCodec} for the scalar candidates and adds
 * {@link TupleRunCodec} as the multi-valued candidate. The encoder takes the per-doc
 * value counts and head/tail straddle offsets so {@code TupleRunCodec} can group
 * consecutive docs that emit the same K-ord tuple into a single (K, runLen, tuple)
 * entry. Targets the K-cycle pattern produced by multi-valued docs sharing the same
 * ord set within a {@code _tsid} run (e.g. {@code host.ip}, {@code host.mac}).
 *
 * <p>Wire format is byte for byte compatible with {@link SortedOrdinalCodec} for
 * encodings 0, 1, and 2; the ADAPTIVE_EXTRA encoding (3) carries sub-modes
 * {@link RleCodec#SUB_MODE} (0), {@link BitpackCodec#SUB_MODE} (1), and
 * {@link TupleRunCodec#SUB_MODE} (2). SORTED blocks never see the multi-valued K-tuple
 * pattern, which is why {@link SortedOrdinalCodec} skips this candidate entirely.
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
    private final TupleRunCodec tupleRunCodec;
    private final CodecContext ctx;
    private final BlockStats stats;
    private final TupleRunCodec.RunBuilder tupleRunStats;

    public SortedSetOrdinalCodec(int blockSize) {
        this(
            blockSize,
            ConstantCodec.INSTANCE,
            TwoRunCodec.INSTANCE,
            BitPackedCodec.INSTANCE,
            RleCodec.INSTANCE,
            BitpackCodec.INSTANCE,
            CycleCodec.INSTANCE,
            TupleRunCodec.INSTANCE
        );
    }

    SortedSetOrdinalCodec(
        int blockSize,
        final ConstantCodec constantCodec,
        final TwoRunCodec twoRunCodec,
        final BitPackedCodec bitPackedCodec,
        final RleCodec rleCodec,
        final BitpackCodec bitpackCodec,
        final CycleCodec cycleCodec,
        final TupleRunCodec tupleRunCodec
    ) {
        this.constantCodec = constantCodec;
        this.twoRunCodec = twoRunCodec;
        this.bitPackedCodec = bitPackedCodec;
        this.rleCodec = rleCodec;
        this.bitpackCodec = bitpackCodec;
        this.cycleCodec = cycleCodec;
        this.tupleRunCodec = tupleRunCodec;
        this.ctx = new CodecContext(blockSize);
        this.stats = new BlockStats();
        this.tupleRunStats = new TupleRunCodec.RunBuilder(blockSize + 1);
    }

    public void encodeOrdinals(
        final long[] in,
        final int[] perDocK,
        int numDocs,
        int headOffset,
        int tailMissing,
        final DataOutput out,
        int bitsPerOrd
    ) throws IOException {
        stats.recomputeWithCycle(in);

        BlockModeCodec winner = bitPackedCodec;
        long winnerSize = bitPackedCodec.estimateSize(in, stats, bitsPerOrd);

        final long constSize = constantCodec.estimateSize(in, stats, bitsPerOrd);
        if (constSize < winnerSize) {
            winner = constantCodec;
            winnerSize = constSize;
        }

        final long twoRunSize = twoRunCodec.estimateSize(in, stats, bitsPerOrd);
        if (twoRunSize < winnerSize) {
            winner = twoRunCodec;
            winnerSize = twoRunSize;
        }

        final long rleSize = rleCodec.estimateSize(in, stats, bitsPerOrd);
        if (rleSize < winnerSize) {
            winner = rleCodec;
            winnerSize = rleSize;
        }

        final long bitpackSize = bitpackCodec.estimateSize(in, stats, bitsPerOrd);
        if (bitpackSize < winnerSize) {
            winner = bitpackCodec;
            winnerSize = bitpackSize;
        }

        final long cycleSize = cycleCodec.estimateSize(in, stats, bitsPerOrd);
        if (cycleSize < winnerSize) {
            winner = cycleCodec;
            winnerSize = cycleSize;
        }

        tupleRunCodec.buildRuns(in, perDocK, numDocs, headOffset, tailMissing, tupleRunStats);
        final long tupleRunSize = tupleRunCodec.estimateSize(tupleRunStats, headOffset, tailMissing);
        if (tupleRunSize < winnerSize) {
            tupleRunCodec.encodePayload(tupleRunStats, headOffset, tailMissing, out);
            return;
        }

        winner.encodePayload(in, stats, ctx, out, bitsPerOrd);
    }

    public void decodeOrdinals(final DataInput in, final long[] out, int bitsPerOrd) throws IOException {
        final long v1 = in.readVLong();
        final int encoding = Long.numberOfTrailingZeros(~v1);
        if (encoding == ConstantCodec.ENCODING) {
            constantCodec.decodePayload(ctx, in, out, bitsPerOrd, v1);
        } else if (encoding == TwoRunCodec.ENCODING) {
            twoRunCodec.decodePayload(ctx, in, out, bitsPerOrd, v1);
        } else if (encoding == BitPackedCodec.ENCODING) {
            bitPackedCodec.decodePayload(ctx, in, out, bitsPerOrd, v1);
        } else if (encoding == ADAPTIVE_EXTRA_ENCODING) {
            final byte subMode = in.readByte();
            if (subMode == RleCodec.SUB_MODE) {
                rleCodec.decodePayload(ctx, in, out, bitsPerOrd, v1);
            } else if (subMode == BitpackCodec.SUB_MODE) {
                bitpackCodec.decodePayload(ctx, in, out, bitsPerOrd, v1);
            } else if (subMode == TupleRunCodec.SUB_MODE) {
                tupleRunCodec.decodePayload(in, out);
            } else if (subMode == CycleCodec.SUB_MODE) {
                cycleCodec.decodePayload(ctx, in, out, bitsPerOrd, v1);
            } else {
                throw new CorruptIndexException(String.format(Locale.ROOT, "unknown ADAPTIVE_EXTRA sub-mode 0x%02x", subMode & 0xff), in);
            }
        } else {
            throw new CorruptIndexException(String.format(Locale.ROOT, "unknown sorted-set ordinal encoding %d", encoding), in);
        }
    }

    /** Trailing-one-bits count for the ADAPTIVE_EXTRA dispatch (shared by {@link RleCodec}, {@link BitpackCodec}, {@link TupleRunCodec}, {@link CycleCodec}). */
    static final int ADAPTIVE_EXTRA_ENCODING = 3;
}
