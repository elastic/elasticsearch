/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.store.DataOutput;

import java.io.IOException;

/**
 * Encodes a block of ES95 ordinals with the smallest {@link OrdinalBlockStrategy} chosen by exact byte
 * size, writing a header whose trailing one bits tag the chosen strategy, which
 * {@link RunLengthOrdinalDecoder} reads back.
 * <p>
 * This is injected into the ES95 ordinal writer in place of the legacy four-strategy encoding on
 * {@link TSDBDocValuesEncoder}, which stays the default for ES87 and ES819.
 */
public final class RunLengthOrdinalEncoder {

    private final DocValuesForUtil forUtil;
    private final int blockSize;

    public RunLengthOrdinalEncoder(int blockSize) {
        this.forUtil = new DocValuesForUtil(blockSize);
        this.blockSize = blockSize;
    }

    /**
     * Encodes a block of ordinals with the smallest applicable strategy.
     *
     * @param in         the block of ordinals to encode
     * @param out        the output to write the encoded block to
     * @param bitsPerOrd bits required to represent the largest ordinal in the block
     * @throws IOException if writing to {@code out} fails
     */
    public void encode(long[] in, DataOutput out, int bitsPerOrd) throws IOException {
        assert in.length == blockSize;
        final OrdinalBlockStrategy.Stats stats = OrdinalBlockStrategy.Stats.of(in);
        final long singleRun = OrdinalBlockStrategy.SingleRun.INSTANCE.encodedSize(stats, bitsPerOrd, blockSize);
        final long runLength = OrdinalBlockStrategy.RunLength.INSTANCE.encodedSize(stats, bitsPerOrd, blockSize);
        final long cyclic = OrdinalBlockStrategy.Cyclic.INSTANCE.encodedSize(stats, bitsPerOrd, blockSize);
        final long twoRun = OrdinalBlockStrategy.TwoRun.INSTANCE.encodedSize(stats, bitsPerOrd, blockSize);
        final long bitPacked = OrdinalBlockStrategy.BitPacked.INSTANCE.encodedSize(stats, bitsPerOrd, blockSize);

        int best = 0;
        long bestBytes = singleRun;
        if (runLength < bestBytes) {
            best = 1;
            bestBytes = runLength;
        }
        if (cyclic < bestBytes) {
            best = 2;
            bestBytes = cyclic;
        }
        if (twoRun < bestBytes) {
            best = 3;
            bestBytes = twoRun;
        }
        if (bitPacked < bestBytes) {
            best = 4;
        }

        // NOTE: switch on the concrete strategy instead of a shared call: with five implementations a
        // single virtual call site is megamorphic, while each switch arm is monomorphic and inlinable.
        switch (best) {
            case 0 -> OrdinalBlockStrategy.SingleRun.INSTANCE.encode(in, stats, bitsPerOrd, forUtil, out);
            case 1 -> OrdinalBlockStrategy.RunLength.INSTANCE.encode(in, stats, bitsPerOrd, forUtil, out);
            case 2 -> OrdinalBlockStrategy.Cyclic.INSTANCE.encode(in, stats, bitsPerOrd, forUtil, out);
            case 3 -> OrdinalBlockStrategy.TwoRun.INSTANCE.encode(in, stats, bitsPerOrd, forUtil, out);
            case 4 -> OrdinalBlockStrategy.BitPacked.INSTANCE.encode(in, stats, bitsPerOrd, forUtil, out);
            default -> throw new AssertionError(best);
        }
    }
}
