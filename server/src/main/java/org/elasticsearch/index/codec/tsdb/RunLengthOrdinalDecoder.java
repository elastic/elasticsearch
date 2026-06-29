/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.store.DataInput;

import java.io.IOException;

/**
 * Decodes a block written by {@link RunLengthOrdinalEncoder}, dispatching to the
 * {@link OrdinalBlockStrategy} named by the header's trailing one bits.
 * <p>
 * This is injected into the ES95 ordinal reader for segments at the run-length format version or later;
 * older ES95 segments are decoded with the legacy four-strategy decoder on {@link TSDBDocValuesEncoder}.
 */
public final class RunLengthOrdinalDecoder {

    private final DocValuesForUtil forUtil;
    private final int blockSize;

    public RunLengthOrdinalDecoder(int blockSize) {
        this.forUtil = new DocValuesForUtil(blockSize);
        this.blockSize = blockSize;
    }

    /**
     * Decodes a block written by {@link RunLengthOrdinalEncoder#encode}.
     *
     * @param in         the input positioned at the start of the block
     * @param out        the array to fill with decoded ordinals
     * @param bitsPerOrd bits required to represent the largest ordinal in the block
     * @throws IOException if reading from {@code in} fails
     */
    public void decode(DataInput in, long[] out, int bitsPerOrd) throws IOException {
        assert out.length == blockSize : out.length;
        final long header = in.readVLong();
        final int encoding = Long.numberOfTrailingZeros(~header);
        final long payload = header >>> (encoding + 1);
        // NOTE: switch on the concrete strategy instead of a shared call: with five implementations a
        // single virtual call site is megamorphic, while each switch arm is monomorphic and inlinable.
        switch (encoding) {
            case 0 -> OrdinalBlockStrategy.SingleRun.INSTANCE.decode(payload, bitsPerOrd, in, out, forUtil);
            case 1 -> OrdinalBlockStrategy.RunLength.INSTANCE.decode(payload, bitsPerOrd, in, out, forUtil);
            case 2 -> OrdinalBlockStrategy.Cyclic.INSTANCE.decode(payload, bitsPerOrd, in, out, forUtil);
            case 3 -> OrdinalBlockStrategy.TwoRun.INSTANCE.decode(payload, bitsPerOrd, in, out, forUtil);
            case 4 -> OrdinalBlockStrategy.BitPacked.INSTANCE.decode(payload, bitsPerOrd, in, out, forUtil);
            default -> throw new IllegalStateException("unexpected ordinal encoding " + encoding);
        }
    }
}
