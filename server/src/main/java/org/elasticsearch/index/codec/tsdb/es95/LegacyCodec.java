/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.elasticsearch.index.codec.tsdb.DocValuesForUtil;

import java.io.IOException;

/** Fallback codec: bit-pack the entire block at the segment-global {@code bitsPerOrd}. */
final class LegacyCodec implements BlockModeCodec {

    static final byte MODE = 0;

    private final DocValuesForUtil forUtil;

    LegacyCodec(int blockSize) {
        this.forUtil = new DocValuesForUtil(blockSize);
    }

    @Override
    public byte mode() {
        return MODE;
    }

    @Override
    public long estimateSize(final long[] in, final BlockStats stats, int bitsPerOrd) {
        int roundedBits = DocValuesForUtil.roundBits(bitsPerOrd);
        return ((long) in.length * roundedBits + 7) / 8;
    }

    @Override
    public void encodePayload(final long[] in, final BlockStats stats, final DataOutput out, int bitsPerOrd) throws IOException {
        forUtil.encode(in, bitsPerOrd, out);
    }

    @Override
    public void decodePayload(final DataInput in, final long[] out, int bitsPerOrd) throws IOException {
        forUtil.decode(bitsPerOrd, in, out);
    }
}
