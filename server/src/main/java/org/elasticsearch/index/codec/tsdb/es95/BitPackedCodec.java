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

/**
 * Full-block bit-packed codec (encoding 2). Writes the constant header byte {@code 0b11},
 * then bit-packs the entire block at the segment-global {@code bitsPerOrd}. Stateless;
 * access via {@link #INSTANCE}.
 */
final class BitPackedCodec implements BlockModeCodec {

    static final int ENCODING = 2;
    static final BitPackedCodec INSTANCE = new BitPackedCodec();

    private BitPackedCodec() {}

    @Override
    public int encoding() {
        return ENCODING;
    }

    @Override
    public long estimateSize(final long[] in, final BlockStats stats, int bitsPerOrd) {
        int roundedBits = DocValuesForUtil.roundBits(bitsPerOrd);
        return 1L + ((long) in.length * roundedBits + 7) / 8;
    }

    @Override
    public void encodePayload(final long[] in, final BlockStats stats, final CodecContext ctx, final DataOutput out, int bitsPerOrd)
        throws IOException {
        out.writeVLong(0b11);
        ctx.forUtil.encode(in, bitsPerOrd, out);
    }

    @Override
    public void decodePayload(final CodecContext ctx, final DataInput in, final long[] out, int bitsPerOrd, long leadingVLong)
        throws IOException {
        ctx.forUtil.decode(bitsPerOrd, in, out);
    }
}
