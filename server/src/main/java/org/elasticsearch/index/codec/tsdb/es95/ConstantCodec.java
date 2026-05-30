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

import java.io.IOException;
import java.util.Arrays;

/**
 * Single-value block codec (encoding 0). Writes {@code value << 1} as a
 * single vlong with trailing zero indicating CONST; the decoder shifts the
 * value back and fills the destination. Byte-for-byte identical to the
 * legacy single-run encoding so the common mid-tsid constant case carries
 * zero overhead. Stateless; access via {@link #INSTANCE}.
 */
final class ConstantCodec implements BlockModeCodec {

    static final int ENCODING = 0;
    static final ConstantCodec INSTANCE = new ConstantCodec();

    private ConstantCodec() {}

    @Override
    public int encoding() {
        return ENCODING;
    }

    @Override
    public long estimateSize(final long[] in, final BlockStats stats, int bitsPerOrd) {
        if (stats.allSame == false || bitsPerOrd >= 63) {
            return Long.MAX_VALUE;
        }
        return vLongSize(in[0] << 1);
    }

    @Override
    public void encodePayload(final long[] in, final BlockStats stats, final CodecContext ctx, final DataOutput out, int bitsPerOrd)
        throws IOException {
        out.writeVLong(in[0] << 1);
    }

    @Override
    public void decodePayload(final CodecContext ctx, final DataInput in, final long[] out, int bitsPerOrd, long leadingVLong) {
        Arrays.fill(out, leadingVLong >>> 1);
    }

    private static int vLongSize(long value) {
        int bytes = 1;
        long unsigned = value;
        while ((unsigned & ~0x7FL) != 0) {
            bytes++;
            unsigned >>>= 7;
        }
        return bytes;
    }
}
