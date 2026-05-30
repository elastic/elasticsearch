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

/** Single-value block: writes one vlong, fills the decoded array uniformly. */
final class ConstantCodec implements BlockModeCodec {

    static final byte MODE = 1;

    @Override
    public byte mode() {
        return MODE;
    }

    @Override
    public long estimateSize(final long[] in, final BlockStats stats, int bitsPerOrd) {
        return stats.allSame ? vLongSize(in[0]) : Long.MAX_VALUE;
    }

    @Override
    public void encodePayload(final long[] in, final BlockStats stats, final DataOutput out, int bitsPerOrd) throws IOException {
        out.writeVLong(in[0]);
    }

    @Override
    public void decodePayload(final DataInput in, final long[] out, int bitsPerOrd) throws IOException {
        Arrays.fill(out, in.readVLong());
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
