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
import org.elasticsearch.index.codec.tsdb.DocValuesForUtil;

import java.io.IOException;
import java.util.Locale;

/** Locally bit-packed block: subtract {@code min}, pack at the bits needed for
 *  {@code max - min}. Payload is {@code [min:vlong][localBits:1][packed]}.
 */
final class BitpackCodec implements BlockModeCodec {

    static final byte MODE = 3;

    private final DocValuesForUtil forUtil;
    private final long[] scratch;

    BitpackCodec(int blockSize) {
        this.forUtil = new DocValuesForUtil(blockSize);
        this.scratch = new long[blockSize];
    }

    @Override
    public byte mode() {
        return MODE;
    }

    @Override
    public long estimateSize(final long[] in, final BlockStats stats, int bitsPerOrd) {
        int localBits = bitsRequired(stats.max - stats.min);
        int roundedLocalBits = DocValuesForUtil.roundBits(localBits);
        return vLongSize(stats.min) + 1L + ((long) in.length * roundedLocalBits + 7) / 8;
    }

    @Override
    public void encodePayload(final long[] in, final BlockStats stats, final DataOutput out, int bitsPerOrd) throws IOException {
        int localBits = bitsRequired(stats.max - stats.min);
        out.writeVLong(stats.min);
        out.writeByte((byte) localBits);
        for (int i = 0; i < in.length; i++) {
            scratch[i] = in[i] - stats.min;
        }
        forUtil.encode(scratch, localBits, out);
    }

    @Override
    public void decodePayload(final DataInput in, final long[] out, int bitsPerOrd) throws IOException {
        long minVal = in.readVLong();
        int bits = in.readByte() & 0xff;
        if (bits > 63) {
            throw new CorruptIndexException(String.format(Locale.ROOT, "invalid BITPACK_LOCAL bits %d", bits), in);
        }
        forUtil.decode(bits, in, out);
        for (int i = 0; i < out.length; i++) {
            out[i] += minVal;
        }
    }

    private static int bitsRequired(long range) {
        return range == 0 ? 0 : 64 - Long.numberOfLeadingZeros(range);
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
