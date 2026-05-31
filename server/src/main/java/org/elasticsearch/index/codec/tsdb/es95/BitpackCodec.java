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

/**
 * Locally bit-packed block codec (encoding 3, sub-mode {@link #SUB_MODE}).
 * Subtracts {@code min}, then packs at the bits needed for {@code max - min}.
 * Payload after the encoding-3 header and sub-mode byte is
 * {@code [min:vlong][localBits:1][packed]}. Stateless; access via
 * {@link #INSTANCE}. Uses {@link CodecContext#scratch} for the min-subtracted
 * buffer and {@link CodecContext#forUtil} for bit-packing.
 */
final class BitpackCodec implements BlockModeCodec {

    static final int ENCODING = 3;
    static final byte SUB_MODE = 1;
    static final BitpackCodec INSTANCE = new BitpackCodec();

    private BitpackCodec() {}

    @Override
    public int encoding() {
        return ENCODING;
    }

    @Override
    public long estimateSize(final long[] in, final BlockStats stats, int bitsPerOrd) {
        int localBits = bitsRequired(stats.max - stats.min);
        int roundedLocalBits = DocValuesForUtil.roundBits(localBits);
        return 1L + 1L + vLongSize(stats.min) + 1L + ((long) in.length * roundedLocalBits + 7) / 8;
    }

    @Override
    public void encodePayload(final long[] in, final BlockStats stats, final CodecContext ctx, final DataOutput out, int bitsPerOrd)
        throws IOException {
        int localBits = bitsRequired(stats.max - stats.min);
        out.writeVLong(0b111);
        out.writeByte(SUB_MODE);
        out.writeVLong(stats.min);
        out.writeByte((byte) localBits);
        final long[] scratch = ctx.scratch;
        for (int i = 0; i < in.length; i++) {
            scratch[i] = in[i] - stats.min;
        }
        ctx.forUtil.encode(scratch, localBits, out);
    }

    @Override
    public void decodePayload(final CodecContext ctx, final DataInput in, final long[] out, int bitsPerOrd, long leadingVLong)
        throws IOException {
        long minVal = in.readVLong();
        int bits = in.readByte() & 0xff;
        if (bits > 63) {
            throw new CorruptIndexException(String.format(Locale.ROOT, "invalid BITPACK_LOCAL bits %d", bits), in);
        }
        ctx.forUtil.decode(bits, in, out);
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
