/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

/**
 * Payload round-trip tests for {@link BitPackedCodec}. The wrapper-level
 * dispatch is tested in {@link SortedOrdinalCodecTests}; this suite
 * exercises the codec in isolation by populating a {@link BlockStats},
 * driving {@link BitPackedCodec#INSTANCE} directly, and asserting that
 * encode + decode is the identity for arbitrary uniform-random inputs.
 */
public class BitPackedCodecTests extends ESTestCase {

    public void testPayloadRoundTripUniformRandom() throws Exception {
        int bitsPerOrd = 16;
        long mask = (1L << bitsPerOrd) - 1L;
        long[] in = new long[128];
        for (int i = 0; i < in.length; i++) {
            in[i] = randomLongBetween(0L, mask);
        }
        final BlockStats stats = new BlockStats();
        stats.recompute(in);
        final CodecContext ctx = new CodecContext(128);

        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        BitPackedCodec.INSTANCE.encodePayload(Arrays.copyOf(in, in.length), stats, ctx, out, bitsPerOrd);

        ByteBuffersDataInput reader = new ByteBuffersDataInput(out.toBufferList());
        long v1 = reader.readVLong();
        long[] decoded = new long[128];
        BitPackedCodec.INSTANCE.decodePayload(ctx, reader, decoded, bitsPerOrd, v1);
        assertArrayEquals(in, decoded);
    }

    public void testEstimateSizeMatchesActualPayload() throws Exception {
        int bitsPerOrd = 12;
        long[] in = new long[128];
        for (int i = 0; i < in.length; i++) {
            in[i] = randomLongBetween(0L, (1L << bitsPerOrd) - 1L);
        }
        final BlockStats stats = new BlockStats();
        stats.recompute(in);
        final CodecContext ctx = new CodecContext(128);

        long estimate = BitPackedCodec.INSTANCE.estimateSize(in, stats, bitsPerOrd);
        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        BitPackedCodec.INSTANCE.encodePayload(Arrays.copyOf(in, in.length), stats, ctx, out, bitsPerOrd);
        // NOTE: estimate must match the actual payload byte count
        assertEquals(estimate, out.size());
    }
}
