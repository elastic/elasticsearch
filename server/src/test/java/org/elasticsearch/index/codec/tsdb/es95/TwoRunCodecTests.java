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
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

/**
 * Payload round-trip and applicability tests for {@link TwoRunCodec}. The
 * codec writes {@code (firstOrd << 2) | 0b01} as a vlong, the first run
 * length as a vint, and the delta to the second ordinal as a zlong. Blocks
 * with anything other than exactly two runs return the sentinel from
 * {@code estimateSize}.
 */
public class TwoRunCodecTests extends ESTestCase {

    public void testPayloadRoundTripTwoRuns() throws Exception {
        long[] in = new long[128];
        Arrays.fill(in, 0, 80, 7L);
        Arrays.fill(in, 80, 128, 11L);
        final BlockStats stats = new BlockStats();
        stats.recompute(in);
        final CodecContext ctx = new CodecContext(128);

        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        TwoRunCodec.INSTANCE.encodePayload(in, stats, ctx, out, 16);

        ByteBuffersDataInput reader = new ByteBuffersDataInput(out.toBufferList());
        long v1 = reader.readVLong();
        long[] decoded = new long[128];
        TwoRunCodec.INSTANCE.decodePayload(ctx, reader, decoded, 16, v1);
        assertArrayEquals(in, decoded);
    }

    public void testEstimateSizeIsSentinelForSingleRun() {
        long[] in = new long[128];
        Arrays.fill(in, 7L);
        final BlockStats stats = new BlockStats();
        stats.recompute(in);

        assertEquals(Long.MAX_VALUE, TwoRunCodec.INSTANCE.estimateSize(in, stats, 16));
    }

    public void testEstimateSizeIsSentinelForThreeRuns() {
        long[] in = new long[128];
        Arrays.fill(in, 0, 40, 1L);
        Arrays.fill(in, 40, 80, 2L);
        Arrays.fill(in, 80, 128, 3L);
        final BlockStats stats = new BlockStats();
        stats.recompute(in);

        assertEquals(Long.MAX_VALUE, TwoRunCodec.INSTANCE.estimateSize(in, stats, 16));
    }

    public void testEstimateSizeMatchesActualPayload() throws Exception {
        long[] in = new long[128];
        Arrays.fill(in, 0, 64, 100L);
        Arrays.fill(in, 64, 128, 200L);
        final BlockStats stats = new BlockStats();
        stats.recompute(in);
        final CodecContext ctx = new CodecContext(128);

        long estimate = TwoRunCodec.INSTANCE.estimateSize(in, stats, 16);
        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        TwoRunCodec.INSTANCE.encodePayload(in, stats, ctx, out, 16);
        // NOTE: estimate must match the actual payload byte count
        assertEquals(estimate, out.size());
    }

    public void testCorruptFirstRunLenThrows() throws Exception {
        long[] in = new long[128];
        Arrays.fill(in, 0, 64, 100L);
        Arrays.fill(in, 64, 128, 200L);
        final BlockStats stats = new BlockStats();
        stats.recompute(in);
        final CodecContext ctx = new CodecContext(128);

        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        // NOTE: synthesise a TWO_RUN header followed by an out-of-range run length
        out.writeVLong((100L << 2) | 0b01);
        out.writeVInt(200);     // NOTE: 200 overflows the 128-slot destination
        out.writeZLong(100L);

        ByteBuffersDataInput reader = new ByteBuffersDataInput(out.toBufferList());
        long v1 = reader.readVLong();
        long[] decoded = new long[128];
        expectThrows(CorruptIndexException.class, () -> TwoRunCodec.INSTANCE.decodePayload(ctx, reader, decoded, 16, v1));
    }
}
