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

/**
 * Payload round-trip, applicability, and corruption tests for {@link CycleCodec}.
 * The codec applies when {@link BlockStats#recomputeWithCycle} detects a period in
 * {@code [2, blockSize / MAX_CYCLE_DIVISOR]} (16 at blockSize 128).
 */
public class CycleCodecTests extends ESTestCase {

    public void testRoundTripPeriodThree() throws Exception {
        final long[] cycleValues = { 17L, 4242L, 65000L };
        final long[] in = new long[128];
        for (int i = 0; i < in.length; i++) {
            in[i] = cycleValues[i % cycleValues.length];
        }
        final BlockStats stats = new BlockStats();
        stats.recomputeWithCycle(in);
        assertEquals(3, stats.cycleLength);
        final CodecContext ctx = new CodecContext(128);

        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        CycleCodec.INSTANCE.encodePayload(in, stats, ctx, out, 16);

        final ByteBuffersDataInput reader = new ByteBuffersDataInput(out.toBufferList());
        final long header = reader.readVLong();
        final byte subMode = reader.readByte();
        assertEquals(CycleCodec.ENCODING, Long.numberOfTrailingZeros(~header));
        assertEquals(CycleCodec.SUB_MODE, subMode);
        final long[] decoded = new long[128];
        CycleCodec.INSTANCE.decodePayload(ctx, reader, decoded, 16, header);
        assertArrayEquals(in, decoded);
    }

    public void testEstimateMatchesActualPayload() throws Exception {
        final long[] cycleValues = { 0L, 21845L, 43690L };
        final long[] in = new long[128];
        for (int i = 0; i < in.length; i++) {
            in[i] = cycleValues[i % cycleValues.length];
        }
        final BlockStats stats = new BlockStats();
        stats.recomputeWithCycle(in);
        final CodecContext ctx = new CodecContext(128);

        final long estimate = CycleCodec.INSTANCE.estimateSize(in, stats, 16);
        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        CycleCodec.INSTANCE.encodePayload(in, stats, ctx, out, 16);
        assertEquals(estimate, out.size());
    }

    public void testDeclinesWhenNoCycle() {
        final long[] in = new long[128];
        for (int i = 0; i < in.length; i++) {
            in[i] = i;
        }
        final BlockStats stats = new BlockStats();
        stats.recomputeWithCycle(in);
        assertEquals(0, stats.cycleLength);
        assertEquals(Long.MAX_VALUE, CycleCodec.INSTANCE.estimateSize(in, stats, 16));
    }

    public void testDeclinesWhenRecomputeSkipsCycleScan() {
        final long[] cycleValues = { 17L, 4242L, 65000L };
        final long[] in = new long[128];
        for (int i = 0; i < in.length; i++) {
            in[i] = cycleValues[i % cycleValues.length];
        }
        final BlockStats stats = new BlockStats();
        stats.recompute(in);
        assertEquals(0, stats.cycleLength);
        assertEquals(Long.MAX_VALUE, CycleCodec.INSTANCE.estimateSize(in, stats, 16));
    }

    public void testDeclinesWhenPeriodTooLarge() {
        final long[] cycleValues = new long[32];
        for (int i = 0; i < cycleValues.length; i++) {
            cycleValues[i] = i;
        }
        final long[] in = new long[128];
        for (int i = 0; i < in.length; i++) {
            in[i] = cycleValues[i % cycleValues.length];
        }
        final BlockStats stats = new BlockStats();
        stats.recomputeWithCycle(in);
        assertEquals(0, stats.cycleLength);
        assertEquals(Long.MAX_VALUE, CycleCodec.INSTANCE.estimateSize(in, stats, 16));
    }

    public void testRoundTripBitForBitParityFootprintAtK2() throws Exception {
        // NOTE: K=2 with HIGH cardinality scattered values lands at ~6 bytes,
        // closing the gap with legacy CYCLE on the pure mid-tsid case.
        final long mask = (1L << 16) - 1L;
        final long[] cycleValues = { 0L, mask / 2 };
        final long[] in = new long[128];
        for (int i = 0; i < in.length; i++) {
            in[i] = cycleValues[i % cycleValues.length];
        }
        final BlockStats stats = new BlockStats();
        stats.recomputeWithCycle(in);
        final CodecContext ctx = new CodecContext(128);

        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        CycleCodec.INSTANCE.encodePayload(in, stats, ctx, out, 16);
        assertTrue("expected compact cycle to land at <=7 bytes for K=2 HIGH, got " + out.size(), out.size() <= 7);
    }

    public void testInvalidPeriodThrows() throws Exception {
        final CodecContext ctx = new CodecContext(128);
        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        out.writeVInt(200);
        final long[] decoded = new long[128];
        expectThrows(
            CorruptIndexException.class,
            () -> CycleCodec.INSTANCE.decodePayload(ctx, new ByteBuffersDataInput(out.toBufferList()), decoded, 16, 0L)
        );
    }
}
