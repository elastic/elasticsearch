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
 * Cycle encoding applies when {@link BlockStats#recomputeWithCycle} detects a period
 * {@code p} in {@code [2, blockSize / MAX_CYCLE_DIVISOR]} (16 at the default block
 * size of 128). Covers a 3-period block, the no-cycle case whose estimate is the
 * sentinel, the run-only path that {@link BlockStats#recompute} drives (cycle
 * detection skipped), and a malformed wire payload whose period exceeds the block
 * length.
 */
public class CycleCodecTests extends ESTestCase {

    public void testPayloadRoundTripPeriodThree() throws Exception {
        long[] in = new long[128];
        long[] cycleValues = { 17L, 4242L, 65000L };
        for (int i = 0; i < in.length; i++) {
            in[i] = cycleValues[i % cycleValues.length];
        }
        final BlockStats stats = new BlockStats();
        stats.recomputeWithCycle(in);
        assertEquals(3, stats.cycleLength);
        final CodecContext ctx = new CodecContext(128);

        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        CycleCodec.INSTANCE.encodePayload(in, stats, ctx, out, 16);

        ByteBuffersDataInput reader = new ByteBuffersDataInput(out.toBufferList());
        long v1 = reader.readVLong();
        byte subMode = reader.readByte();
        assertEquals(CycleCodec.SUB_MODE, subMode);
        long[] decoded = new long[128];
        CycleCodec.INSTANCE.decodePayload(ctx, reader, decoded, 16, v1);
        assertArrayEquals(in, decoded);
    }

    public void testEstimateSizeIsSentinelWhenNoCycle() {
        long[] in = new long[128];
        for (int i = 0; i < in.length; i++) {
            // NOTE: monotonically increasing -> no repetition, no detectable cycle
            in[i] = i;
        }
        final BlockStats stats = new BlockStats();
        stats.recomputeWithCycle(in);
        assertEquals(0, stats.cycleLength);
        assertEquals(Long.MAX_VALUE, CycleCodec.INSTANCE.estimateSize(in, stats, 16));
    }

    public void testEstimateSizeIsSentinelWhenRecomputeSkipsCycleScan() {
        long[] in = new long[128];
        long[] cycleValues = { 17L, 4242L, 65000L };
        for (int i = 0; i < in.length; i++) {
            in[i] = cycleValues[i % cycleValues.length];
        }
        final BlockStats stats = new BlockStats();
        // NOTE: plain recompute leaves cycleLength = 0, so CycleCodec must decline
        stats.recompute(in);
        assertEquals(0, stats.cycleLength);
        assertEquals(Long.MAX_VALUE, CycleCodec.INSTANCE.estimateSize(in, stats, 16));
    }

    public void testEstimateSizeMatchesActualPayload() throws Exception {
        long[] in = new long[128];
        long[] cycleValues = { 17L, 4242L, 65000L };
        for (int i = 0; i < in.length; i++) {
            in[i] = cycleValues[i % cycleValues.length];
        }
        final BlockStats stats = new BlockStats();
        stats.recomputeWithCycle(in);
        final CodecContext ctx = new CodecContext(128);

        long estimate = CycleCodec.INSTANCE.estimateSize(in, stats, 16);
        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        CycleCodec.INSTANCE.encodePayload(in, stats, ctx, out, 16);
        assertEquals(estimate, out.size());
    }

    public void testEstimateSizeIsSentinelWhenCyclePeriodTooLarge() {
        // NOTE: period 32 exceeds blockSize / MAX_CYCLE_DIVISOR = 16 at blockSize 128
        long[] in = new long[128];
        long[] cycleValues = new long[32];
        for (int i = 0; i < cycleValues.length; i++) {
            cycleValues[i] = i;
        }
        for (int i = 0; i < in.length; i++) {
            in[i] = cycleValues[i % cycleValues.length];
        }
        final BlockStats stats = new BlockStats();
        stats.recomputeWithCycle(in);
        assertEquals(0, stats.cycleLength);
        assertEquals(Long.MAX_VALUE, CycleCodec.INSTANCE.estimateSize(in, stats, 16));
    }

    public void testInvalidPeriodThrows() {
        final CodecContext ctx = new CodecContext(128);
        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        try {
            // NOTE: period 200 is larger than the 128-slot destination
            out.writeVInt(200);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        long[] decoded = new long[128];
        expectThrows(
            CorruptIndexException.class,
            () -> CycleCodec.INSTANCE.decodePayload(ctx, new ByteBuffersDataInput(out.toBufferList()), decoded, 16, 0L)
        );
    }
}
