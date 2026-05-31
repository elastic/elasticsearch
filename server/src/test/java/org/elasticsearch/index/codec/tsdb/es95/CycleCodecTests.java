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
        assertEquals(CycleCodec.ENCODING, Long.numberOfTrailingZeros(~header));
        assertEquals(3, (int) (header >>> 5));
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

    public void testWireFormatBytesLowK2() throws Exception {
        assertEncodedSize(new long[] { 0L, 7L }, 4, 3);
    }

    public void testWireFormatBytesLowK3() throws Exception {
        assertEncodedSize(new long[] { 0L, 5L, 10L }, 4, 4);
    }

    public void testWireFormatBytesLowK4() throws Exception {
        // NOTE: LOW K=4 spills the period into a 2-byte header because encoding 4
        // reserves 5 trailing bits, so period 4 no longer fits in a 1-byte vlong.
        assertEncodedSize(new long[] { 0L, 4L, 8L, 12L }, 4, 6);
    }

    public void testWireFormatBytesHighK2() throws Exception {
        final long mask = (1L << 16) - 1L;
        assertEncodedSize(new long[] { 0L, mask / 2 }, 16, 5);
    }

    public void testWireFormatBytesHighK4() throws Exception {
        final long mask = (1L << 16) - 1L;
        final long[] tuple = { 0L, mask / 4, mask / 2, (3 * mask) / 4 };
        assertEncodedSize(tuple, 16, 9);
    }

    public void testHeaderTrailingOneBitsCountIsFour() throws Exception {
        final long[] cycleValues = { 0L, 100L, 200L };
        final long[] in = new long[128];
        for (int i = 0; i < in.length; i++) {
            in[i] = cycleValues[i % cycleValues.length];
        }
        final BlockStats stats = new BlockStats();
        stats.recomputeWithCycle(in);
        final CodecContext ctx = new CodecContext(128);

        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        CycleCodec.INSTANCE.encodePayload(in, stats, ctx, out, 16);
        final ByteBuffersDataInput reader = new ByteBuffersDataInput(out.toBufferList());
        final long header = reader.readVLong();
        assertEquals(4, Long.numberOfTrailingZeros(~header));
        assertEquals(3, (int) (header >>> 5));
    }

    public void testInvalidPeriodInHeaderThrows() {
        final CodecContext ctx = new CodecContext(128);
        final long[] decoded = new long[128];
        // NOTE: period 200 exceeds blockSize / MAX_CYCLE_DIVISOR (16 at blockSize 128).
        final long badLeading = (200L << 5) | CycleCodec.ENCODING_MARKER;
        expectThrows(
            CorruptIndexException.class,
            () -> CycleCodec.INSTANCE.decodePayload(
                ctx,
                new ByteBuffersDataInput(new ByteBuffersDataOutput().toBufferList()),
                decoded,
                16,
                badLeading
            )
        );
    }

    private static void assertEncodedSize(long[] tuple, int bitsPerOrd, int expectedBytes) throws Exception {
        final int period = tuple.length;
        final long[] in = new long[128];
        for (int i = 0; i < in.length; i++) {
            in[i] = tuple[i % period];
        }
        final BlockStats stats = new BlockStats();
        stats.recomputeWithCycle(in);
        assertEquals(period, stats.cycleLength);
        final CodecContext ctx = new CodecContext(128);

        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        CycleCodec.INSTANCE.encodePayload(in, stats, ctx, out, bitsPerOrd);
        assertEquals("period=" + period + " bitsPerOrd=" + bitsPerOrd, expectedBytes, (int) out.size());
    }
}
