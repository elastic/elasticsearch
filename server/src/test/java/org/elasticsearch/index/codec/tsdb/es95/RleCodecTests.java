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
 * Payload round-trip, applicability, and corruption tests for
 * {@link RleCodec}. Covers a 2-run block (in-budget), a too-fragmented
 * block whose {@link RleCodec#estimateSize} returns the sentinel, and a
 * malformed wire payload whose run lengths overflow the destination.
 */
public class RleCodecTests extends ESTestCase {

    public void testPayloadRoundTripTwoRuns() throws Exception {
        long[] in = new long[128];
        Arrays.fill(in, 0, 80, 7L);
        Arrays.fill(in, 80, 128, 11L);
        final BlockStats stats = new BlockStats();
        stats.recompute(in);
        final CodecContext ctx = new CodecContext(128);

        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        RleCodec.INSTANCE.encodePayload(in, stats, ctx, out, 16);

        long[] decoded = new long[128];
        RleCodec.INSTANCE.decodePayload(ctx, new ByteBuffersDataInput(out.toBufferList()), decoded, 16);
        assertArrayEquals(in, decoded);
    }

    public void testEstimateSizeIsSentinelWhenRunsExceedCap() {
        long[] in = new long[128];
        // NOTE: alternate 1/2/1/2/... so n_runs = 128 >> MAX_TRACKED_RUNS
        for (int i = 0; i < in.length; i++) {
            in[i] = (i & 1) == 0 ? 1L : 2L;
        }
        final BlockStats stats = new BlockStats();
        stats.recompute(in);

        assertEquals(Long.MAX_VALUE, RleCodec.INSTANCE.estimateSize(in, stats, 16));
    }

    public void testRunLengthOverflowThrows() {
        final CodecContext ctx = new CodecContext(128);
        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        try {
            out.writeVInt(1);
            out.writeVLong(42);
            out.writeVInt(200); // NOTE: run length 200 overflows the 128-slot destination
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        long[] decoded = new long[128];
        expectThrows(
            CorruptIndexException.class,
            () -> RleCodec.INSTANCE.decodePayload(ctx, new ByteBuffersDataInput(out.toBufferList()), decoded, 16)
        );
    }
}
