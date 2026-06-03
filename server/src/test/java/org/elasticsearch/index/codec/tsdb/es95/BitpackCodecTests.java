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
 * Payload round-trip and corruption tests for {@link BitpackCodec}. The
 * codec writes the encoding-3 header ({@code vlong 0b111}) followed by the
 * BITPACK_LOCAL sub-mode byte, then {@code vlong(min), localBits, packed}.
 * Covers a narrow-range block (small local bits relative to segment width)
 * and a malformed wire payload whose stored bit width exceeds 63.
 */
public class BitpackCodecTests extends ESTestCase {

    public void testPayloadRoundTripNarrowRange() throws Exception {
        long[] in = new long[128];
        for (int i = 0; i < in.length; i++) {
            in[i] = 1000L + randomIntBetween(0, 15);
        }
        final BlockStats stats = new BlockStats();
        stats.recompute(in);
        final CodecContext ctx = new CodecContext(128);

        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        BitpackCodec.INSTANCE.encodePayload(Arrays.copyOf(in, in.length), stats, ctx, out, 16);

        ByteBuffersDataInput reader = new ByteBuffersDataInput(out.toBufferList());
        long v1 = reader.readVLong();
        byte subMode = reader.readByte();
        assertEquals(BitpackCodec.SUB_MODE, subMode);
        long[] decoded = new long[128];
        BitpackCodec.INSTANCE.decodePayload(ctx, reader, decoded, 16, v1);
        assertArrayEquals(in, decoded);
    }

    public void testEstimateSizeMatchesActualPayload() throws Exception {
        long[] in = new long[128];
        for (int i = 0; i < in.length; i++) {
            in[i] = 1000L + randomIntBetween(0, 15);
        }
        final BlockStats stats = new BlockStats();
        stats.recompute(in);
        final CodecContext ctx = new CodecContext(128);

        long estimate = BitpackCodec.INSTANCE.estimateSize(in, stats, 16);
        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        BitpackCodec.INSTANCE.encodePayload(Arrays.copyOf(in, in.length), stats, ctx, out, 16);
        // NOTE: estimate must match the actual payload byte count
        assertEquals(estimate, out.size());
    }

    public void testInvalidLocalBitsThrows() {
        final CodecContext ctx = new CodecContext(128);
        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        try {
            out.writeVLong(0L);
            out.writeByte((byte) 99); // NOTE: 99 > 63, must throw
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        long[] decoded = new long[128];
        expectThrows(
            CorruptIndexException.class,
            () -> BitpackCodec.INSTANCE.decodePayload(ctx, new ByteBuffersDataInput(out.toBufferList()), decoded, 16, 0L)
        );
    }
}
