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
 * Payload round-trip and applicability tests for {@link ConstantCodec}.
 * The codec writes a single vlong of {@code value << 1} (trailing zero
 * indicates CONST). Uniform blocks decode by reading back the leading vlong
 * and right-shifting; non-uniform input returns {@code Long.MAX_VALUE} from
 * {@code estimateSize} so the wrapper never selects this codec.
 */
public class ConstantCodecTests extends ESTestCase {

    public void testPayloadRoundTripUniform() throws Exception {
        long[] in = new long[128];
        Arrays.fill(in, 42L);
        final BlockStats stats = new BlockStats();
        stats.recompute(in);
        final CodecContext ctx = new CodecContext(128);

        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        ConstantCodec.INSTANCE.encodePayload(in, stats, ctx, out, 16);

        ByteBuffersDataInput reader = new ByteBuffersDataInput(out.toBufferList());
        long v1 = reader.readVLong();
        long[] decoded = new long[128];
        ConstantCodec.INSTANCE.decodePayload(ctx, reader, decoded, 16, v1);
        assertArrayEquals(in, decoded);
    }

    public void testEstimateSizeIsSentinelForNonUniform() {
        long[] in = new long[128];
        Arrays.fill(in, 7L);
        in[64] = 8L;
        final BlockStats stats = new BlockStats();
        stats.recompute(in);

        assertEquals(Long.MAX_VALUE, ConstantCodec.INSTANCE.estimateSize(in, stats, 16));
    }

    public void testEstimateSizeMatchesActualPayload() throws Exception {
        long[] in = new long[128];
        Arrays.fill(in, 12345L);
        final BlockStats stats = new BlockStats();
        stats.recompute(in);
        final CodecContext ctx = new CodecContext(128);

        long estimate = ConstantCodec.INSTANCE.estimateSize(in, stats, 16);
        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        ConstantCodec.INSTANCE.encodePayload(in, stats, ctx, out, 16);
        // NOTE: 12345 << 1 = 24690 -> 3-byte vlong; matches legacy CONST cost
        assertEquals(estimate, out.size());
        assertEquals(3L, out.size());
    }
}
