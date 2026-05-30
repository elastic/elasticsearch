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

import static org.hamcrest.Matchers.equalTo;

/**
 * Round-trip and mode-selection tests for {@link AdaptiveOrdinalEncoder}.
 * Each test crafts a 128-value block whose distribution forces a specific
 * mode, then verifies encode/decode symmetry and (where relevant) the
 * mode-byte selection by peeking the wire format directly.
 */
public class AdaptiveOrdinalEncoderTests extends ESTestCase {

    public void testLegacyRoundTripUniformRandom() throws Exception {
        // NOTE: 128 random ords spread across the full bits range -> forces LEGACY
        int bitsPerOrd = 16;
        long mask = (1L << bitsPerOrd) - 1L;
        long[] in = new long[128];
        for (int i = 0; i < in.length; i++) {
            in[i] = randomLongBetween(0L, mask);
        }

        AdaptiveOrdinalEncoder encoder = new AdaptiveOrdinalEncoder(128);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        encoder.encodeOrdinals(Arrays.copyOf(in, in.length), out, bitsPerOrd);

        long[] decoded = new long[128];
        encoder.decodeOrdinals(new ByteBuffersDataInput(out.toBufferList()), decoded, bitsPerOrd);

        assertArrayEquals(in, decoded);
    }

    public void testConstModeChosenForUniformBlock() throws Exception {
        long[] in = new long[128];
        Arrays.fill(in, 42L);

        AdaptiveOrdinalEncoder encoder = new AdaptiveOrdinalEncoder(128);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        encoder.encodeOrdinals(Arrays.copyOf(in, in.length), out, 16);

        ByteBuffersDataInput peek = new ByteBuffersDataInput(out.toBufferList());
        assertEquals(AdaptiveOrdinalEncoder.MODE_CONST, peek.readByte());

        long[] decoded = new long[128];
        encoder.decodeOrdinals(new ByteBuffersDataInput(out.toBufferList()), decoded, 16);
        assertArrayEquals(in, decoded);
    }

    public void testConstBlockEncodesToVeryFewBytes() throws Exception {
        long[] in = new long[128];
        Arrays.fill(in, 12345L);

        AdaptiveOrdinalEncoder encoder = new AdaptiveOrdinalEncoder(128);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        encoder.encodeOrdinals(Arrays.copyOf(in, in.length), out, 16);

        // NOTE: 1 byte mode + 2-byte vlong for 12345 = 3 bytes total
        assertThat(out.size(), equalTo(3L));
    }

    public void testBitpackLocalChosenWhenRangeIsNarrow() throws Exception {
        // NOTE: segment bits says 16; block uses ords in [1000, 1015] so local bits = 4
        long[] in = new long[128];
        for (int i = 0; i < in.length; i++) {
            in[i] = 1000L + randomIntBetween(0, 15);
        }
        int segmentBitsPerOrd = 16;

        AdaptiveOrdinalEncoder encoder = new AdaptiveOrdinalEncoder(128);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        encoder.encodeOrdinals(Arrays.copyOf(in, in.length), out, segmentBitsPerOrd);

        ByteBuffersDataInput peek = new ByteBuffersDataInput(out.toBufferList());
        assertEquals(AdaptiveOrdinalEncoder.MODE_BITPACK_LOCAL, peek.readByte());

        long[] decoded = new long[128];
        encoder.decodeOrdinals(new ByteBuffersDataInput(out.toBufferList()), decoded, segmentBitsPerOrd);
        assertArrayEquals(in, decoded);
    }
}
