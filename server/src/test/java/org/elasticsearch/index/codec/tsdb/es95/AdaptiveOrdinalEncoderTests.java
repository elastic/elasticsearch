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
}
