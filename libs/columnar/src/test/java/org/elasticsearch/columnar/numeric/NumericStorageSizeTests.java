/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

import org.apache.lucene.store.ByteBuffersDataOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;

/**
 * Byte-exact storage-size checks for {@link NumericBlockEncoder} with the default pipeline. Deterministic
 * block shapes are encoded and their encoded length is pinned, so a silent storage regression (a stage
 * ceasing to fire, or an encoding growing) is caught. Compressible shapes must be far below the raw
 * {@code 128 * 8 = 1024} bytes; a full-random block sits right at the raw width.
 */
public class NumericStorageSizeTests extends ESTestCase {

    private static final int BLOCK = 128;
    private static final int RAW_BYTES = BLOCK * Long.BYTES; // 1024

    public void testAllConstant() throws IOException {
        // A single run collapses to a fire bitmask, a zero bits-per-value marker, and the offset param.
        long[] block = new long[BLOCK];
        Arrays.fill(block, 42L);
        int size = encodedSize(block);
        assertTrue("constant block must be far below raw: " + size, size < RAW_BYTES / 8);
        assertEquals(3, size);
    }

    public void testPerfectlyMonotonicStep() throws IOException {
        // Delta -> constant step of 1000 -> GCD 1000 -> all zeros: only the reversal params survive.
        long[] block = new long[BLOCK];
        for (int i = 0; i < BLOCK; i++) {
            block[i] = 1000L * i;
        }
        int size = encodedSize(block);
        assertTrue("monotonic step block must be far below raw: " + size, size < RAW_BYTES / 8);
        assertEquals(6, size);
    }

    public void testFullRandom() throws IOException {
        // Fixed-seed random so the size is deterministic; full-width longs resist all stages, so the
        // block bit-packs at 64 bits per value (1024 bytes) plus a couple of header bytes.
        java.util.Random r = new java.util.Random(42L);
        long[] block = new long[BLOCK];
        for (int i = 0; i < BLOCK; i++) {
            block[i] = r.nextLong();
        }
        int size = encodedSize(block);
        assertTrue("random block must sit at the raw width: " + size, size >= RAW_BYTES && size <= RAW_BYTES + 16);
        assertEquals(1026, size);
    }

    private static int encodedSize(long[] block) throws IOException {
        NumericBlockEncoder encoder = new NumericBlockEncoder(NumericPipeline.defaultPipeline(BLOCK), BLOCK);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        encoder.encode(block.clone(), out);
        return Math.toIntExact(out.size());
    }
}
