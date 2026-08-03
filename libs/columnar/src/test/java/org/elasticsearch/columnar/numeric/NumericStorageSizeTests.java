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
import org.apache.lucene.util.NumericUtils;
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

    public void testSplitDeltaTsdbBlock() throws IOException {
        // Two sub-runs of 64 values each with constant deltas -1 and +1; collapses to 1 bit per value.
        long[] block = new long[BLOCK];
        for (int i = 0; i < 64; i++) {
            block[i] = 10L - i;
        }
        for (int i = 64; i < BLOCK; i++) {
            block[i] = 10L + (i - 64);
        }
        NumericPipeline pipeline = new NumericPipeline(
            new BlockTransform[] { new SplitDeltaTransform(), DeltaTransform.INSTANCE, OffsetTransform.INSTANCE, GcdTransform.INSTANCE },
            new ForTerminal(BLOCK),
            BLOCK
        );
        int size = encodedSize(pipeline, block);
        assertTrue("SplitDelta TSDB block must be far below raw: " + size, size < RAW_BYTES / 10);
        assertEquals(24, size);
    }

    public void testAlpConstantDouble() throws IOException {
        // Constant 22.5: ALP maps to mantissa 225 (e=1, f=0), zero exceptions, Offset+FOR collapse to zero.
        long sv = NumericUtils.doubleToSortableLong(22.5);
        long[] block = new long[BLOCK];
        Arrays.fill(block, sv);
        NumericPipeline pipeline = new NumericPipeline(
            new BlockTransform[] {
                new AlpDoubleTransform(BLOCK),
                DeltaTransform.INSTANCE,
                OffsetTransform.INSTANCE,
                GcdTransform.INSTANCE },
            new ForTerminal(BLOCK),
            BLOCK
        );
        int size = encodedSize(pipeline, block);
        assertTrue("ALP constant-double block must be far below raw: " + size, size < RAW_BYTES / 100);
        assertEquals(7, size);
    }

    private static int encodedSize(long[] block) throws IOException {
        return encodedSize(NumericPipeline.defaultPipeline(BLOCK), block);
    }

    private static int encodedSize(NumericPipeline pipeline, long[] block) throws IOException {
        NumericBlockEncoder encoder = new NumericBlockEncoder(pipeline, BLOCK);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        encoder.encode(block.clone(), block.length, out);
        return Math.toIntExact(out.size());
    }
}
