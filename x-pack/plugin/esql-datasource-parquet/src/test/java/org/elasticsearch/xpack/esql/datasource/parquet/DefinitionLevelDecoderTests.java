/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

/**
 * Verifies that {@link DefinitionLevelDecoder} produces the correct null bitmask
 * for a variety of RLE-hybrid encoded def-level streams. Inputs are produced by
 * parquet-mr's {@link RunLengthBitPackingHybridEncoder} (the same encoder Parquet
 * writers use), so this test asserts byte-level format compatibility.
 */
public class DefinitionLevelDecoderTests extends ESTestCase {

    public void testNonNullableShortCircuit() {
        DefinitionLevelDecoder decoder = new DefinitionLevelDecoder();
        decoder.init(ByteBuffer.allocate(0), 0, false);

        WordMask mask = new WordMask();
        mask.reset(100);
        int nonNull = decoder.readBatch(100, mask, 0);
        assertThat(nonNull, equalTo(100));
        assertTrue("mask should be empty (no nulls)", mask.isEmpty());

        // skip is also a no-op-ish (returns count as nonNull)
        DefinitionLevelDecoder skipDecoder = new DefinitionLevelDecoder();
        skipDecoder.init(ByteBuffer.allocate(0), 0, false);
        assertThat(skipDecoder.skip(50), equalTo(50));
    }

    public void testEmptyBatch() throws IOException {
        int[] defs = new int[] { 1, 0, 1, 1, 0 };
        DefinitionLevelDecoder decoder = newDecoder(defs, 1, false);
        WordMask mask = new WordMask();
        mask.reset(8);
        assertThat(decoder.readBatch(0, mask, 0), equalTo(0));
        assertTrue(mask.isEmpty());
    }

    public void testAllPresentRleRun() throws IOException {
        int[] defs = new int[1024];
        for (int i = 0; i < defs.length; i++) {
            defs[i] = 1;
        }
        DefinitionLevelDecoder decoder = newDecoder(defs, 1, false);

        WordMask mask = new WordMask();
        mask.reset(defs.length);
        int nonNull = decoder.readBatch(defs.length, mask, 0);
        assertThat(nonNull, equalTo(defs.length));
        assertTrue("no nulls expected", mask.isEmpty());
    }

    public void testAllNullRleRun() throws IOException {
        int[] defs = new int[1024];
        // all zeros: all-null run
        DefinitionLevelDecoder decoder = newDecoder(defs, 1, false);

        WordMask mask = new WordMask();
        mask.reset(defs.length);
        int nonNull = decoder.readBatch(defs.length, mask, 0);
        assertThat(nonNull, equalTo(0));
        assertThat(mask.popCount(), equalTo(defs.length));
        for (int i = 0; i < defs.length; i++) {
            assertTrue("bit " + i, mask.get(i));
        }
    }

    public void testAlternatingPattern() throws IOException {
        // alternating 0/1 forces bit-packed encoding (no long runs)
        int[] defs = new int[256];
        for (int i = 0; i < defs.length; i++) {
            defs[i] = i & 1;
        }
        DefinitionLevelDecoder decoder = newDecoder(defs, 1, false);

        WordMask mask = new WordMask();
        mask.reset(defs.length);
        int nonNull = decoder.readBatch(defs.length, mask, 0);
        assertThat(nonNull, equalTo(defs.length / 2));
        for (int i = 0; i < defs.length; i++) {
            assertEquals("bit " + i, defs[i] == 0, mask.get(i));
        }
    }

    public void testMixedRunsAndBitPacked() throws IOException {
        // 64 ones, then 64 alternating, then 64 zeros, then 64 alternating
        int[] defs = new int[256];
        for (int i = 0; i < 64; i++) {
            defs[i] = 1;
        }
        for (int i = 64; i < 128; i++) {
            defs[i] = i & 1;
        }
        for (int i = 128; i < 192; i++) {
            defs[i] = 0;
        }
        for (int i = 192; i < 256; i++) {
            defs[i] = (i + 1) & 1;
        }

        DefinitionLevelDecoder decoder = newDecoder(defs, 1, false);
        WordMask mask = new WordMask();
        mask.reset(defs.length);
        int nonNull = decoder.readBatch(defs.length, mask, 0);

        int expectedNulls = 0;
        for (int d : defs) {
            if (d == 0) {
                expectedNulls++;
            }
        }
        assertThat(nonNull, equalTo(defs.length - expectedNulls));
        for (int i = 0; i < defs.length; i++) {
            assertEquals("bit " + i, defs[i] == 0, mask.get(i));
        }
    }

    public void testSplitAcrossMultipleReadBatchCalls() throws IOException {
        int[] defs = randomDefLevels(500, 1);
        DefinitionLevelDecoder decoder = newDecoder(defs, 1, false);

        WordMask mask = new WordMask();
        mask.reset(defs.length);

        // Drain in random-sized chunks to exercise cross-call state for both RLE runs and bit-packed groups
        int totalNonNull = 0;
        int produced = 0;
        while (produced < defs.length) {
            int chunk = Math.min(defs.length - produced, randomIntBetween(1, 73));
            totalNonNull += decoder.readBatch(chunk, mask, produced);
            produced += chunk;
        }

        int expectedNulls = 0;
        for (int d : defs) {
            if (d == 0) {
                expectedNulls++;
            }
        }
        assertThat(totalNonNull, equalTo(defs.length - expectedNulls));
        for (int i = 0; i < defs.length; i++) {
            assertEquals("bit " + i + " (def=" + defs[i] + ")", defs[i] == 0, mask.get(i));
        }
    }

    public void testReadBatchWithNonZeroOffset() throws IOException {
        int[] defs = new int[] { 1, 0, 0, 1, 1, 0, 1, 1, 0, 0 };
        DefinitionLevelDecoder decoder = newDecoder(defs, 1, false);

        WordMask mask = new WordMask();
        mask.reset(20);
        // pre-set a bit before the offset to ensure we don't accidentally clobber unrelated bits
        mask.set(0);
        int nonNull = decoder.readBatch(defs.length, mask, 5);
        int expectedNulls = 0;
        for (int d : defs) {
            if (d == 0) {
                expectedNulls++;
            }
        }
        assertThat(nonNull, equalTo(defs.length - expectedNulls));
        assertTrue("pre-existing bit at 0 must be preserved", mask.get(0));
        for (int i = 0; i < defs.length; i++) {
            assertEquals("bit " + (5 + i), defs[i] == 0, mask.get(5 + i));
        }
    }

    public void testMaxDefLevelGreaterThanOne() throws IOException {
        // Simulate a column with maxDefLevel=3: any value < 3 is null, value 3 is present.
        int[] defs = new int[200];
        for (int i = 0; i < defs.length; i++) {
            defs[i] = randomIntBetween(0, 3);
        }
        DefinitionLevelDecoder decoder = newDecoder(defs, 3, false);

        WordMask mask = new WordMask();
        mask.reset(defs.length);
        int nonNull = decoder.readBatch(defs.length, mask, 0);

        int expectedNulls = 0;
        for (int d : defs) {
            if (d < 3) {
                expectedNulls++;
            }
        }
        assertThat(nonNull, equalTo(defs.length - expectedNulls));
        for (int i = 0; i < defs.length; i++) {
            assertEquals("bit " + i + " (def=" + defs[i] + ")", defs[i] < 3, mask.get(i));
        }
    }

    public void testWithLengthPrefix() throws IOException {
        // V1 pages prefix the def-level payload with a 4-byte little-endian length.
        int[] defs = randomDefLevels(300, 1);
        DefinitionLevelDecoder decoder = newDecoder(defs, 1, true);

        WordMask mask = new WordMask();
        mask.reset(defs.length);
        int nonNull = decoder.readBatch(defs.length, mask, 0);

        int expectedNulls = 0;
        for (int d : defs) {
            if (d == 0) {
                expectedNulls++;
            }
        }
        assertThat(nonNull, equalTo(defs.length - expectedNulls));
        for (int i = 0; i < defs.length; i++) {
            assertEquals("bit " + i + " (def=" + defs[i] + ")", defs[i] == 0, mask.get(i));
        }
    }

    public void testWideBitWidthsExerciseUnpackPaths() throws IOException {
        // Drive bitWidth=4 (maxDefLevel=15) and bitWidth=8 (maxDefLevel=255) so the dedicated
        // case branches of unpack8Values are exercised end-to-end.
        for (int maxDefLevel : new int[] { 15, 255 }) {
            int[] defs = randomDefLevels(513, maxDefLevel);
            DefinitionLevelDecoder decoder = newDecoder(defs, maxDefLevel, false);
            WordMask mask = new WordMask();
            mask.reset(defs.length);

            int produced = 0;
            int totalNonNull = 0;
            while (produced < defs.length) {
                int chunk = Math.min(defs.length - produced, randomIntBetween(1, 67));
                totalNonNull += decoder.readBatch(chunk, mask, produced);
                produced += chunk;
            }

            int expectedNulls = 0;
            for (int d : defs) {
                if (d < maxDefLevel) {
                    expectedNulls++;
                }
            }
            assertEquals("maxDefLevel=" + maxDefLevel, defs.length - expectedNulls, totalNonNull);
            for (int i = 0; i < defs.length; i++) {
                assertEquals("maxDefLevel=" + maxDefLevel + " bit=" + i + " def=" + defs[i], defs[i] < maxDefLevel, mask.get(i));
            }
        }
    }

    public void testSkipWithLengthPrefix() throws IOException {
        int[] defs = randomDefLevels(400, 1);

        DefinitionLevelDecoder readDecoder = newDecoder(defs, 1, true);
        WordMask mask = new WordMask();
        mask.reset(defs.length);
        int nonNullFromRead = readDecoder.readBatch(defs.length, mask, 0);

        DefinitionLevelDecoder skipDecoder = newDecoder(defs, 1, true);
        int nonNullFromSkip = 0;
        int skipped = 0;
        while (skipped < defs.length) {
            int chunk = Math.min(defs.length - skipped, randomIntBetween(1, 89));
            nonNullFromSkip += skipDecoder.skip(chunk);
            skipped += chunk;
        }
        assertThat(nonNullFromSkip, equalTo(nonNullFromRead));
    }

    public void testSkipMatchesReadBatchNullCount() throws IOException {
        int[] defs = randomDefLevels(700, 1);

        // First decoder: full readBatch
        DefinitionLevelDecoder readDecoder = newDecoder(defs, 1, false);
        WordMask mask = new WordMask();
        mask.reset(defs.length);
        int nonNullFromRead = readDecoder.readBatch(defs.length, mask, 0);

        // Second decoder: skip everything
        DefinitionLevelDecoder skipDecoder = newDecoder(defs, 1, false);
        int nonNullFromSkip = skipDecoder.skip(defs.length);

        assertThat(nonNullFromSkip, equalTo(nonNullFromRead));
    }

    public void testSkipInChunks() throws IOException {
        int[] defs = randomDefLevels(500, 1);
        DefinitionLevelDecoder decoder = newDecoder(defs, 1, false);

        int expectedNonNull = 0;
        for (int d : defs) {
            if (d == 1) {
                expectedNonNull++;
            }
        }

        int totalNonNull = 0;
        int skipped = 0;
        while (skipped < defs.length) {
            int chunk = Math.min(defs.length - skipped, randomIntBetween(1, 91));
            totalNonNull += decoder.skip(chunk);
            skipped += chunk;
        }
        assertThat(totalNonNull, equalTo(expectedNonNull));
    }

    public void testInterleavedReadAndSkip() throws IOException {
        int[] defs = randomDefLevels(400, 1);

        // Generate a deterministic chunk plan ahead of time so we can reuse it for the verifier.
        record Chunk(int size, boolean read) {}
        List<Chunk> plan = new ArrayList<>();
        int planned = 0;
        boolean readMode = true;
        while (planned < defs.length) {
            int chunk = Math.min(defs.length - planned, randomIntBetween(1, 47));
            plan.add(new Chunk(chunk, readMode));
            planned += chunk;
            readMode = readMode == false;
        }

        DefinitionLevelDecoder decoder = newDecoder(defs, 1, false);
        WordMask mask = new WordMask();
        mask.reset(defs.length);

        int position = 0;
        for (Chunk chunk : plan) {
            if (chunk.read()) {
                decoder.readBatch(chunk.size(), mask, position);
            } else {
                decoder.skip(chunk.size());
            }
            position += chunk.size();
        }

        // Verify each region: read regions match defs[]; skip regions remain 0 in the mask.
        position = 0;
        for (Chunk chunk : plan) {
            if (chunk.read()) {
                for (int j = 0; j < chunk.size(); j++) {
                    int idx = position + j;
                    assertEquals("bit " + idx, defs[idx] == 0, mask.get(idx));
                }
            } else {
                for (int j = 0; j < chunk.size(); j++) {
                    int idx = position + j;
                    assertFalse("skipped bit " + idx + " must remain 0", mask.get(idx));
                }
            }
            position += chunk.size();
        }
    }

    public void testAllPresentSkipsBitmask() throws IOException {
        // Scenario: every value present (def=1). The decoder should set NO bits in the mask
        // even after multiple readBatch calls.
        int[] defs = new int[400];
        for (int i = 0; i < defs.length; i++) {
            defs[i] = 1;
        }
        DefinitionLevelDecoder decoder = newDecoder(defs, 1, false);

        WordMask mask = new WordMask();
        mask.reset(defs.length);
        int produced = 0;
        while (produced < defs.length) {
            int chunk = Math.min(defs.length - produced, randomIntBetween(1, 97));
            int nonNull = decoder.readBatch(chunk, mask, produced);
            assertThat(nonNull, equalTo(chunk));
            produced += chunk;
        }
        assertTrue("no nulls expected", mask.isEmpty());
    }

    public void testReinitOnSameInstance() throws IOException {
        // First page: maxDefLevel=1, fully present
        int[] firstDefs = new int[100];
        for (int i = 0; i < firstDefs.length; i++) {
            firstDefs[i] = 1;
        }
        DefinitionLevelDecoder decoder = newDecoder(firstDefs, 1, false);
        WordMask firstMask = new WordMask();
        firstMask.reset(firstDefs.length);
        int firstNonNull = decoder.readBatch(firstDefs.length, firstMask, 0);
        assertThat(firstNonNull, equalTo(firstDefs.length));
        assertTrue("first page: no nulls expected", firstMask.isEmpty());

        // Re-init the same instance with a brand new buffer + different maxDefLevel,
        // BEFORE draining the first stream. State must be fully replaced.
        int[] secondDefs = randomDefLevels(150, 3);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (
            RunLengthBitPackingHybridEncoder encoder = new RunLengthBitPackingHybridEncoder(
                32 - Integer.numberOfLeadingZeros(3),
                64,
                1024,
                new HeapByteBufferAllocator()
            )
        ) {
            for (int d : secondDefs) {
                encoder.writeInt(d);
            }
            byte[] payload = encoder.toBytes().toByteArray();
            ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
            decoder.init(buf, 3, false);
        }

        WordMask secondMask = new WordMask();
        secondMask.reset(secondDefs.length);
        int secondNonNull = decoder.readBatch(secondDefs.length, secondMask, 0);
        int expectedNulls = 0;
        for (int d : secondDefs) {
            if (d < 3) {
                expectedNulls++;
            }
        }
        assertThat(secondNonNull, equalTo(secondDefs.length - expectedNulls));
        for (int i = 0; i < secondDefs.length; i++) {
            assertEquals("bit " + i + " (def=" + secondDefs[i] + ")", secondDefs[i] < 3, secondMask.get(i));
        }

        // Re-init again, this time switching to nonNullable (maxDefLevel=0). All ops must be no-ops.
        decoder.init(ByteBuffer.allocate(0), 0, false);
        WordMask thirdMask = new WordMask();
        thirdMask.reset(50);
        int thirdNonNull = decoder.readBatch(50, thirdMask, 0);
        assertThat(thirdNonNull, equalTo(50));
        assertTrue("nonNullable re-init: no nulls expected", thirdMask.isEmpty());
    }

    public void testRandomizedManyConfigurations() throws IOException {
        for (int trial = 0; trial < 25; trial++) {
            int maxDefLevel = randomFrom(1, 1, 1, 1, 2, 3, 7); // bias to bitWidth=1 (the common case)
            int length = randomIntBetween(1, 4096);
            int[] defs = randomDefLevels(length, maxDefLevel);
            boolean withLengthPrefix = randomBoolean();

            DefinitionLevelDecoder decoder = newDecoder(defs, maxDefLevel, withLengthPrefix);
            WordMask mask = new WordMask();
            mask.reset(length);

            int totalNonNull = 0;
            int produced = 0;
            while (produced < length) {
                int chunk = Math.min(length - produced, randomIntBetween(1, 257));
                totalNonNull += decoder.readBatch(chunk, mask, produced);
                produced += chunk;
            }

            int expectedNulls = 0;
            for (int d : defs) {
                if (d < maxDefLevel) {
                    expectedNulls++;
                }
            }
            assertEquals("trial=" + trial + " maxDefLevel=" + maxDefLevel + " length=" + length, length - expectedNulls, totalNonNull);
            for (int i = 0; i < length; i++) {
                assertEquals(
                    "trial=" + trial + " bit=" + i + " def=" + defs[i] + " maxDefLevel=" + maxDefLevel,
                    defs[i] < maxDefLevel,
                    mask.get(i)
                );
            }
        }
    }

    // --- helpers ---

    /**
     * Builds a {@link DefinitionLevelDecoder} initialized over the encoded form of {@code defs},
     * mimicking what a parquet-mr writer would emit for a page with this set of def levels.
     */
    private static DefinitionLevelDecoder newDecoder(int[] defs, int maxDefLevel, boolean withLengthPrefix) throws IOException {
        int bitWidth = 32 - Integer.numberOfLeadingZeros(maxDefLevel);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (
            RunLengthBitPackingHybridEncoder encoder = new RunLengthBitPackingHybridEncoder(
                bitWidth,
                64,
                1024,
                new HeapByteBufferAllocator()
            )
        ) {
            for (int d : defs) {
                encoder.writeInt(d);
            }
            BytesInput bytes = encoder.toBytes();
            byte[] payload = bytes.toByteArray();

            ByteBuffer buf;
            if (withLengthPrefix) {
                buf = ByteBuffer.allocate(4 + payload.length).order(ByteOrder.LITTLE_ENDIAN);
                buf.putInt(payload.length);
                buf.put(payload);
                buf.flip();
            } else {
                buf = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
            }

            DefinitionLevelDecoder decoder = new DefinitionLevelDecoder();
            decoder.init(buf, maxDefLevel, withLengthPrefix);
            return decoder;
        }
    }

    private int[] randomDefLevels(int length, int maxDefLevel) {
        int[] defs = new int[length];
        // Mix of run-friendly stretches and noisy stretches to stress both RLE and bit-packed paths
        int i = 0;
        while (i < length) {
            int span = Math.min(length - i, randomIntBetween(1, 200));
            int mode = randomIntBetween(0, 3);
            switch (mode) {
                case 0 -> {
                    int v = randomIntBetween(0, maxDefLevel);
                    for (int j = 0; j < span; j++) {
                        defs[i + j] = v;
                    }
                }
                case 1 -> {
                    for (int j = 0; j < span; j++) {
                        defs[i + j] = (i + j) & 1;
                    }
                }
                default -> {
                    for (int j = 0; j < span; j++) {
                        defs[i + j] = randomIntBetween(0, maxDefLevel);
                    }
                }
            }
            i += span;
        }
        return defs;
    }
}
