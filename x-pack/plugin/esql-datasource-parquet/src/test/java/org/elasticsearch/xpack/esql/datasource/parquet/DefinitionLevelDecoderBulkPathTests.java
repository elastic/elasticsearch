/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Tests that decode {@link DefinitionLevelDecoder} output and parquet-mr's
 * {@link RunLengthBitPackingHybridDecoder} from the same encoded byte stream and assert
 * bit-by-bit equivalence — i.e. parquet-mr's decoder is the ground truth, not the original
 * {@code int[]} of def levels. This guards against any drift between our format reading and
 * parquet-mr's wire-format interpretation, even in cases where the def-level array round-trips
 * correctly through both encoder and decoder by coincidence.
 *
 * <p>The bulk of this suite targets the bulk 64-bits-at-a-time fast path for {@code bitWidth==1}:
 * <ul>
 *   <li>Force runs long enough ({@code >= 64}) that the bulk path is exercised.</li>
 *   <li>Vary the alignment of the destination offset within the {@link WordMask} to cover
 *       both word-aligned and misaligned long writes.</li>
 *   <li>Mix RLE + bit-packed + bit-packed-spanning-multiple-readBatch-calls so the bulk path
 *       gets entered/exited at varying state-machine points.</li>
 *   <li>Cover all-null, all-present, sparse-null, dense-null and random patterns.</li>
 * </ul>
 *
 * <p>An additional case covers the {@code unpack8Values} {@code default} branch (bit widths
 * other than 1, 2, 4, 8) by driving {@code bitWidth==3}, so the general bit-shifted readout
 * path is also tied to parquet-mr's wire format.
 */
public class DefinitionLevelDecoderBulkPathTests extends ESTestCase {

    /** Long enough to comfortably exceed 64 def levels in a single bit-packed run. */
    private static final int LARGE_LENGTH = 1024;

    public void testBulkPathWordAlignedAllNullsBitPacked() throws IOException {
        // Force a bit-packed run with all zeros (= all null). After encoding, parquet-mr will
        // typically choose RLE for long all-zero runs — so we use a known anti-RLE pattern (a
        // few sparse non-zero values) to push the encoder toward bit-packed and isolate the
        // bulk path. We then assert against parquet-mr.
        int[] defs = new int[LARGE_LENGTH];
        // Sprinkle one non-null every 64 levels: this prevents long RLE runs of zeros and
        // forces bit-packed encoding for stretches >= 64.
        for (int i = 0; i < defs.length; i += 64) {
            defs[i] = 1;
        }
        assertParityWithParquetMrDecoder(defs, 1);
    }

    public void testBulkPathDenseNullsBitPacked() throws IOException {
        int[] defs = new int[LARGE_LENGTH];
        // Random pattern with ~50% nulls (high-entropy → bit-packed runs).
        for (int i = 0; i < defs.length; i++) {
            defs[i] = randomBoolean() ? 1 : 0;
        }
        assertParityWithParquetMrDecoder(defs, 1);
    }

    public void testBulkPathSparseNullsBitPacked() throws IOException {
        int[] defs = new int[LARGE_LENGTH];
        // Mostly-present with rare nulls — exercises the `if (nullLong != 0)` short-circuit
        // when the inverted long happens to be zero (no nulls in this 64-level chunk).
        for (int i = 0; i < defs.length; i++) {
            defs[i] = randomIntBetween(0, 99) < 95 ? 1 : 0;
        }
        // Drop a small bit-packed seed to discourage the encoder from emitting one giant RLE run.
        defs[37] = 0;
        defs[83] = 0;
        defs[140] = 0;
        assertParityWithParquetMrDecoder(defs, 1);
    }

    public void testBulkPathAlternatingPattern() throws IOException {
        // Alternating 0/1 → maximum entropy → guaranteed bit-packed encoding.
        int[] defs = new int[LARGE_LENGTH];
        for (int i = 0; i < defs.length; i++) {
            defs[i] = i & 1;
        }
        assertParityWithParquetMrDecoder(defs, 1);
    }

    public void testBulkPathMisalignedOffsets() throws IOException {
        // Verify the bulk path handles every word-internal alignment. We do this by reading
        // the same encoded stream multiple times into a WordMask, but starting at different
        // offsets within the destination mask. Parquet-mr ground truth is computed once.
        int[] defs = new int[LARGE_LENGTH];
        for (int i = 0; i < defs.length; i++) {
            defs[i] = randomBoolean() ? 1 : 0;
        }
        boolean[] expectedNulls = referenceNulls(defs, 1);

        for (int alignment = 0; alignment < 64; alignment++) {
            DefinitionLevelDecoder decoder = newDecoder(defs, 1);
            int totalBits = alignment + defs.length;
            WordMask mask = new WordMask();
            mask.reset(totalBits);
            int nonNull = decoder.readBatch(defs.length, mask, alignment);

            int expectedNonNull = 0;
            for (boolean isNull : expectedNulls) {
                if (isNull == false) {
                    expectedNonNull++;
                }
            }
            assertEquals("alignment=" + alignment, expectedNonNull, nonNull);
            for (int i = 0; i < defs.length; i++) {
                assertEquals("alignment=" + alignment + " bit=" + (alignment + i), expectedNulls[i], mask.get(alignment + i));
            }
            // bits below the offset must remain zero (we did not touch them)
            for (int i = 0; i < alignment; i++) {
                assertFalse("alignment=" + alignment + " pre-offset bit=" + i, mask.get(i));
            }
        }
    }

    public void testBulkPathSpansMultipleReadBatchCalls() throws IOException {
        // Decode the same large stream in random-sized chunks. The bulk path may be entered
        // multiple times across separate readBatch invocations, with the per-call partial-drain
        // loop kicking in when a previous call ended mid-byte.
        int[] defs = new int[LARGE_LENGTH * 2];
        for (int i = 0; i < defs.length; i++) {
            defs[i] = randomBoolean() ? 1 : 0;
        }
        boolean[] expectedNulls = referenceNulls(defs, 1);

        DefinitionLevelDecoder decoder = newDecoder(defs, 1);
        WordMask mask = new WordMask();
        mask.reset(defs.length);

        int produced = 0;
        int totalNonNull = 0;
        while (produced < defs.length) {
            // Use chunk sizes that often land mid-byte and mid-long to stress all paths.
            int chunk = Math.min(defs.length - produced, randomIntBetween(1, 197));
            totalNonNull += decoder.readBatch(chunk, mask, produced);
            produced += chunk;
        }

        int expectedNonNull = 0;
        for (boolean isNull : expectedNulls) {
            if (isNull == false) {
                expectedNonNull++;
            }
        }
        assertThat(totalNonNull, equalTo(expectedNonNull));
        for (int i = 0; i < defs.length; i++) {
            assertEquals("bit " + i, expectedNulls[i], mask.get(i));
        }
    }

    public void testBulkPathInterleavedRleAndBitPacked() throws IOException {
        // Mixed scenario: long all-present RLE (no-op for the mask; readBatch only touches
        // it for null runs via setRange) interleaved with long bit-packed runs (where the
        // bulk path should dominate) and long all-null RLE runs (which exercise setRange).
        int[] defs = new int[2048];
        // 0..199: all present (RLE; mask is not touched here)
        for (int i = 0; i < 200; i++) {
            defs[i] = 1;
        }
        // 200..599: alternating (bit-packed)
        for (int i = 200; i < 600; i++) {
            defs[i] = i & 1;
        }
        // 600..899: all null (RLE; setRange path)
        for (int i = 600; i < 900; i++) {
            defs[i] = 0;
        }
        // 900..1499: random (probably bit-packed, definitely entering bulk path)
        for (int i = 900; i < 1500; i++) {
            defs[i] = randomBoolean() ? 1 : 0;
        }
        // 1500..end: all present
        for (int i = 1500; i < defs.length; i++) {
            defs[i] = 1;
        }
        assertParityWithParquetMrDecoder(defs, 1);
    }

    public void testBulkPathSkipMatchesReadBatch() throws IOException {
        // The skip variant of the bulk path uses a popcount-only branch (no mask write). Ensure
        // it returns the same non-null count as the read path on the same byte stream.
        int[] defs = new int[LARGE_LENGTH * 3];
        for (int i = 0; i < defs.length; i++) {
            defs[i] = randomBoolean() ? 1 : 0;
        }

        DefinitionLevelDecoder readDecoder = newDecoder(defs, 1);
        WordMask mask = new WordMask();
        mask.reset(defs.length);
        int readNonNull = readDecoder.readBatch(defs.length, mask, 0);

        DefinitionLevelDecoder skipDecoder = newDecoder(defs, 1);
        int skipNonNull = skipDecoder.skip(defs.length);

        assertThat(skipNonNull, equalTo(readNonNull));
    }

    public void testBulkPathSkipInChunksLargeStream() throws IOException {
        // Stress the skip-bulk path across multiple skip calls so the per-call partial-drain
        // path is also exercised in skip().
        int[] defs = new int[LARGE_LENGTH * 4];
        for (int i = 0; i < defs.length; i++) {
            defs[i] = randomBoolean() ? 1 : 0;
        }

        DefinitionLevelDecoder readDecoder = newDecoder(defs, 1);
        WordMask mask = new WordMask();
        mask.reset(defs.length);
        int readNonNull = readDecoder.readBatch(defs.length, mask, 0);

        DefinitionLevelDecoder skipDecoder = newDecoder(defs, 1);
        int totalNonNull = 0;
        int skipped = 0;
        while (skipped < defs.length) {
            int chunk = Math.min(defs.length - skipped, randomIntBetween(1, 251));
            totalNonNull += skipDecoder.skip(chunk);
            skipped += chunk;
        }
        assertThat(totalNonNull, equalTo(readNonNull));
    }

    public void testBulkPathParityWithParquetMrLargeRandom() throws IOException {
        // Heavyweight randomized test: build a large stream with a mix of regimes and decode
        // it with both our decoder and parquet-mr's reference. They must agree bit-by-bit and
        // value-by-value (non-null count).
        int length = randomIntBetween(2000, 8000);
        int[] defs = new int[length];
        int i = 0;
        while (i < length) {
            int span = Math.min(length - i, randomIntBetween(1, 400));
            int mode = randomIntBetween(0, 3);
            switch (mode) {
                case 0 -> {
                    int v = randomBoolean() ? 1 : 0;
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
                        defs[i + j] = randomBoolean() ? 1 : 0;
                    }
                }
            }
            i += span;
        }
        assertParityWithParquetMrDecoder(defs, 1);
        // Belt-and-braces: also verify the encoded stream actually contains a long-enough
        // bit-packed run, so this test cannot silently degrade into "RLE + per-byte loop only"
        // if a future parquet-mr encoder version changes its run-shape choices.
        assertHasBitPackedRunOfAtLeast(defs, 1, 64);
    }

    public void testBulkPathParityAtArbitraryOffsetsRandomized() throws IOException {
        // Combine large random data + non-trivial output offset (not word-aligned) to exercise
        // the misaligned orLongAt path under realistic data.
        for (int trial = 0; trial < 10; trial++) {
            int length = randomIntBetween(LARGE_LENGTH, LARGE_LENGTH * 2);
            int[] defs = new int[length];
            for (int j = 0; j < length; j++) {
                defs[j] = randomBoolean() ? 1 : 0;
            }
            int offset = randomIntBetween(1, 63);

            boolean[] expectedNulls = referenceNulls(defs, 1);
            DefinitionLevelDecoder decoder = newDecoder(defs, 1);
            WordMask mask = new WordMask();
            mask.reset(offset + length);
            int nonNull = decoder.readBatch(length, mask, offset);

            int expectedNonNull = 0;
            for (boolean isNull : expectedNulls) {
                if (isNull == false) {
                    expectedNonNull++;
                }
            }
            assertEquals("trial=" + trial + " offset=" + offset, expectedNonNull, nonNull);
            for (int j = 0; j < length; j++) {
                assertEquals("trial=" + trial + " offset=" + offset + " bit=" + j, expectedNulls[j], mask.get(offset + j));
            }
        }
    }

    public void testParityWithParquetMrForBitWidth3DefaultUnpackBranch() throws IOException {
        // bitWidth=3 (maxDefLevel=7) hits unpack8Values' `default` branch, which routes through
        // the generic readBitsLE bit-shifted readout. Bit widths 4 and 8 are covered by dedicated
        // switch cases (and in DefinitionLevelDecoderTests). This test ties the default branch
        // directly to parquet-mr's wire format by decoding the same bytes with both decoders.
        int[] defs = new int[1024];
        for (int i = 0; i < defs.length; i++) {
            defs[i] = randomIntBetween(0, 7);
        }
        assertParityWithParquetMrDecoder(defs, 7);
        // Guard-rail: random 0..7 values are entropy-rich enough that the encoder will emit
        // at least one bit-packed run, ensuring readBitsLE actually ran.
        assertHasBitPackedRunOfAtLeast(defs, 7, 8);
    }

    public void testBulkPathExercisesSanityForceLongBitPackedRun() throws IOException {
        // Sanity check: confirm the encoder actually emits a bit-packed run >= 64 levels
        // for our high-entropy test data. Otherwise, "bulk path" tests would silently degrade
        // into "RLE path" tests and not exercise what they claim to.
        int[] defs = new int[256];
        for (int i = 0; i < defs.length; i++) {
            defs[i] = i & 1;
        }
        assertHasBitPackedRunOfAtLeast(defs, 1, 64);
    }

    // --- helpers ---

    /**
     * Encode {@code defs} with parquet-mr's encoder, decode the same bytes with both
     * {@link DefinitionLevelDecoder} (our impl) and {@link RunLengthBitPackingHybridDecoder}
     * (parquet-mr reference decoder), and assert the resulting null bitmasks agree at every
     * position. Parquet-mr's decoder is the ground truth — this gives stronger format-fidelity
     * coverage than asserting against the original {@code int[]} alone.
     */
    private void assertParityWithParquetMrDecoder(int[] defs, int maxDefLevel) throws IOException {
        boolean[] expectedNulls = referenceNulls(defs, maxDefLevel);

        DefinitionLevelDecoder ourDecoder = newDecoder(defs, maxDefLevel);
        WordMask mask = new WordMask();
        mask.reset(defs.length);
        int nonNull = ourDecoder.readBatch(defs.length, mask, 0);

        int expectedNonNull = 0;
        for (boolean isNull : expectedNulls) {
            if (isNull == false) {
                expectedNonNull++;
            }
        }
        assertEquals("non-null count must match parquet-mr", expectedNonNull, nonNull);
        for (int i = 0; i < defs.length; i++) {
            assertEquals("bit " + i + " (def=" + defs[i] + ")", expectedNulls[i], mask.get(i));
        }
    }

    /**
     * Walks the encoded RLE/BitPacked-hybrid byte stream produced by parquet-mr for {@code defs}
     * and asserts that at least one bit-packed run is at least {@code minLevels} def levels long.
     *
     * <p>Used as a guard-rail: tests that claim to exercise the bulk path (which only triggers
     * for bit-packed runs of {@literal >=} 64 levels) rely on the encoder choosing a bit-packed
     * encoding for the input. If a future parquet-mr release changes its run-shape heuristics
     * such that our test inputs only produce RLE runs, this assertion fails loudly instead of
     * the bulk path silently going untested.
     */
    private static void assertHasBitPackedRunOfAtLeast(int[] defs, int maxDefLevel, int minLevels) throws IOException {
        int bitWidth = 32 - Integer.numberOfLeadingZeros(maxDefLevel);
        byte[] payload = encode(defs, maxDefLevel);
        ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
        int paddedValueBytes = (bitWidth + 7) >>> 3;
        int maxBitPackedLevels = 0;
        while (buf.hasRemaining()) {
            int header = readVarInt(buf);
            if ((header & 1) == 0) {
                int runLen = header >>> 1;
                if (runLen == 0) {
                    break;
                }
                buf.position(buf.position() + paddedValueBytes);
            } else {
                int numGroups = header >>> 1;
                int levels = numGroups * 8;
                maxBitPackedLevels = Math.max(maxBitPackedLevels, levels);
                buf.position(buf.position() + numGroups * bitWidth);
            }
        }
        assertThat(
            "encoded stream must contain a bit-packed run of >= " + minLevels + " levels to exercise the bulk path",
            maxBitPackedLevels,
            greaterThanOrEqualTo(minLevels)
        );
    }

    /**
     * Decode the encoded byte stream with parquet-mr's reference decoder and return a boolean[]
     * where {@code true} means "null" (def {@literal <} maxDefLevel). This is the ground truth.
     */
    private static boolean[] referenceNulls(int[] defs, int maxDefLevel) throws IOException {
        byte[] payload = encode(defs, maxDefLevel);
        int bitWidth = 32 - Integer.numberOfLeadingZeros(maxDefLevel);
        RunLengthBitPackingHybridDecoder ref = new RunLengthBitPackingHybridDecoder(bitWidth, new ByteArrayInputStream(payload));
        boolean[] nulls = new boolean[defs.length];
        for (int i = 0; i < defs.length; i++) {
            int v = ref.readInt();
            nulls[i] = v < maxDefLevel;
        }
        return nulls;
    }

    private static byte[] encode(int[] defs, int maxDefLevel) throws IOException {
        int bitWidth = 32 - Integer.numberOfLeadingZeros(maxDefLevel);
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
            return encoder.toBytes().toByteArray();
        }
    }

    private static DefinitionLevelDecoder newDecoder(int[] defs, int maxDefLevel) throws IOException {
        byte[] payload = encode(defs, maxDefLevel);
        ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
        DefinitionLevelDecoder decoder = new DefinitionLevelDecoder();
        decoder.init(buf, maxDefLevel, false);
        return decoder;
    }

    private static int readVarInt(ByteBuffer buf) {
        int shift = 0;
        int result = 0;
        while (true) {
            int b = buf.get() & 0xFF;
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
            shift += 7;
        }
    }
}
