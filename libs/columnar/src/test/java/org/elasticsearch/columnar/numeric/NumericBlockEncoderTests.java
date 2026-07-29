/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;

/**
 * Round-trips {@link NumericBlockEncoder} over
 * full blocks that exercise its delta / offset / GCD / bit-pack paths and value extremes.
 */
public class NumericBlockEncoderTests extends ESTestCase {

    private static final int BLOCK = 128;

    public void testConstant() throws IOException {
        assertRoundTrip(filled(0L));
        assertRoundTrip(filled(42L));
        assertRoundTrip(filled(-7L));
    }

    public void testMonotonicAscending() throws IOException {
        long[] block = new long[BLOCK];
        long v = randomLong() >> 2;
        for (int i = 0; i < BLOCK; i++) {
            block[i] = v;
            v += between(0, 1000);
        }
        assertRoundTrip(block);
    }

    public void testMonotonicDescending() throws IOException {
        long[] block = new long[BLOCK];
        long v = randomLong() >> 2;
        for (int i = 0; i < BLOCK; i++) {
            block[i] = v;
            v -= between(0, 1000);
        }
        assertRoundTrip(block);
    }

    public void testGcdFriendly() throws IOException {
        long[] block = new long[BLOCK];
        for (int i = 0; i < BLOCK; i++) {
            block[i] = 1000L * between(-500, 500);
        }
        assertRoundTrip(block);
    }

    public void testGaugeAroundLargeBase() throws IOException {
        long[] block = new long[BLOCK];
        for (int i = 0; i < BLOCK; i++) {
            block[i] = 1_000_000_000_000L + between(0, 255);
        }
        assertRoundTrip(block);
    }

    public void testRandomAndExtremes() throws IOException {
        for (int iter = 0; iter < 50; iter++) {
            long[] block = new long[BLOCK];
            for (int i = 0; i < BLOCK; i++) {
                block[i] = switch (between(0, 3)) {
                    case 0 -> randomLong();
                    case 1 -> between(-1000, 1000);
                    case 2 -> Long.MIN_VALUE;
                    default -> Long.MAX_VALUE;
                };
            }
            assertRoundTrip(block);
        }
    }

    public void testTsdbTimestamps() throws IOException {
        // A realistic @timestamp series: millisecond-precision, strictly ascending with small jitter in
        // the inter-arrival gap. Delta compression fires, so the block must round-trip exactly and encode
        // far below the raw 1024 bytes (128 * 8).
        long[] block = new long[BLOCK];
        long ts = 1_700_000_000_000L; // ~2023-11 in epoch millis
        for (int i = 0; i < BLOCK; i++) {
            ts += between(1, 50); // small positive jitter keeps the series monotonic
            block[i] = ts;
        }
        NumericBlockEncoder encoder = new NumericBlockEncoder(NumericPipeline.defaultPipeline(BLOCK), BLOCK);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        encoder.encode(block.clone(), block.length, out);
        assertTrue("delta-compressed timestamps must be far below raw: " + out.size(), out.size() < BLOCK * 8L / 2);

        long[] decoded = new long[BLOCK];
        encoder.decode(new ByteArrayDataInput(out.toArrayCopy()), block.length, decoded);
        assertArrayEquals(block, decoded);
    }

    public void testPartialBlockRoundTrips() throws IOException {
        // A partial last block: only the first valueCount entries are real; the rest of the buffer is
        // stale. Every stage must fit and round-trip the real values without the caller padding.
        for (int valueCount : new int[] { 1, 2, 3, 7, 63, BLOCK - 1 }) {
            long[] real = new long[valueCount];
            long ts = 1_700_000_000_000L;
            for (int i = 0; i < valueCount; i++) {
                ts += between(1, 50); // monotonic so delta/offset/gcd all engage
                real[i] = ts;
            }
            long[] buffer = new long[BLOCK];
            System.arraycopy(real, 0, buffer, 0, valueCount);
            for (int i = valueCount; i < BLOCK; i++) {
                buffer[i] = randomLong(); // stale tail the encoder must ignore
            }
            NumericBlockEncoder encoder = new NumericBlockEncoder(NumericPipeline.defaultPipeline(BLOCK), BLOCK);
            ByteBuffersDataOutput out = new ByteBuffersDataOutput();
            encoder.encode(buffer, valueCount, out);

            long[] decoded = new long[BLOCK];
            encoder.decode(new ByteArrayDataInput(out.toArrayCopy()), valueCount, decoded);
            for (int i = 0; i < valueCount; i++) {
                assertEquals("value " + i + " of " + valueCount, real[i], decoded[i]);
            }
        }
    }

    private static long[] filled(long value) {
        long[] block = new long[BLOCK];
        Arrays.fill(block, value);
        return block;
    }

    public void testDefaultPipelineIds() {
        NumericPipeline pipeline = NumericPipeline.defaultPipeline(BLOCK);
        assertEquals(ForTerminal.ID, pipeline.terminalId());
        assertArrayEquals(new byte[] { DeltaTransform.ID, OffsetTransform.ID, GcdTransform.ID }, pipeline.transformIds());
    }

    public void testRebuiltPipelineRoundTrips() throws IOException {
        // A reader rebuilds the pipeline from the recorded ids and decodes the same bytes.
        long[] block = new long[BLOCK];
        long v = randomLong() >> 2;
        for (int i = 0; i < BLOCK; i++) {
            block[i] = v;
            v += between(0, 1000);
        }
        NumericPipeline writePipeline = NumericPipeline.defaultPipeline(BLOCK);
        NumericBlockEncoder writer = new NumericBlockEncoder(writePipeline, BLOCK);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        writer.encode(block.clone(), BLOCK, out);

        NumericPipeline readPipeline = NumericPipeline.Registry.rebuild(writePipeline.terminalId(), writePipeline.transformIds(), BLOCK);
        long[] decoded = new long[BLOCK];
        new NumericBlockEncoder(readPipeline, BLOCK).decode(new ByteArrayDataInput(out.toArrayCopy()), BLOCK, decoded);
        assertArrayEquals(block, decoded);
    }

    public void testNonDefaultPipelinesRoundTrip() throws IOException {
        // Prove the additive contract: an arbitrary pipeline (fewer, reordered stages) is rebuilt from
        // its recorded ids and decodes correctly — the same path a newer reader takes for old data.
        long[] base = new long[BLOCK];
        for (int i = 0; i < BLOCK; i++) {
            base[i] = between(-5000, 5000);
        }
        NumericPipeline[] pipelines = {
            new NumericPipeline(new BlockTransform[] {}, new ForTerminal(BLOCK)),
            new NumericPipeline(new BlockTransform[] { new GcdTransform() }, new ForTerminal(BLOCK)),
            new NumericPipeline(new BlockTransform[] { new OffsetTransform(), new DeltaTransform() }, new ForTerminal(BLOCK)), };
        for (NumericPipeline write : pipelines) {
            ByteBuffersDataOutput out = new ByteBuffersDataOutput();
            new NumericBlockEncoder(write, BLOCK).encode(base.clone(), BLOCK, out);
            NumericPipeline read = NumericPipeline.Registry.rebuild(write.terminalId(), write.transformIds(), BLOCK);
            long[] decoded = new long[BLOCK];
            new NumericBlockEncoder(read, BLOCK).decode(new ByteArrayDataInput(out.toArrayCopy()), BLOCK, decoded);
            assertArrayEquals(base, decoded);
        }
    }

    private void assertRoundTrip(long[] block) throws IOException {
        NumericBlockEncoder encoder = new NumericBlockEncoder(NumericPipeline.defaultPipeline(BLOCK), BLOCK);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        encoder.encode(block.clone(), block.length, out);

        long[] decoded = new long[BLOCK];
        encoder.decode(new ByteArrayDataInput(out.toArrayCopy()), block.length, decoded);
        assertArrayEquals(block, decoded);
    }
}
