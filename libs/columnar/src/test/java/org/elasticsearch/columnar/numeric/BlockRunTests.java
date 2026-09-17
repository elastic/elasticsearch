/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.columnar.substrate.BlockBytesCodec;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

/**
 * A block that repeats the one before it writes no bytes, and the empty extent it leaves is what a reader
 * follows back to the copy. The cases that matter are the ones where a run ends: a block that only looks
 * like a repeat, and the short block at the end of a column.
 */
public class BlockRunTests extends ESTestCase {

    private static final int BLOCK_SIZE = 128;

    public void testAConstantColumnKeepsOneBlock() throws IOException {
        final long[] values = new long[BLOCK_SIZE * 50];
        java.util.Arrays.fill(values, 7L);
        final long bytes = assertRoundTrip(values);
        // One block of a constant value, not fifty: the run writes its first block and nothing after it.
        assertThat("a run cost more than one block", bytes, lessThan(64L));
    }

    public void testBlocksThatDifferAreAllWritten() throws IOException {
        final long[] values = new long[BLOCK_SIZE * 8];
        for (int i = 0; i < values.length; i++) {
            // Constant within a block, different between them: nothing may be deduplicated.
            values[i] = i / BLOCK_SIZE;
        }
        final long bytes = assertRoundTrip(values);
        assertThat("blocks with different values were deduplicated", bytes, greaterThan(8L));
    }

    public void testARunIsReadableFromAnyPointInside() throws IOException {
        // A varied block, a long run, then a varied block: reaching into the middle of the run has to find
        // the copy at its head rather than the offsets either side of it.
        final long[] values = new long[BLOCK_SIZE * 40];
        for (int i = 0; i < values.length; i++) {
            final int block = i / BLOCK_SIZE;
            values[i] = block == 0 || block == 39 ? randomLong() : 99L;
        }
        assertRoundTrip(values);
    }

    public void testARunResumesAfterItIsBroken() throws IOException {
        // The same value either side of a block that is not constant: the second run cannot point back
        // through the block between them, so it writes its own copy.
        final long[] values = new long[BLOCK_SIZE * 6];
        for (int i = 0; i < values.length; i++) {
            final int block = i / BLOCK_SIZE;
            values[i] = block == 3 ? randomLong() : 5L;
        }
        assertRoundTrip(values);
    }

    public void testAShortFinalBlockCanRepeat() throws IOException {
        final long[] values = new long[BLOCK_SIZE * 3 + 17];
        java.util.Arrays.fill(values, -4L);
        assertRoundTrip(values);
    }

    public void testASingleShortBlock() throws IOException {
        final long[] values = new long[13];
        java.util.Arrays.fill(values, 3L);
        assertRoundTrip(values);
    }

    /** Writes the values, reads every one of them back, and answers how many bytes the blocks took. */
    private long assertRoundTrip(long[] values) throws IOException {
        try (Directory directory = newDirectory()) {
            final LongBlocks.Metadata metadata;
            try (IndexOutput out = directory.createOutput("c.bin", IOContext.DEFAULT)) {
                try (
                    LongBlocks.Writer writer = LongBlocks.Writer.into(
                        NumericPipeline.defaultPipeline(BLOCK_SIZE),
                        BlockBytesCodec.forId(BlockBytesCodec.IDENTITY_ID),
                        values.length,
                        directory,
                        IOContext.DEFAULT,
                        "run",
                        out
                    )
                ) {
                    for (long value : values) {
                        writer.add(value);
                    }
                    metadata = writer.finish(out);
                }
            }
            try (IndexInput in = directory.openInput("c.bin", IOContext.DEFAULT)) {
                final LongBlocks.Reader reader = new LongBlocks.Reader(metadata, in);
                // Ascending, then a shuffled pass, because reaching into a run out of order is what the
                // single-block cache does not answer.
                assertValues(reader, values, ascending(values.length));
                assertValues(reader, values, shuffled(values.length));
            }
            return metadata.blockOffsets().dataOffset();
        }
    }

    private static void assertValues(LongBlocks.Reader reader, long[] values, int[] order) throws IOException {
        for (int index : order) {
            final long[] block = reader.block(index / BLOCK_SIZE);
            assertEquals("value at " + index, values[index], block[index % BLOCK_SIZE]);
        }
    }

    private static int[] ascending(int length) {
        final int[] order = new int[length];
        for (int i = 0; i < length; i++) {
            order[i] = i;
        }
        return order;
    }

    private int[] shuffled(int length) {
        final int[] order = ascending(length);
        for (int i = length - 1; i > 0; i--) {
            final int j = randomIntBetween(0, i);
            final int swap = order[i];
            order[i] = order[j];
            order[j] = swap;
        }
        return order;
    }
}
