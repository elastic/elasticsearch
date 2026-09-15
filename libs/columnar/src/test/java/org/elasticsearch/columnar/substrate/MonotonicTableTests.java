/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.substrate;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.LongValues;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

/**
 * Round-trips the monotonic table that every offset structure in the format is built on — block offsets,
 * value addresses, chunk starts, escape ranks. It is written into a temporary file and read back through a
 * mapped slice, so nothing about it is held on the heap; what this checks is that every entry comes back
 * exactly, across the shapes real offsets take.
 */
public class MonotonicTableTests extends ESTestCase {

    public void testSingleEntry() throws IOException {
        assertRoundTrip(new long[] { 0 });
        assertRoundTrip(new long[] { randomNonNegativeLong() });
    }

    /** Every entry the same, which is what an offset table over empty values looks like. */
    public void testConstant() throws IOException {
        final long[] values = new long[between(2, 5000)];
        final long constant = randomLongBetween(0, 1 << 20);
        java.util.Arrays.fill(values, constant);
        assertRoundTrip(values);
    }

    /** A fixed stride, which is what offsets over fixed-width values look like and packs to nothing. */
    public void testFixedStride() throws IOException {
        for (int stride : new int[] { 1, 7, 128, 65536 }) {
            final long[] values = new long[between(2, 3000)];
            for (int i = 1; i < values.length; i++) {
                values[i] = values[i - 1] + stride;
            }
            assertRoundTrip(values);
        }
    }

    /** Runs of no growth between jumps, which is what offsets over a mix of empty and large values look like. */
    public void testPlateausAndJumps() throws IOException {
        final long[] values = new long[between(100, 4000)];
        for (int i = 1; i < values.length; i++) {
            values[i] = values[i - 1] + (random().nextDouble() < 0.7 ? 0 : between(1, 100_000));
        }
        assertRoundTrip(values);
    }

    /** Offsets past the range of an int, which a column larger than 2GB reaches. */
    public void testBeyondIntRange() throws IOException {
        final long[] values = new long[between(2, 2000)];
        values[0] = (long) Integer.MAX_VALUE + between(1, 1_000_000);
        for (int i = 1; i < values.length; i++) {
            values[i] = values[i - 1] + between(0, 1 << 20);
        }
        assertRoundTrip(values);
    }

    /** More entries than one monotonic block holds, so the table spans several of them. */
    public void testSpanningManyBlocks() throws IOException {
        final long[] values = new long[(1 << MonotonicWriter.BLOCK_SHIFT) * 2 + between(1, 1000)];
        for (int i = 1; i < values.length; i++) {
            values[i] = values[i - 1] + between(0, 40);
        }
        assertRoundTrip(values);
    }

    public void testRandom() throws IOException {
        for (int iteration = 0; iteration < 20; iteration++) {
            final long[] values = new long[between(1, 6000)];
            values[0] = randomBoolean() ? 0 : randomLongBetween(0, 1L << 40);
            for (int i = 1; i < values.length; i++) {
                values[i] = values[i - 1] + randomFrom(0L, 1L, (long) between(0, 1000), randomLongBetween(0, 1 << 20));
            }
            assertRoundTrip(values);
        }
    }

    /** The temporary file the table is staged in must not outlive the write. */
    public void testTemporaryFileRemoved() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexOutput out = dir.createOutput("table.bin", IOContext.DEFAULT)) {
                try (MonotonicWriter writer = new MonotonicWriter(dir, IOContext.DEFAULT, "table", 100)) {
                    for (int i = 0; i < 100; i++) {
                        writer.add(i * 3L);
                    }
                    writer.finish(out);
                }
            }
            for (String file : dir.listAll()) {
                assertFalse("a temporary file was left behind: " + file, file.contains("columnar-monotonic"));
            }
        }
    }

    /** An abandoned write must clean up after itself too. */
    public void testTemporaryFileRemovedWhenUnfinished() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexOutput out = dir.createOutput("table.bin", IOContext.DEFAULT)) {
                try (MonotonicWriter writer = new MonotonicWriter(dir, IOContext.DEFAULT, "table", 100)) {
                    writer.add(0);
                    writer.add(1);
                }
                out.writeByte((byte) 0);
            }
            for (String file : dir.listAll()) {
                assertFalse("a temporary file was left behind: " + file, file.contains("columnar-monotonic"));
            }
        }
    }

    private void assertRoundTrip(long[] values) throws IOException {
        final String label = "entries=" + values.length + " last=" + values[values.length - 1];
        try (Directory dir = newDirectory()) {
            final MonotonicWriter.Table table;
            try (IndexOutput out = dir.createOutput("table.bin", IOContext.DEFAULT)) {
                // A leading byte, so the table does not begin at zero and its recorded offset has to be used.
                out.writeByte((byte) 42);
                try (MonotonicWriter writer = new MonotonicWriter(dir, IOContext.DEFAULT, "table", values.length)) {
                    for (long value : values) {
                        writer.add(value);
                    }
                    table = writer.finish(out);
                }
            }
            try (IndexInput in = dir.openInput("table.bin", IOContext.DEFAULT)) {
                final LongValues read = MonotonicReader.open(in, table.meta(), values.length, table.dataOffset(), table.dataLength());
                for (int i = 0; i < values.length; i++) {
                    assertEquals(label + " at " + i, values[i], read.get(i));
                }
                // Out of order, since the readers a column drives are not sequential.
                for (int probe = 0; probe < Math.min(200, values.length); probe++) {
                    final int i = between(0, values.length - 1);
                    assertEquals(label + " random at " + i, values[i], read.get(i));
                }
            }
        }
    }
}
