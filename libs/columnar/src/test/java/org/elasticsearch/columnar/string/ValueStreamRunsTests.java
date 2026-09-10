/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.substrate.ChunkCodec;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.lessThan;

/**
 * The block form that stores a run of equal values once. Every value still has to read back as itself,
 * whichever form its block took, so each case is checked against the values it was given.
 */
public class ValueStreamRunsTests extends ESTestCase {

    private static final String FILE = "runs.bin";

    /** Long runs, as a column sorted on this field holds. */
    public void testLongRuns() throws IOException {
        assertRoundTrip(runs(200, 40));
    }

    /** Runs shorter than a block, so a block spans several of them. */
    public void testShortRuns() throws IOException {
        assertRoundTrip(runs(400, 3));
    }

    /** One run covering everything, so a block is a single value and a repeat. */
    public void testOneRun() throws IOException {
        assertRoundTrip(runs(1, 1000));
    }

    /** A run longer than a block, so it continues across block boundaries. */
    public void testRunSpansBlocks() throws IOException {
        assertRoundTrip(runs(2, 700));
    }

    /** Nothing repeats, so the run form is larger and must not be chosen. */
    public void testDistinctValuesDoNotUseRuns() throws IOException {
        final List<BytesRef> values = new ArrayList<>();
        for (int i = 0; i < 600; i++) {
            values.add(new BytesRef("id-" + i + "-" + randomAlphaOfLength(8)));
        }
        assertRoundTrip(values);
    }

    /** Values of no bytes repeat like any other. */
    public void testRunsOfEmptyValues() throws IOException {
        final List<BytesRef> values = new ArrayList<>();
        for (int i = 0; i < 600; i++) {
            values.add(new BytesRef(i % 100 < 50 ? "" : "x"));
        }
        assertRoundTrip(values);
    }

    /** Runs of values long enough that a block would otherwise pack its lengths. */
    public void testRunsOfLongValues() throws IOException {
        final List<BytesRef> values = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            values.add(new BytesRef(("v" + (i / 25)).repeat(30)));
        }
        assertRoundTrip(values);
    }

    /**
     * Runs of values too long for their length to fit in one byte, so the run header carries a length over
     * two bytes and a reader has to walk it as far as it goes.
     */
    public void testRunsOfValuesLongerThanAByteOfLength() throws IOException {
        final List<BytesRef> values = new ArrayList<>();
        for (int i = 0; i < 400; i++) {
            // Past 127 bytes, so the length takes two, and past 16383 for a few, so one takes three.
            final int repeats = i < 380 ? 40 : 4000;
            values.add(new BytesRef(("value-" + (i / 20) + "-").repeat(repeats / 8)));
        }
        assertRoundTrip(values);
    }

    /**
     * The run form is chosen, not merely tolerated. Uncompressed, so what is measured is the layout rather
     * than what a compressor would have found anyway: a column of runs stores each value once, so it is a
     * fraction of the same values written out one by one.
     */
    public void testRunsAreActuallyChosen() throws IOException {
        final int distinct = 200;
        final int repeat = 40;
        final List<BytesRef> repeated = runs(distinct, repeat);
        final List<BytesRef> allDistinct = new ArrayList<>();
        for (int i = 0; i < repeated.size(); i++) {
            allDistinct.add(new BytesRef("host-" + i + ".eu-west-1.internal"));
        }
        final long runsBytes = write(repeated, ChunkCodec.IDENTITY);
        final long inlineBytes = write(allDistinct, ChunkCodec.IDENTITY);
        assertThat("a run of " + repeat + " equal values should store one copy, not " + repeat, runsBytes * 10, lessThan(inlineBytes));
    }

    private long write(List<BytesRef> values, ChunkCodec codec) throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexOutput out = dir.createOutput(FILE, IOContext.DEFAULT)) {
                try (
                    ValueStream.Writer writer = new ValueStream.Writer(
                        codec,
                        65536,
                        128,
                        values.size(),
                        dir,
                        IOContext.DEFAULT,
                        "runs",
                        out
                    )
                ) {
                    for (BytesRef value : values) {
                        writer.add(value);
                    }
                    writer.finish();
                }
            }
            return dir.fileLength(FILE);
        }
    }

    private List<BytesRef> runs(int distinct, int repeat) {
        final List<BytesRef> values = new ArrayList<>();
        for (int d = 0; d < distinct; d++) {
            final BytesRef value = new BytesRef("host-" + d + ".eu-west-1.internal");
            for (int r = 0; r < repeat; r++) {
                values.add(value);
            }
        }
        return values;
    }

    private void assertRoundTrip(List<BytesRef> values) throws IOException {
        for (int perBlock : new int[] { 8, 128, 512 }) {
            try (Directory dir = newDirectory()) {
                final ValueStream.Metadata metadata;
                try (IndexOutput out = dir.createOutput(FILE, IOContext.DEFAULT)) {
                    try (
                        ValueStream.Writer writer = new ValueStream.Writer(
                            randomFrom(ChunkCodec.IDENTITY, ChunkCodec.ZSTD),
                            randomFrom(64, 4096, 65536),
                            perBlock,
                            values.size(),
                            dir,
                            IOContext.DEFAULT,
                            "runs",
                            out
                        )
                    ) {
                        for (BytesRef value : values) {
                            writer.add(value);
                        }
                        metadata = writer.finish();
                    }
                }
                final String label = "perBlock=" + perBlock + " n=" + values.size();
                assertEquals(label + " numValues", values.size(), metadata.numValues());
                try (IndexInput in = dir.openInput(FILE, IOContext.DEFAULT)) {
                    final ValueStream.Reader reader = metadata.open(in);
                    final BytesRef read = new BytesRef();
                    for (int i = 0; i < values.size(); i++) {
                        reader.get(i, read);
                        assertEquals(label + " in order at " + i, values.get(i), read);
                    }
                    for (int i = values.size() - 1; i >= 0; i--) {
                        reader.get(i, read);
                        assertEquals(label + " backwards at " + i, values.get(i), read);
                    }
                    for (int probe = 0; probe < 200; probe++) {
                        final int i = between(0, values.size() - 1);
                        reader.get(i, read);
                        assertEquals(label + " at random " + i, values.get(i), read);
                    }
                }
            }
        }
    }
}
