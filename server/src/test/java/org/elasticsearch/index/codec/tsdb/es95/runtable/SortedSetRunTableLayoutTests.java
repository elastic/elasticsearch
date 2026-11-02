/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95.runtable;

import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.IntStream;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.hamcrest.Matchers.equalTo;

/**
 * Round trips per-doc ordinal sets through {@link RunTableSortedSetOrdinalWriter},
 * {@link SortedSetRunTableLayout#encode}, {@link SortedSetRunTableLayout#readMeta} and
 * {@link SortedSetRunTableLayout#open}, and checks every way the resulting doc values can be iterated.
 */
public class SortedSetRunTableLayoutTests extends ESTestCase {

    private static final int[] EMPTY = new int[0];

    public void testRoundTripDense() throws IOException {
        assertRoundTrip(randomRuns(randomIntBetween(1, 5000), randomIntBetween(2, 100), false), 100);
    }

    public void testRoundTripSparse() throws IOException {
        assertRoundTrip(randomRuns(randomIntBetween(1, 5000), randomIntBetween(2, 100), true), 100);
    }

    public void testRoundTripSingleRun() throws IOException {
        final int[][] sets = new int[randomIntBetween(1, 1000)][];
        Arrays.fill(sets, new int[] { 1, 3, 4 });
        assertRoundTrip(sets, 5);
    }

    public void testRoundTripEmptyRunsAtBothEnds() throws IOException {
        // no value, then two runs, then no value again: exercises skipping a leading and a trailing empty run
        final int[] a = { 0, 2 };
        final int[] b = { 1 };
        final int[][] sets = { EMPTY, EMPTY, a, a, a, b, b, EMPTY, EMPTY, EMPTY };
        assertRoundTrip(sets, 3);
    }

    public void testRoundTripOneRunPerDoc() throws IOException {
        // the layout itself has no size bar; that is the gate's job
        final int[][] sets = new int[randomIntBetween(1, 500)][];
        for (int doc = 0; doc < sets.length; doc++) {
            sets[doc] = doc % 2 == 0 ? new int[] { 0, 1 } : new int[] { 1 };
        }
        assertRoundTrip(sets, 2);
    }

    public void testWriterAssertsValidSets() {
        final int valueCount = randomIntBetween(2, 100);
        final RunTableSortedSetOrdinalWriter writer = new RunTableSortedSetOrdinalWriter(valueCount);
        expectThrows(AssertionError.class, () -> writer.add(new int[] { -1 }, 1));
        expectThrows(AssertionError.class, () -> writer.add(new int[] { valueCount }, 1));
        expectThrows(AssertionError.class, () -> writer.add(new int[] { 1, 1 }, 2));
        expectThrows(AssertionError.class, () -> writer.add(new int[] { 1, 0 }, 2));
        // only the first count entries are the set
        writer.add(new int[] { 0, 1, -5 }, 2);
        assertThat(writer.numRuns(), equalTo(1));
        writer.add(new int[] { 0, 1 }, 2);
        assertThat("an equal set extends the run", writer.numRuns(), equalTo(1));
        writer.add(new int[] { 0 }, 1);
        assertThat("a prefix of the set is a different set", writer.numRuns(), equalTo(2));
    }

    public void testWriterRejectsNonPositiveValueCount() {
        expectThrows(IllegalArgumentException.class, () -> new RunTableSortedSetOrdinalWriter(randomIntBetween(-10, 0)));
    }

    private static int[][] randomRuns(int maxDoc, int valueCount, boolean sparse) {
        final int[][] sets = new int[maxDoc][];
        int doc = 0;
        while (doc < maxDoc) {
            final int runLength = Math.min(maxDoc - doc, randomIntBetween(1, 200));
            final int[] set = sparse && randomBoolean() ? EMPTY : randomSet(valueCount);
            Arrays.fill(sets, doc, doc + runLength, set);
            doc += runLength;
        }
        return sets;
    }

    private static int[] randomSet(int valueCount) {
        return randomSubsetOf(randomIntBetween(1, Math.min(5, valueCount)), IntStream.range(0, valueCount).boxed().toList()).stream()
            .mapToInt(Integer::intValue)
            .sorted()
            .toArray();
    }

    private static void assertRoundTrip(final int[][] sets, int valueCount) throws IOException {
        final int maxDoc = sets.length;
        final RunTableSortedSetOrdinalWriter writer = new RunTableSortedSetOrdinalWriter(valueCount);
        int numDocsWithField = 0;
        long numValues = 0;
        // a reused buffer longer than the set, as the codec passes it
        final int[] buffer = new int[valueCount + 3];
        for (int[] set : sets) {
            System.arraycopy(set, 0, buffer, 0, set.length);
            writer.add(buffer, set.length);
            if (set.length > 0) {
                numDocsWithField++;
                numValues += set.length;
            }
        }

        final ByteBuffersDataOutput metaBytes = new ByteBuffersDataOutput();
        final ByteBuffersDataOutput dataBytes = new ByteBuffersDataOutput();
        // a non-zero start offset checks that the reader honors the recorded absolute data offset
        final int dataPrefix = randomIntBetween(0, 64);
        try (
            IndexOutput meta = new ByteBuffersIndexOutput(metaBytes, "meta", "meta");
            IndexOutput data = new ByteBuffersIndexOutput(dataBytes, "data", "data")
        ) {
            for (int i = 0; i < dataPrefix; i++) {
                data.writeByte((byte) i);
            }
            SortedSetRunTableLayout.encode(writer, numDocsWithField, numValues, data, meta);
        }

        final SortedSetRunTableLayout.Meta meta;
        try (IndexInput metaIn = new ByteArrayIndexInput("meta", metaBytes.toArrayCopy())) {
            meta = SortedSetRunTableLayout.readMeta(metaIn);
            assertThat("metadata stream not fully consumed", metaIn.getFilePointer(), equalTo(metaIn.length()));
        }
        assertThat(meta.numDocsWithField(), equalTo(numDocsWithField));
        assertThat(meta.numValues(), equalTo(numValues));
        assertThat(meta.numRuns(), equalTo(writer.numRuns()));
        assertThat(meta.valueCount(), equalTo(valueCount));
        assertThat(meta.dataStart(), equalTo((long) dataPrefix));

        try (IndexInput data = new ByteArrayIndexInput("data", dataBytes.toArrayCopy())) {
            final SortedNumericDocValues iterated = SortedSetRunTableLayout.open(meta, data, maxDoc);
            assertThat(iterated.cost(), equalTo((long) numDocsWithField));
            int expectedDoc = nextDocWithValue(sets, 0);
            for (int doc = iterated.nextDoc(); doc != NO_MORE_DOCS; doc = iterated.nextDoc()) {
                assertThat(doc, equalTo(expectedDoc));
                assertThat(iterated.docID(), equalTo(doc));
                assertSet(iterated, sets[doc], doc);
                expectedDoc = nextDocWithValue(sets, doc + 1);
            }
            assertThat(expectedDoc, equalTo(NO_MORE_DOCS));
            assertThat(iterated.nextDoc(), equalTo(NO_MORE_DOCS));

            final SortedNumericDocValues exact = SortedSetRunTableLayout.open(meta, data, maxDoc);
            for (int doc = randomIntBetween(0, 3); doc < maxDoc; doc += randomIntBetween(1, 20)) {
                assertThat("doc " + doc, exact.advanceExact(doc), equalTo(sets[doc].length > 0));
                if (sets[doc].length > 0) {
                    assertSet(exact, sets[doc], doc);
                }
            }

            final SortedNumericDocValues advanced = SortedSetRunTableLayout.open(meta, data, maxDoc);
            int target = randomIntBetween(0, 3);
            while (true) {
                final int doc = advanced.advance(target);
                assertThat("target " + target, doc, equalTo(nextDocWithValue(sets, target)));
                if (doc == NO_MORE_DOCS) {
                    break;
                }
                assertSet(advanced, sets[doc], doc);
                target = doc + randomIntBetween(1, 300);
                if (target >= maxDoc) {
                    assertThat(advanced.advance(target), equalTo(NO_MORE_DOCS));
                    break;
                }
            }
        }
    }

    private static void assertSet(final SortedNumericDocValues values, final int[] expected, int doc) throws IOException {
        assertThat("doc " + doc, values.docValueCount(), equalTo(expected.length));
        for (int ord : expected) {
            assertThat("doc " + doc, values.nextValue(), equalTo((long) ord));
        }
    }

    private static int nextDocWithValue(final int[][] sets, int from) {
        for (int doc = from; doc < sets.length; doc++) {
            if (sets[doc].length > 0) {
                return doc;
            }
        }
        return NO_MORE_DOCS;
    }
}
