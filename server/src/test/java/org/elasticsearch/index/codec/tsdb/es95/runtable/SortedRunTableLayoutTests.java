/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95.runtable;

import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.hamcrest.Matchers.equalTo;

/**
 * Round trips a per-doc ordinal stream through {@link RunTableSortedOrdinalWriter},
 * {@link SortedRunTableLayout#encode}, {@link SortedRunTableLayout#readMeta} and
 * {@link SortedRunTableLayout#open}, and checks every way the resulting doc values can be iterated.
 */
public class SortedRunTableLayoutTests extends ESTestCase {

    private static final int MISSING = -1;

    public void testRoundTripDense() throws IOException {
        assertRoundTrip(randomRuns(randomIntBetween(1, 5000), randomIntBetween(1, 100), false));
    }

    public void testRoundTripSparse() throws IOException {
        assertRoundTrip(randomRuns(randomIntBetween(1, 5000), randomIntBetween(1, 100), true));
    }

    public void testRoundTripSingleRun() throws IOException {
        final int[] ords = new int[randomIntBetween(1, 1000)];
        Arrays.fill(ords, randomIntBetween(0, 9));
        assertRoundTrip(new Stream(ords, 10));
    }

    public void testRoundTripSentinelAtBothEnds() throws IOException {
        // no value, then two runs, then no value again: exercises skipping a leading and a trailing sentinel run
        final int[] ords = { MISSING, MISSING, 0, 0, 0, 1, 1, MISSING, MISSING, MISSING };
        assertRoundTrip(new Stream(ords, 2));
    }

    public void testRoundTripOneRunPerDoc() throws IOException {
        // the layout itself has no size bar; that is the gate's job
        final int[] ords = new int[randomIntBetween(1, 500)];
        for (int doc = 0; doc < ords.length; doc++) {
            ords[doc] = doc % 2;
        }
        assertRoundTrip(new Stream(ords, 2));
    }

    public void testWriterAssertsOrdinalsInRange() {
        final int valueCount = randomIntBetween(1, 100);
        final RunTableSortedOrdinalWriter writer = new RunTableSortedOrdinalWriter(valueCount);
        expectThrows(AssertionError.class, () -> writer.add(-1));
        expectThrows(AssertionError.class, () -> writer.add(valueCount + 1));
        writer.add(valueCount); // the sentinel is accepted
        assertThat(writer.numRuns(), equalTo(1));
    }

    public void testWriterRejectsNonPositiveValueCount() {
        expectThrows(IllegalArgumentException.class, () -> new RunTableSortedOrdinalWriter(randomIntBetween(-10, 0)));
    }

    /** A per-doc ordinal stream, {@link #MISSING} marking a doc without a value, and the field's value count. */
    private record Stream(int[] ords, int valueCount) {}

    private static Stream randomRuns(int maxDoc, int valueCount, boolean sparse) {
        final int[] ords = new int[maxDoc];
        int doc = 0;
        while (doc < maxDoc) {
            final int runLength = Math.min(maxDoc - doc, randomIntBetween(1, 200));
            final int ord = sparse && randomBoolean() ? MISSING : randomIntBetween(0, valueCount - 1);
            Arrays.fill(ords, doc, doc + runLength, ord);
            doc += runLength;
        }
        return new Stream(ords, valueCount);
    }

    private static void assertRoundTrip(final Stream stream) throws IOException {
        final int[] ords = stream.ords();
        final int maxDoc = ords.length;
        final RunTableSortedOrdinalWriter writer = new RunTableSortedOrdinalWriter(stream.valueCount());
        int numDocsWithField = 0;
        for (int ord : ords) {
            writer.add(ord == MISSING ? stream.valueCount() : ord);
            if (ord != MISSING) {
                numDocsWithField++;
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
            SortedRunTableLayout.encode(writer, numDocsWithField, data, meta);
        }

        final SortedRunTableLayout.Meta meta;
        try (IndexInput metaIn = new ByteArrayIndexInput("meta", metaBytes.toArrayCopy())) {
            meta = SortedRunTableLayout.readMeta(metaIn);
            assertThat("metadata stream not fully consumed", metaIn.getFilePointer(), equalTo(metaIn.length()));
        }
        assertThat(meta.numDocsWithField(), equalTo(numDocsWithField));
        assertThat(meta.numRuns(), equalTo(writer.numRuns()));
        assertThat(meta.valueCount(), equalTo(stream.valueCount()));
        assertThat(meta.dataStart(), equalTo((long) dataPrefix));

        try (IndexInput data = new ByteArrayIndexInput("data", dataBytes.toArrayCopy())) {
            final NumericDocValues iterated = SortedRunTableLayout.open(meta, data, maxDoc);
            int expectedDoc = nextDocWithValue(ords, 0);
            for (int doc = iterated.nextDoc(); doc != NO_MORE_DOCS; doc = iterated.nextDoc()) {
                assertThat(doc, equalTo(expectedDoc));
                assertThat(iterated.docID(), equalTo(doc));
                assertThat(iterated.longValue(), equalTo((long) ords[doc]));
                expectedDoc = nextDocWithValue(ords, doc + 1);
            }
            assertThat(expectedDoc, equalTo(NO_MORE_DOCS));
            assertThat(iterated.docID(), equalTo(NO_MORE_DOCS));

            final NumericDocValues exact = SortedRunTableLayout.open(meta, data, maxDoc);
            for (int doc = randomIntBetween(0, 3); doc < maxDoc; doc += randomIntBetween(1, 20)) {
                assertThat("doc " + doc, exact.advanceExact(doc), equalTo(ords[doc] != MISSING));
                if (ords[doc] != MISSING) {
                    assertThat("doc " + doc, exact.longValue(), equalTo((long) ords[doc]));
                }
            }

            final NumericDocValues advanced = SortedRunTableLayout.open(meta, data, maxDoc);
            int target = randomIntBetween(0, 3);
            while (true) {
                final int doc = advanced.advance(target);
                assertThat("target " + target, doc, equalTo(nextDocWithValue(ords, target)));
                if (doc == NO_MORE_DOCS) {
                    break;
                }
                assertThat("doc " + doc, advanced.longValue(), equalTo((long) ords[doc]));
                target = doc + randomIntBetween(1, 300);
                if (target >= maxDoc) {
                    assertThat(advanced.advance(target), equalTo(NO_MORE_DOCS));
                    break;
                }
            }
        }
    }

    private static int nextDocWithValue(final int[] ords, int from) {
        for (int doc = from; doc < ords.length; doc++) {
            if (ords[doc] != MISSING) {
                return doc;
            }
        }
        return NO_MORE_DOCS;
    }
}
