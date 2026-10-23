/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests the {@link OrdinalRunsDocValues.OrdinalRuns} a secondary index sort field records when each of its values maps to a single
 * value of the primary sort field, so that its documents form one contiguous run per ordinal (see {@link OrdinalRunsWriter}).
 */
public abstract class AbstractOrdinalRunsTests extends ESTestCase {

    private static final String PRIMARY = "group";
    private static final String SECONDARY = "series";

    protected abstract Codec getCodec();

    /** A document of the test index: its primary and secondary sort values. */
    private record Doc(BytesRef group, BytesRef series) {}

    private void runTest(List<Doc> docs, boolean sortedSet, CheckedConsumer<LeafReader, IOException> assertions) throws IOException {
        SortField secondarySortField = sortedSet
            ? new SortedSetSortField(SECONDARY, false)
            : new SortField(SECONDARY, SortField.Type.STRING);
        IndexWriterConfig config = new IndexWriterConfig().setCodec(getCodec())
            .setIndexSort(new Sort(new SortField(PRIMARY, SortField.Type.STRING), secondarySortField))
            .setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH)
            .setRAMBufferSizeMB(100);
        try (Directory directory = newDirectory(); IndexWriter writer = new IndexWriter(directory, config)) {
            int flushAt = docs.size() > 1 && randomBoolean() ? between(1, docs.size() - 1) : -1;
            for (int i = 0; i < docs.size(); i++) {
                Doc doc = docs.get(i);
                Document document = new Document();
                document.add(new SortedDocValuesField(PRIMARY, doc.group()));
                document.add(
                    sortedSet ? new SortedSetDocValuesField(SECONDARY, doc.series()) : new SortedDocValuesField(SECONDARY, doc.series())
                );
                writer.addDocument(document);
                if (i == flushAt) {
                    // The runs survive a merge, which rewrites the field from the merged doc values
                    writer.flush();
                }
            }
            if (flushAt != -1) {
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                assertThat(reader.leaves(), hasSize(1));
                assertions.accept(reader.leaves().get(0).reader());
            }
        }
    }

    private static SortedDocValues sortedDocValues(LeafReader leaf, String field) throws IOException {
        SortedDocValues sorted = leaf.getSortedDocValues(field);
        if (sorted == null) {
            sorted = DocValues.unwrapSingleton(leaf.getSortedSetDocValues(field));
        }
        assertThat(sorted, notNullValue());
        assertThat(sorted, instanceOf(OrdinalRunsDocValues.class));
        return sorted;
    }

    private static OrdinalRunsDocValues.OrdinalRuns ordinalRuns(LeafReader leaf, String field) throws IOException {
        return ((OrdinalRunsDocValues) sortedDocValues(leaf, field)).ordinalRuns();
    }

    /** Asserts the runs against the actual document order of the segment: the first run of each ordinal. */
    private static void assertRuns(LeafReader leaf) throws IOException {
        SortedDocValues series = sortedDocValues(leaf, SECONDARY);
        int[] expectedFirstDoc = new int[series.getValueCount()];
        int[] expectedEndDoc = new int[series.getValueCount()];
        Arrays.fill(expectedFirstDoc, -1);
        int lastOrd = -1;
        for (int doc = series.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = series.nextDoc()) {
            int ord = series.ordValue();
            if (ord != lastOrd) {
                if (lastOrd != -1 && expectedEndDoc[lastOrd] == 0) {
                    expectedEndDoc[lastOrd] = doc;
                }
                if (expectedFirstDoc[ord] == -1) {
                    expectedFirstDoc[ord] = doc;
                }
                lastOrd = ord;
            }
        }
        if (expectedEndDoc[lastOrd] == 0) {
            expectedEndDoc[lastOrd] = leaf.maxDoc();
        }

        OrdinalRunsDocValues.OrdinalRuns runs = ordinalRuns(leaf, SECONDARY);
        assertThat(runs, notNullValue());
        for (int ord = 0; ord < series.getValueCount(); ord++) {
            assertThat("first doc of ord " + ord, runs.firstDoc(ord), equalTo(expectedFirstDoc[ord]));
            assertThat("end doc of ord " + ord, runs.endDoc(ord), equalTo(expectedEndDoc[ord]));
        }
    }

    public void testSingleSeries() throws Exception {
        int numDocs = between(1, 100);
        List<Doc> docs = new ArrayList<>(numDocs);
        for (int i = 0; i < numDocs; i++) {
            docs.add(new Doc(new BytesRef("host-1"), new BytesRef("series-1")));
        }
        runTest(docs, randomBoolean(), leaf -> {
            OrdinalRunsDocValues.OrdinalRuns runs = ordinalRuns(leaf, SECONDARY);
            assertThat(runs, notNullValue());
            assertThat(runs.firstDoc(0), equalTo(0));
            assertThat(runs.endDoc(0), equalTo(numDocs));
            assertRuns(leaf);
        });
    }

    /**
     * Series belong to one host each, so sorting on the host first keeps the documents of a series together, but the series are
     * not in order: the series of host 1 sort ahead of those of host 0.
     */
    public void testSeriesGroupedByHost() throws Exception {
        List<Doc> docs = new ArrayList<>();
        int numSeries = between(2, 500);
        for (int series = 0; series < numSeries; series++) {
            BytesRef host = new BytesRef("host-" + (series % 2 == 0 ? 1 : 0));
            BytesRef seriesValue = new BytesRef(String.format(java.util.Locale.ROOT, "series-%05d", series));
            int numDocs = between(1, 5);
            for (int d = 0; d < numDocs; d++) {
                docs.add(new Doc(host, seriesValue));
            }
        }
        Randomness.shuffle(docs);
        runTest(docs, randomBoolean(), leaf -> {
            assertRuns(leaf);
            // The ordinals of the series are not in document order
            SortedDocValues series = sortedDocValues(leaf, SECONDARY);
            assertTrue(series.advanceExact(0));
            assertThat(series.ordValue(), equalTo(1));
        });
    }

    public void testRandom() throws Exception {
        int numHosts = between(1, 10);
        int numSeries = between(1, 2_000);
        List<Doc> docs = new ArrayList<>();
        for (int series = 0; series < numSeries; series++) {
            BytesRef host = new BytesRef("host-" + between(0, numHosts - 1));
            BytesRef seriesValue = new BytesRef(randomAlphaOfLengthBetween(1, 20) + "-" + series);
            int numDocs = between(1, 10);
            for (int d = 0; d < numDocs; d++) {
                docs.add(new Doc(host, seriesValue));
            }
        }
        Randomness.shuffle(docs);
        runTest(docs, randomBoolean(), AbstractOrdinalRunsTests::assertRuns);
    }

    /**
     * A series reported by two hosts has two runs; only the first is recorded and the documents of the second are strays. With a
     * single series the two runs are adjacent and merge into one, as the writer only sees the ordinals: that is what the delete
     * tombstones of the last live series do under the grouped time series sort, and readers clip that case (see
     * {@code TSDBSyntheticIdDocValuesHolder#liveRunsEnd}).
     */
    public void testStraysWhenSeriesSpanHosts() throws Exception {
        List<Doc> docs = new ArrayList<>();
        int numSeries = between(2, 100);
        for (int series = 0; series < numSeries; series++) {
            BytesRef seriesValue = new BytesRef(String.format(java.util.Locale.ROOT, "series-%05d", series));
            docs.add(new Doc(new BytesRef("host-0"), seriesValue));
            docs.add(new Doc(new BytesRef("host-1"), seriesValue));
        }
        Randomness.shuffle(docs);
        runTest(docs, randomBoolean(), leaf -> {
            assertRuns(leaf);
            OrdinalRunsDocValues.OrdinalRuns runs = ordinalRuns(leaf, SECONDARY);
            for (int ord = 0; ord < numSeries; ord++) {
                // host-0 sorts first, with one document per series
                assertThat(runs.firstDoc(ord), equalTo(ord));
                assertThat(runs.endDoc(ord), equalTo(ord + 1));
            }
        });
    }

    public void testNoRunsForPrimarySortField() throws Exception {
        List<Doc> docs = new ArrayList<>();
        int numSeries = between(1, 100);
        for (int series = 0; series < numSeries; series++) {
            docs.add(new Doc(new BytesRef("host-" + between(0, 3)), new BytesRef("series-" + series)));
        }
        runTest(docs, randomBoolean(), leaf -> assertThat(ordinalRuns(leaf, PRIMARY), nullValue()));
    }
}
