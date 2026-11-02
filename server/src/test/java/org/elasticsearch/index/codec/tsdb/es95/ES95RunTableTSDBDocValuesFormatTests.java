/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.index.codec.bwc.Elasticsearch93Lucene104Codec;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesFormatTests;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer;
import org.elasticsearch.index.codec.tsdb.pipeline.FieldContext;
import org.elasticsearch.index.codec.tsdb.pipeline.FieldContextResolver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.elasticsearch.index.codec.tsdb.es95.ES95TSDBDocValuesFormat.NUMERIC_BLOCK_SHIFT;
import static org.elasticsearch.index.codec.tsdb.es95.ES95TSDBDocValuesFormat.NUMERIC_LARGE_BLOCK_SHIFT;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Runs the shared TSDB doc values format suite against {@link ES95RunTableTSDBDocValuesFormat}, with every
 * field reported as a dimension so the run-table layout is attempted for every sorted field. Most of the
 * shared tests index data that is not run-shaped and so cover the fallback path; the tests in this class
 * index series-shaped data that does end up in the run-table layout.
 */
public class ES95RunTableTSDBDocValuesFormatTests extends AbstractTSDBDocValuesFormatTests {

    private static final FieldContextResolver ALL_DIMENSIONS = (name, blockSize) -> new FieldContext(blockSize, name, null, null, true);

    private static final String TSID_FIELD = "tsid";
    private static final String SERIES_FIELD = "series";
    private static final String SEQ_FIELD = "seq";
    private static final String ID_FIELD = "id";

    private final Codec codec = new Elasticsearch93Lucene104Codec() {

        final DocValuesFormat docValuesFormat = new ES95RunTableTSDBDocValuesFormat(
            random().nextBoolean() ? NUMERIC_LARGE_BLOCK_SHIFT : NUMERIC_BLOCK_SHIFT,
            random().nextBoolean(),
            ES95TSDBDocValuesFormat.BINARY_DV_BLOCK_BYTES_THRESHOLD_DEFAULT,
            ES95TSDBDocValuesFormat.BINARY_DV_BLOCK_COUNT_THRESHOLD_DEFAULT,
            ALL_DIMENSIONS
        );

        @Override
        public DocValuesFormat getDocValuesFormatForField(String field) {
            return docValuesFormat;
        }
    };

    @Override
    protected Codec getCodec() {
        return codec;
    }

    /** The expected value of a field for the {@code seq}-th doc of series {@code series}, or {@code null} if it has none. */
    @FunctionalInterface
    private interface ExpectedValue {
        String of(int series, int seq);
    }

    /**
     * Series-shaped data spread over several segments and then force-merged: a dense dimension, a sparse one
     * missing for whole series, a single-valued {@code SortedSet} dimension, and one that changes on every doc
     * of a few series, so the run table, its sentinel runs and the fallback all occur in one segment.
     */
    public void testDimensionsAcrossSegmentsAndMerges() throws IOException {
        final int numSeries = randomIntBetween(1, 30);
        final int numHosts = randomIntBetween(1, numSeries);
        final boolean[] hasRegion = new boolean[numSeries];
        final boolean[] flapping = new boolean[numSeries];
        final List<int[]> docs = new ArrayList<>();
        for (int series = 0; series < numSeries; series++) {
            hasRegion[series] = randomBoolean();
            flapping[series] = rarely();
            final int numDocs = randomIntBetween(1, 300);
            for (int seq = 0; seq < numDocs; seq++) {
                docs.add(new int[] { series, seq });
            }
        }
        Collections.shuffle(docs, random());

        final ExpectedValue host = (series, seq) -> "host-" + (series % numHosts);
        final ExpectedValue region = (series, seq) -> hasRegion[series] ? "region-" + (series % 3) : null;
        final ExpectedValue ip = (series, seq) -> "10.0.0." + (series % 7);
        final ExpectedValue state = (series, seq) -> flapping[series] ? (seq % 2 == 0 ? "up" : "down") : "steady-" + (series % 2);

        try (
            Directory dir = newDirectory();
            IndexWriter writer = new IndexWriter(dir, getTimeSeriesIndexWriterConfig(TSID_FIELD, TIMESTAMP_FIELD))
        ) {
            final int commitEvery = randomIntBetween(1, Math.max(1, docs.size() / 3));
            for (int[] doc : docs) {
                final int series = doc[0];
                final int seq = doc[1];
                final Document document = seriesDocument(series, seq);
                document.add(new SortedDocValuesField("host", new BytesRef(host.of(series, seq))));
                if (region.of(series, seq) != null) {
                    document.add(new SortedDocValuesField("region", new BytesRef(region.of(series, seq))));
                }
                document.add(new SortedSetDocValuesField("ip", new BytesRef(ip.of(series, seq))));
                document.add(new SortedDocValuesField("state", new BytesRef(state.of(series, seq))));
                writer.addDocument(document);
                if (random().nextInt(commitEvery) == 0) {
                    writer.commit();
                }
            }
            writer.commit();

            final CheckedFunction<LeafReader, SortedDocValues, IOException> ipValues = leaf -> {
                final SortedDocValues values = DocValues.unwrapSingleton(DocValues.getSortedSet(leaf, "ip"));
                assertThat("single-valued sorted set unwraps to sorted", values, notNullValue());
                return values;
            };
            for (boolean merged : new boolean[] { false, true }) {
                if (merged) {
                    writer.forceMerge(1);
                }
                try (DirectoryReader reader = DirectoryReader.open(writer)) {
                    if (merged) {
                        assertThat(reader.leaves().size(), equalTo(1));
                    }
                    for (LeafReaderContext context : reader.leaves()) {
                        final LeafReader leaf = context.reader();
                        assertSortedValues(leaf, leafReader -> DocValues.getSorted(leafReader, "host"), host);
                        assertSortedValues(leaf, leafReader -> DocValues.getSorted(leafReader, "region"), region);
                        assertSortedValues(leaf, ipValues, ip);
                        assertSortedValues(leaf, leafReader -> DocValues.getSorted(leafReader, "state"), state);
                    }
                }
            }
        }
    }

    /**
     * A segment with a sparse run-table field merged with a segment whose values change on every doc, so the
     * merged field falls back to the blocked layout. The optimized merge sizes that layout from the value and
     * document counts recorded for each source segment, so the run-table segment must record exact counts.
     * The first segment holds series 0 and 2 and the second series 1, so the merge has to interleave them;
     * Lucene only runs a merge as an index-sorting one, which the optimized merge requires, in that case.
     */
    public void testMergeRunTableSegmentIntoFallbackLayout() throws IOException {
        final int runShapedDocs = randomIntBetween(64, 2000);
        final int flappingDocs = 6 * runShapedDocs;
        // series 0 and 2: "x", then no value, then "y"; series 1: alternates on every doc
        final ExpectedValue dim = (series, seq) -> {
            if (series == 1) {
                return seq % 2 == 0 ? "x" : "y";
            }
            if (seq < runShapedDocs / 2) {
                return "x";
            }
            return seq < 3 * runShapedDocs / 4 ? null : "y";
        };

        try (
            Directory dir = newDirectory();
            IndexWriter writer = new IndexWriter(dir, getTimeSeriesIndexWriterConfig(TSID_FIELD, TIMESTAMP_FIELD))
        ) {
            for (int[] segmentSeries : new int[][] { { 0, 2 }, { 1 } }) {
                for (int series : segmentSeries) {
                    final int numDocs = series == 1 ? flappingDocs : runShapedDocs;
                    for (int seq = 0; seq < numDocs; seq++) {
                        final Document document = seriesDocument(series, seq);
                        final String value = dim.of(series, seq);
                        if (value != null) {
                            document.add(new SortedDocValuesField("dim", new BytesRef(value)));
                        }
                        writer.addDocument(document);
                    }
                }
                writer.commit();
            }
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                assertThat(reader.leaves().size(), equalTo(2));
                for (LeafReaderContext context : reader.leaves()) {
                    final LeafReader leaf = context.reader();
                    final NumericDocValues series = DocValues.getNumeric(leaf, SERIES_FIELD);
                    assertTrue(series.advanceExact(0));
                    final boolean flappingSegment = series.longValue() == 1;
                    assertThat(isRunTable(leaf.getSortedDocValues("dim")), equalTo(flappingSegment == false));
                }
            }

            writer.forceMerge(1);
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                assertThat(reader.leaves().size(), equalTo(1));
                assertSortedValues(reader.leaves().get(0).reader(), leaf -> DocValues.getSorted(leaf, "dim"), dim);
                assertFalse(isRunTable(reader.leaves().get(0).reader().getSortedDocValues("dim")));
            }
        }
    }

    /**
     * Run-table segments read back through the format Lucene looks up by name, which is the no-argument SPI
     * instance rather than the writer's, both before and after a merge. The deleted docs make the merge take
     * the non-optimized path, which remaps the ordinals of the live values before writing them.
     */
    public void testRunTableAfterDeletesAndReopenFromDirectory() throws IOException {
        final int numSeries = randomIntBetween(4, 20);
        final int docsPerSeries = randomIntBetween(50, 200);
        final ExpectedValue host = (series, seq) -> "host-" + (series % 3);
        // only even series have a region, so the merged field has sentinel runs
        final ExpectedValue region = (series, seq) -> series % 2 == 0 ? "region-" + (series % 4) : null;

        try (Directory dir = newDirectory()) {
            int deleted = 0;
            try (IndexWriter writer = new IndexWriter(dir, getTimeSeriesIndexWriterConfig(TSID_FIELD, TIMESTAMP_FIELD))) {
                // even series in the first segment, odd ones in the second, so the merge interleaves them
                for (int parity = 0; parity < 2; parity++) {
                    for (int series = parity; series < numSeries; series += 2) {
                        for (int seq = 0; seq < docsPerSeries; seq++) {
                            final Document document = seriesDocument(series, seq);
                            document.add(new StringField(ID_FIELD, series + "/" + seq, Field.Store.NO));
                            document.add(new SortedDocValuesField("host", new BytesRef(host.of(series, seq))));
                            if (region.of(series, seq) != null) {
                                document.add(new SortedDocValuesField("region", new BytesRef(region.of(series, seq))));
                            }
                            writer.addDocument(document);
                        }
                    }
                    writer.commit();
                }
                for (int series = 0; series < numSeries; series++) {
                    for (int seq = 0; seq < docsPerSeries; seq++) {
                        if (randomIntBetween(0, 9) == 0) {
                            writer.deleteDocuments(new Term(ID_FIELD, series + "/" + seq));
                            deleted++;
                        }
                    }
                }
                writer.commit();
            }

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertThat(reader.leaves().size(), equalTo(2));
                for (LeafReaderContext context : reader.leaves()) {
                    final LeafReader leaf = context.reader();
                    assertTrue("host is run-shaped in every segment", isRunTable(leaf.getSortedDocValues("host")));
                    assertSortedValues(leaf, leafReader -> DocValues.getSorted(leafReader, "host"), host);
                    assertSortedValues(leaf, leafReader -> DocValues.getSorted(leafReader, "region"), region);
                }
            }

            try (IndexWriter writer = new IndexWriter(dir, getTimeSeriesIndexWriterConfig(TSID_FIELD, TIMESTAMP_FIELD))) {
                writer.forceMerge(1);
            }

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertThat(reader.leaves().size(), equalTo(1));
                final LeafReader leaf = reader.leaves().get(0).reader();
                assertThat("the merge dropped the deleted docs", leaf.getLiveDocs(), nullValue());
                assertThat(leaf.maxDoc(), equalTo(numSeries * docsPerSeries - deleted));
                assertTrue(isRunTable(leaf.getSortedDocValues("host")));
                assertTrue(isRunTable(leaf.getSortedDocValues("region")));
                assertSortedValues(leaf, leafReader -> DocValues.getSorted(leafReader, "host"), host);
                assertSortedValues(leaf, leafReader -> DocValues.getSorted(leafReader, "region"), region);
            }
        }
    }

    /** Whether {@code values} were read from the run-table layout. */
    static boolean isRunTable(final SortedDocValues values) {
        return values instanceof AbstractTSDBDocValuesProducer.BaseSortedDocValues base
            && base.ordinalsReaderForTesting() instanceof RunTableSortedCodec.RunTableReader;
    }

    private static Document seriesDocument(int series, int seq) {
        final Document document = new Document();
        document.add(new SortedDocValuesField(TSID_FIELD, new BytesRef(String.format(Locale.ROOT, "series-%04d", series))));
        document.add(new SortedNumericDocValuesField(TIMESTAMP_FIELD, BASE_TIMESTAMP + seq * 1000L));
        document.add(new NumericDocValuesField(SERIES_FIELD, series));
        document.add(new NumericDocValuesField(SEQ_FIELD, seq));
        return document;
    }

    /**
     * Checks the field's values on every doc of {@code leaf} through {@code nextDoc}, {@code advanceExact}
     * and {@code advance}, each on a fresh instance from {@code open}.
     */
    private static void assertSortedValues(
        final LeafReader leaf,
        final CheckedFunction<LeafReader, SortedDocValues, IOException> open,
        final ExpectedValue expectedValue
    ) throws IOException {
        final int maxDoc = leaf.maxDoc();
        final String[] expected = new String[maxDoc];
        final NumericDocValues series = DocValues.getNumeric(leaf, SERIES_FIELD);
        final NumericDocValues seq = DocValues.getNumeric(leaf, SEQ_FIELD);
        for (int doc = 0; doc < maxDoc; doc++) {
            assertTrue(series.advanceExact(doc));
            assertTrue(seq.advanceExact(doc));
            expected[doc] = expectedValue.of((int) series.longValue(), (int) seq.longValue());
        }

        final SortedDocValues iterated = open.apply(leaf);
        int expectedDoc = nextDocWithValue(expected, 0);
        for (int doc = iterated.nextDoc(); doc != NO_MORE_DOCS; doc = iterated.nextDoc()) {
            assertThat(doc, equalTo(expectedDoc));
            assertThat(iterated.lookupOrd(iterated.ordValue()).utf8ToString(), equalTo(expected[doc]));
            expectedDoc = nextDocWithValue(expected, doc + 1);
        }
        assertThat("docs with a value left unvisited", expectedDoc, equalTo(NO_MORE_DOCS));

        final SortedDocValues exact = open.apply(leaf);
        for (int doc = randomIntBetween(0, 10); doc < maxDoc; doc += randomIntBetween(1, 50)) {
            assertThat("doc " + doc, exact.advanceExact(doc), equalTo(expected[doc] != null));
            if (expected[doc] != null) {
                assertThat("doc " + doc, exact.lookupOrd(exact.ordValue()).utf8ToString(), equalTo(expected[doc]));
            }
        }

        final SortedDocValues advanced = open.apply(leaf);
        int target = randomIntBetween(0, 10);
        while (target < maxDoc) {
            final int doc = advanced.advance(target);
            assertThat("target " + target, doc, equalTo(nextDocWithValue(expected, target)));
            if (doc == NO_MORE_DOCS) {
                break;
            }
            assertThat("doc " + doc, advanced.lookupOrd(advanced.ordValue()).utf8ToString(), equalTo(expected[doc]));
            target = doc + randomIntBetween(1, 100);
        }
    }

    private static int nextDocWithValue(final String[] expected, int from) {
        for (int doc = from; doc < expected.length; doc++) {
            if (expected[doc] != null) {
                return doc;
            }
        }
        return NO_MORE_DOCS;
    }
}
