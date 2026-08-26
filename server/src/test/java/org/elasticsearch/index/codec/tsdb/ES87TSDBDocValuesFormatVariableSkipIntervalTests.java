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
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.BaseDocValuesFormatTestCase;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.IndexVersion;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.LongSupplier;

/** Tests ES87TSDBDocValuesFormat with custom skipper interval size. */
public class ES87TSDBDocValuesFormatVariableSkipIntervalTests extends BaseDocValuesFormatTestCase {

    @Override
    protected Codec getCodec() {
        // small interval size to test with many intervals
        return TestUtil.alwaysDocValuesFormat(new ES87TSDBDocValuesFormatTests.TestES87TSDBDocValuesFormat(random().nextInt(4, 16)));
    }

    public void testSkipIndexIntervalSize() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new ES87TSDBDocValuesFormat(random().nextInt(Integer.MIN_VALUE, 2))
        );
        assertTrue(ex.getMessage().contains("skipIndexIntervalSize must be > 1"));
    }

    public void testSkipperAllEqualValue() throws IOException {
        final IndexWriterConfig config = new IndexWriterConfig().setCodec(getCodec());
        try (Directory directory = newDirectory(); RandomIndexWriter writer = new RandomIndexWriter(random(), directory, config)) {
            final int numDocs = atLeast(100);
            for (int i = 0; i < numDocs; i++) {
                final Document doc = new Document();
                doc.add(NumericDocValuesField.indexedField("dv", 0L));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
            try (IndexReader reader = writer.getReader()) {
                assertEquals(1, reader.leaves().size());
                final DocValuesSkipper skipper = reader.leaves().get(0).reader().getDocValuesSkipper("dv");
                assertNotNull(skipper);
                skipper.advance(0);
                assertEquals(0L, skipper.minValue(0));
                assertEquals(0L, skipper.maxValue(0));
                assertEquals(numDocs, skipper.docCount(0));
                skipper.advance(skipper.maxDocID(0) + 1);
                assertEquals(DocIdSetIterator.NO_MORE_DOCS, skipper.minDocID(0));
            }
        }
    }

    // break on different value
    public void testSkipperFewValuesSorted() throws IOException {
        final IndexWriterConfig config = new IndexWriterConfig().setCodec(getCodec());
        boolean reverse = random().nextBoolean();
        config.setIndexSort(new Sort(new SortField("dv", SortField.Type.LONG, reverse)));
        try (Directory directory = newDirectory(); RandomIndexWriter writer = new RandomIndexWriter(random(), directory, config)) {
            final int intervals = random().nextInt(2, 10);
            final int[] numDocs = new int[intervals];
            for (int i = 0; i < intervals; i++) {
                numDocs[i] = random().nextInt(10) + 16;
                for (int j = 0; j < numDocs[i]; j++) {
                    final Document doc = new Document();
                    doc.add(NumericDocValuesField.indexedField("dv", i));
                    writer.addDocument(doc);
                }
            }
            writer.forceMerge(1);
            try (IndexReader reader = writer.getReader()) {
                assertEquals(1, reader.leaves().size());
                final DocValuesSkipper skipper = reader.leaves().get(0).reader().getDocValuesSkipper("dv");
                assertNotNull(skipper);
                assertEquals(Arrays.stream(numDocs).sum(), skipper.docCount());
                skipper.advance(0);
                if (reverse) {
                    for (int i = intervals - 1; i >= 0; i--) {
                        assertEquals(i, skipper.minValue(0));
                        assertEquals(i, skipper.maxValue(0));
                        assertEquals(numDocs[i], skipper.docCount(0));
                        skipper.advance(skipper.maxDocID(0) + 1);
                    }
                } else {
                    for (int i = 0; i < intervals; i++) {
                        assertEquals(i, skipper.minValue(0));
                        assertEquals(i, skipper.maxValue(0));
                        assertEquals(numDocs[i], skipper.docCount(0));
                        skipper.advance(skipper.maxDocID(0) + 1);
                    }
                }
                assertEquals(DocIdSetIterator.NO_MORE_DOCS, skipper.minDocID(0));
            }
        }
    }

    // break on empty doc values
    public void testSkipperAllEqualValueWithGaps() throws IOException {
        final IndexWriterConfig config = new IndexWriterConfig().setCodec(getCodec());
        config.setIndexSort(new Sort(new SortField("sort", SortField.Type.LONG, false)));
        try (Directory directory = newDirectory(); RandomIndexWriter writer = new RandomIndexWriter(random(), directory, config)) {
            final int gaps = random().nextInt(2, 10);
            final int[] numDocs = new int[gaps];
            long totaldocs = 0;
            for (int i = 0; i < gaps; i++) {
                numDocs[i] = random().nextInt(10) + 16;
                for (int j = 0; j < numDocs[i]; j++) {
                    final Document doc = new Document();
                    doc.add(new NumericDocValuesField("sort", totaldocs++));
                    doc.add(SortedNumericDocValuesField.indexedField("dv", 0L));
                    writer.addDocument(doc);
                }
                // add doc with empty "dv"
                final Document doc = new Document();
                doc.add(new NumericDocValuesField("sort", totaldocs++));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
            try (IndexReader reader = writer.getReader()) {
                assertEquals(1, reader.leaves().size());
                final DocValuesSkipper skipper = reader.leaves().get(0).reader().getDocValuesSkipper("dv");
                assertNotNull(skipper);
                assertEquals(Arrays.stream(numDocs).sum(), skipper.docCount());
                skipper.advance(0);
                for (int i = 0; i < gaps; i++) {
                    assertEquals(0L, skipper.minValue(0));
                    assertEquals(0L, skipper.maxValue(0));
                    assertEquals(numDocs[i], skipper.docCount(0));
                    skipper.advance(skipper.maxDocID(0) + 1);
                }
                assertEquals(DocIdSetIterator.NO_MORE_DOCS, skipper.minDocID(0));
            }
        }
    }

    // break on multi-values
    public void testSkipperAllEqualValueWithMultiValues() throws IOException {
        final IndexWriterConfig config = new IndexWriterConfig().setCodec(getCodec());
        config.setIndexSort(new Sort(new SortField("sort", SortField.Type.LONG, false)));
        try (Directory directory = newDirectory(); RandomIndexWriter writer = new RandomIndexWriter(random(), directory, config)) {
            final int gaps = random().nextInt(2, 10);
            final int[] numDocs = new int[gaps];
            long totaldocs = 0;
            for (int i = 0; i < gaps; i++) {
                int docs = random().nextInt(10) + 16;
                numDocs[i] += docs;
                for (int j = 0; j < docs; j++) {
                    final Document doc = new Document();
                    doc.add(new NumericDocValuesField("sort", totaldocs++));
                    doc.add(SortedNumericDocValuesField.indexedField("dv", 0L));
                    writer.addDocument(doc);
                }
                if (i != gaps - 1) {
                    // add doc with mutivalues
                    final Document doc = new Document();
                    doc.add(new NumericDocValuesField("sort", totaldocs++));
                    doc.add(SortedNumericDocValuesField.indexedField("dv", 0L));
                    doc.add(SortedNumericDocValuesField.indexedField("dv", 0L));
                    writer.addDocument(doc);
                    numDocs[i + 1] = 1;
                }
            }
            writer.forceMerge(1);
            try (IndexReader reader = writer.getReader()) {
                assertEquals(1, reader.leaves().size());
                final DocValuesSkipper skipper = reader.leaves().get(0).reader().getDocValuesSkipper("dv");
                assertNotNull(skipper);
                assertEquals(Arrays.stream(numDocs).sum(), skipper.docCount());
                skipper.advance(0);
                for (int i = 0; i < gaps; i++) {
                    assertEquals(0L, skipper.minValue(0));
                    assertEquals(0L, skipper.maxValue(0));
                    assertEquals(numDocs[i], skipper.docCount(0));
                    skipper.advance(skipper.maxDocID(0) + 1);
                }
                assertEquals(DocIdSetIterator.NO_MORE_DOCS, skipper.minDocID(0));
            }
        }
    }

    /**
     * Patched copy of the base class method that adds a missing {@code writer.commit()} after
     * deleting docs so that {@code DirectoryReader.open(dir)} sees the deletions.
     * This can be removed when the upstream Lucene fix is integrated (Lucene 10.5+).
     */
    private void doTestSortedNumericsVsStoredFieldsPatched(LongSupplier counts, LongSupplier values) throws Exception {
        assumeFalse(
            "Remove this method and the overrides that call it; the upstream Lucene bug has been fixed in 10.5",
            IndexVersion.current().luceneVersion().onOrAfter(org.apache.lucene.util.Version.fromBits(10, 5, 0))
        );
        Directory dir = newDirectory();
        IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir, conf);

        int numDocs = atLeast(300);
        assert numDocs > 256;
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));

            int valueCount = (int) counts.getAsLong();
            long[] valueArray = new long[valueCount];
            for (int j = 0; j < valueCount; j++) {
                long value = values.getAsLong();
                valueArray[j] = value;
                doc.add(new SortedNumericDocValuesField("dv", value));
            }
            Arrays.sort(valueArray);
            for (int j = 0; j < valueCount; j++) {
                doc.add(new StoredField("stored", Long.toString(valueArray[j])));
            }
            writer.addDocument(doc);
            if (random().nextInt(31) == 0) {
                writer.commit();
            }
        }

        // delete some docs
        int numDeletions = random().nextInt(numDocs / 10);
        for (int i = 0; i < numDeletions; i++) {
            int id = random().nextInt(numDocs);
            writer.deleteDocuments(new Term("id", Integer.toString(id)));
        }
        writer.commit();
        try (DirectoryReader reader = maybeWrapWithMergingReader(DirectoryReader.open(dir))) {
            TestUtil.checkReader(reader);
            compareStoredFieldWithSortedNumericsDV(reader, "stored", "dv");
        }
        writer.forceMerge(numDocs / 256);
        try (DirectoryReader reader = maybeWrapWithMergingReader(DirectoryReader.open(dir))) {
            TestUtil.checkReader(reader);
            compareStoredFieldWithSortedNumericsDV(reader, "stored", "dv");
        }
        IOUtils.close(writer, dir);
    }

    @Override
    public void testSortedNumericsSingleValuedVsStoredFields() throws Exception {
        int numIterations = atLeast(1);
        for (int i = 0; i < numIterations; i++) {
            doTestSortedNumericsVsStoredFieldsPatched(() -> 1, random()::nextLong);
        }
    }

    @Override
    public void testSortedNumericsSingleValuedMissingVsStoredFields() throws Exception {
        int numIterations = atLeast(1);
        for (int i = 0; i < numIterations; i++) {
            doTestSortedNumericsVsStoredFieldsPatched(() -> random().nextBoolean() ? 0 : 1, random()::nextLong);
        }
    }

    @Override
    public void testSortedNumericsMultipleValuesVsStoredFields() throws Exception {
        int numIterations = atLeast(1);
        for (int i = 0; i < numIterations; i++) {
            doTestSortedNumericsVsStoredFieldsPatched(() -> TestUtil.nextLong(random(), 0, 50), random()::nextLong);
        }
    }

    @Override
    public void testSortedNumericsFewUniqueSetsVsStoredFields() throws Exception {
        final long[] uniqueValues = new long[TestUtil.nextInt(random(), 2, 6)];
        for (int i = 0; i < uniqueValues.length; ++i) {
            uniqueValues[i] = random().nextLong();
        }
        int numIterations = atLeast(1);
        for (int i = 0; i < numIterations; i++) {
            doTestSortedNumericsVsStoredFieldsPatched(
                () -> TestUtil.nextLong(random(), 0, 6),
                () -> uniqueValues[random().nextInt(uniqueValues.length)]
            );
        }
    }
}
