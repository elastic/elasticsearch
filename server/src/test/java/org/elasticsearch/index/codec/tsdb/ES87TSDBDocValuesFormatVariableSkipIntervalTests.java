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
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.BaseDocValuesFormatTestCase;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.TestUtil;

import java.io.IOException;
import java.util.Arrays;

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
}
