/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.ColumNARDocValuesFormat;
import org.elasticsearch.columnar.ColumnarFieldType;
import org.elasticsearch.columnar.string.PageBudget;
import org.elasticsearch.columnar.string.StringBinaryPayload;
import org.elasticsearch.index.fielddata.ColumnarPayloadSortableBinaryDocValues;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.hamcrest.Matchers.equalTo;

/**
 * Counting a columnar field a page at a time has to give the counts that reading it a document at a time gives:
 * a document counts once for each distinct value it holds, however it is laid out in the column. Each test builds
 * a real columnar segment and compares the collector with a count made straight from the documents.
 */
public class ColumnarPageTermsCollectorTests extends ESTestCase {

    private static final String FIELD = "kw";

    /** The collector the last {@link #collect} built, so a test can ask which shapes of page it was given. */
    private ColumnarPageTermsCollector lastCollector;

    /**
     * Counts into a map. The real target is the aggregator, which cannot be built without a whole search context,
     * so this stands in for it and does nothing but add up what it is given.
     */
    private static final class CountingTarget implements ColumnarPageTermsCollector.Target {
        final Map<String, Long> counts = new TreeMap<>();
        int pageDocs;
        int fallbackDocs;

        @Override
        public void count(BytesRef term, long docCount) {
            counts.merge(term.utf8ToString(), docCount, Long::sum);
        }

        @Override
        public PageBudget budget() {
            return PageBudget.UNLIMITED;
        }

        @Override
        public void flushed(int docs, boolean asPages) {
            if (asPages) {
                pageDocs += docs;
            } else {
                fallbackDocs += docs;
            }
        }
    }

    /** A column that repeats a lot, which is the case a dictionary page is for. Several pages' worth of documents. */
    public void testFewTermsOverSeveralPages() throws IOException {
        final String[] vocabulary = { "7.17.0", "8.4.1", "8.11.0", "8.12.2", "9.0.0" };
        final int numDocs = ColumnarPageTermsCollector.PAGE_DOCS * 2 + randomIntBetween(1, 1000);
        final String[][] docs = new String[numDocs][];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = new String[] { vocabulary[randomIntBetween(0, vocabulary.length - 1)] };
        }
        final CountingTarget target = collectAll(docs);
        assertThat(target.counts, equalTo(expected(docs, null)));
        assertThat("a column this repetitive is read as pages", target.pageDocs, equalTo(numDocs));
        assertThat(target.fallbackDocs, equalTo(0));
        assertThat("every page is a dictionary and ordinals", lastCollector.ordinalPages, equalTo(3));
        assertThat(lastCollector.flatPages, equalTo(0));
    }

    /** A document that holds a value twice still counts once for it. */
    public void testADocumentCountsOncePerDistinctValue() throws IOException {
        final String[][] docs = { { "a", "a", "b" }, { "b" }, { "a", "b", "a", "b" }, { "c", "c", "c" }, { "a" } };
        final CountingTarget target = collectAll(docs);
        assertThat(target.counts, equalTo(expected(docs, null)));
        assertThat(target.counts.get("a"), equalTo(3L));
        assertThat(target.counts.get("c"), equalTo(1L));
    }

    /** Documents without the field, and slots that hold null, are not values. */
    public void testMissingDocumentsAndNullSlots() throws IOException {
        final String[][] docs = { { "a" }, null, { null }, { "a", null }, { null, "b", null }, null, { "b" } };
        final CountingTarget target = collectAll(docs);
        assertThat(target.counts, equalTo(expected(docs, null)));
        assertThat(target.counts.get("a"), equalTo(2L));
        assertThat(target.counts.get("b"), equalTo(2L));
    }

    /** A column that does not repeat comes back as values, not a dictionary, and still has to count the same. */
    public void testNoRepeatsStillCounts() throws IOException {
        final int numDocs = 3000;
        final String[][] docs = new String[numDocs][];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = new String[] { "unique-" + i };
        }
        final CountingTarget target = collectAll(docs);
        assertThat(target.counts, equalTo(expected(docs, null)));
        assertThat("a page of distinct values is handed over as values", lastCollector.flatPages, equalTo(1));
        assertThat(lastCollector.ordinalPages, equalTo(0));
    }

    /** As above with several values a document, some repeated within the document. */
    public void testNoRepeatsAcrossDocumentsButWithinThem() throws IOException {
        final int numDocs = 3000;
        final String[][] docs = new String[numDocs][];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = new String[] { "u-" + i, "v-" + i, "u-" + i };
        }
        final CountingTarget target = collectAll(docs);
        assertThat(target.counts, equalTo(expected(docs, null)));
        assertThat("the page is flat, so a repeat within a document is found by its bytes", lastCollector.flatPages, equalTo(1));
    }

    /** What a filtered search does: only some documents are collected, in order. */
    public void testOnlyTheCollectedDocumentsCount() throws IOException {
        final String[][] docs = randomDocs(5000, 6, 3);
        final Set<Integer> chosen = new HashSet<>();
        for (int i = 0; i < docs.length; i++) {
            if (randomIntBetween(0, 3) == 0) {
                chosen.add(i);
            }
        }
        final CountingTarget target = collect(docs, chosen);
        assertThat(target.counts, equalTo(expected(docs, chosen)));
    }

    /** Nothing is counted until the buffer fills or the owner flushes, and flushing twice counts nothing twice. */
    public void testNothingIsCountedUntilFlushed() throws IOException {
        final String[][] docs = { { "a" }, { "b" }, { "a" } };
        withValues(docs, values -> {
            final CountingTarget target = new CountingTarget();
            final ColumnarPageTermsCollector collector = new ColumnarPageTermsCollector(values, target);
            for (int doc = 0; doc < docs.length; doc++) {
                collector.collect(doc, 0);
            }
            assertThat(target.counts.size(), equalTo(0));
            collector.flush();
            assertThat(target.counts, equalTo(expected(docs, null)));
            collector.flush();
            assertThat(target.counts, equalTo(expected(docs, null)));
        });
    }

    /** Random shapes: how many distinct values, how many a document, how much missing. */
    public void testRandomLayouts() throws IOException {
        for (int round = 0; round < 10; round++) {
            final int numDocs = randomIntBetween(1, ColumnarPageTermsCollector.PAGE_DOCS * 2 + 500);
            final int vocabularySize = randomFrom(2, 10, 200, 20000);
            final int maxSlots = randomFrom(1, 1, 3, 12);
            final String[][] docs = randomDocs(numDocs, vocabularySize, maxSlots);
            final CountingTarget target = collectAll(docs);
            assertThat(
                "vocabulary " + vocabularySize + ", up to " + maxSlots + " slots, " + numDocs + " docs",
                target.counts,
                equalTo(expected(docs, null))
            );
        }
    }

    private static String[][] randomDocs(int numDocs, int vocabularySize, int maxSlots) {
        final String[][] docs = new String[numDocs][];
        for (int i = 0; i < numDocs; i++) {
            if (randomIntBetween(0, 19) == 0) {
                continue; // the field is absent
            }
            final String[] slots = new String[randomIntBetween(1, maxSlots)];
            for (int s = 0; s < slots.length; s++) {
                slots[s] = randomIntBetween(0, 19) == 0 ? null : "term-" + randomIntBetween(0, vocabularySize - 1);
            }
            docs[i] = slots;
        }
        return docs;
    }

    /** The counts a document-at-a-time reading gives: each document once for each distinct non-null value. */
    private static Map<String, Long> expected(String[][] docs, Set<Integer> only) {
        final Map<String, Long> counts = new TreeMap<>();
        for (int doc = 0; doc < docs.length; doc++) {
            if (docs[doc] == null || (only != null && only.contains(doc) == false)) {
                continue;
            }
            final Set<String> distinct = new HashSet<>();
            for (String slot : docs[doc]) {
                if (slot != null) {
                    distinct.add(slot);
                }
            }
            for (String value : distinct) {
                counts.merge(value, 1L, Long::sum);
            }
        }
        return counts;
    }

    private CountingTarget collectAll(String[][] docs) throws IOException {
        return collect(docs, null);
    }

    private CountingTarget collect(String[][] docs, Set<Integer> only) throws IOException {
        final CountingTarget target = new CountingTarget();
        withValues(docs, values -> {
            final ColumnarPageTermsCollector collector = new ColumnarPageTermsCollector(values, target);
            lastCollector = collector;
            for (int doc = 0; doc < docs.length; doc++) {
                if (only == null || only.contains(doc)) {
                    collector.collect(doc, 0);
                }
            }
            collector.flush();
        });
        return target;
    }

    private interface Check {
        void check(ColumnarPayloadSortableBinaryDocValues values) throws IOException;
    }

    /** {@code null} in place of a document's slots writes no field for it at all. */
    private void withValues(String[][] docs, Check check) throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig().setCodec(columnarCodec()))) {
                for (String[] slots : docs) {
                    final Document doc = new Document();
                    if (slots != null) {
                        doc.add(new Field(FIELD, encode(slots), binaryDocValuesType()));
                    }
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final LeafReader leaf = reader.leaves().get(0).reader();
                final ColumnarPayloadSortableBinaryDocValues values = ColumnarPayloadSortableBinaryDocValues.from(leaf, FIELD);
                assertNotNull("the field is a column", values.columnSource());
                check.check(values);
            }
        }
    }

    private static BytesRef encode(String[] slots) {
        final List<BytesRef> refs = new ArrayList<>(slots.length);
        for (String slot : slots) {
            refs.add(slot == null ? null : new BytesRef(slot));
        }
        return BytesRef.deepCopyOf(new StringBinaryPayload.Builder().encode(refs));
    }

    private static FieldType binaryDocValuesType() {
        final FieldType type = new FieldType();
        type.setDocValuesType(DocValuesType.BINARY);
        type.freeze();
        return type;
    }

    private static Codec columnarCodec() {
        final Codec base = TestUtil.getDefaultCodec();
        final DocValuesFormat columnar = new ColumNARDocValuesFormat(field -> ColumnarFieldType.STRING);
        return new FilterCodec(base.getName(), base) {
            private final DocValuesFormat perField = new PerFieldDocValuesFormat() {
                @Override
                public DocValuesFormat getDocValuesFormatForField(String field) {
                    return columnar;
                }
            };

            @Override
            public DocValuesFormat docValuesFormat() {
                return perField;
            }
        };
    }
}
