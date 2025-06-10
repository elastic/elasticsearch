/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.lucene.queries;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.search.DummyTotalHitCountCollector;
import org.apache.lucene.tests.search.QueryUtils;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.lucene.search.XIndexSortSortedNumericDocValuesRangeQuery;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.MatcherAssert;

import java.io.IOException;

import static org.hamcrest.Matchers.instanceOf;

@LuceneTestCase.SuppressCodecs(value = "SimpleText")
public class TimestampQueryTests extends ESTestCase {

    static final String FIELD_NAME = "@timestamp";

    public void testSimple() throws IOException {
        final String primarySortFieldName = "host.name";
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
            SortField primarySortField = new SortField(primarySortFieldName, SortField.Type.STRING, false);
            SortField secondarySortField = new SortedNumericSortField(FIELD_NAME, SortField.Type.LONG, true);
            iwc.setIndexSort(new Sort(primarySortField, secondarySortField));

            try (IndexWriter iw = new IndexWriter(dir, iwc)) {
                {
                    Document doc = new Document();
                    doc.add(SortedDocValuesField.indexedField(primarySortFieldName, new BytesRef("host-1")));
                    final long value = 1;
                    doc.add(SortedNumericDocValuesField.indexedField(FIELD_NAME, value));
                    doc.add(new LongPoint(FIELD_NAME, value));
                    iw.addDocument(doc);
                }
                {
                    Document doc = new Document();
                    doc.add(SortedDocValuesField.indexedField(primarySortFieldName, new BytesRef("host-1")));
                    final long value = 3;
                    doc.add(SortedNumericDocValuesField.indexedField(FIELD_NAME, value));
                    doc.add(new LongPoint(FIELD_NAME, value));
                    iw.addDocument(doc);
                }
                {
                    Document doc = new Document();
                    doc.add(SortedDocValuesField.indexedField(primarySortFieldName, new BytesRef("host-2")));
                    final long value = 2;
                    doc.add(SortedNumericDocValuesField.indexedField(FIELD_NAME, value));
                    doc.add(new LongPoint(FIELD_NAME, value));
                    iw.addDocument(doc);
                }
                {
                    Document doc = new Document();
                    doc.add(SortedDocValuesField.indexedField(primarySortFieldName, new BytesRef("host-1")));
                    final long value = 5;
                    doc.add(SortedNumericDocValuesField.indexedField(FIELD_NAME, value));
                    doc.add(new LongPoint(FIELD_NAME, value));
                    iw.addDocument(doc);
                }
                {
                    Document doc = new Document();
                    doc.add(SortedDocValuesField.indexedField(primarySortFieldName, new BytesRef("host-2")));
                    final long value = 4;
                    doc.add(SortedNumericDocValuesField.indexedField(FIELD_NAME, value));
                    doc.add(new LongPoint(FIELD_NAME, value));
                    iw.addDocument(doc);
                }
                {
                    Document doc = new Document();
                    doc.add(SortedDocValuesField.indexedField(primarySortFieldName, new BytesRef("host-1")));
                    final long value = 7;
                    doc.add(SortedNumericDocValuesField.indexedField(FIELD_NAME, value));
                    doc.add(new LongPoint(FIELD_NAME, value));
                    iw.addDocument(doc);
                }
                {
                    Document doc = new Document();
                    doc.add(SortedDocValuesField.indexedField(primarySortFieldName, new BytesRef("host-2")));
                    final long value = 8;
                    doc.add(SortedNumericDocValuesField.indexedField(FIELD_NAME, value));
                    doc.add(new LongPoint(FIELD_NAME, value));
                    iw.addDocument(doc);
                }
            }

            try (IndexReader reader = DirectoryReader.open(dir)) {
                final IndexSearcher searcher = newSearcher(reader);
                {
                    final long min = 0;
                    final long max = 2;
                    final Query q1 = LongPoint.newRangeQuery(FIELD_NAME, min, max);
                    final Query q2 = createQuery(min, max);
                    assertSameHits(searcher, q1, q2, false);
                }
                {
                    final long min = 1;
                    final long max = 3;
                    final Query q1 = LongPoint.newRangeQuery(FIELD_NAME, min, max);
                    final Query q2 = createQuery(min, max);
                    assertSameHits(searcher, q1, q2, false);
                }
                {
                    final long min = 2;
                    final long max = 6;
                    final Query q1 = LongPoint.newRangeQuery(FIELD_NAME, min, max);
                    final Query q2 = createQuery(min, max);
                    assertSameHits(searcher, q1, q2, false);
                }
                {
                    final long min = 5;
                    final long max = 8;
                    final Query q1 = LongPoint.newRangeQuery(FIELD_NAME, min, max);
                    final Query q2 = createQuery(min, max);
                    assertSameHits(searcher, q1, q2, false);
                }
                {
                    final long min = 0;
                    final long max = 0;
                    final Query q1 = LongPoint.newRangeQuery(FIELD_NAME, min, max);
                    final Query q2 = createQuery(min, max);
                    assertSameHits(searcher, q1, q2, false);
                }
            }
        }
    }

    public void testSameHitsAsPointRangeQueryIndexSortByTwoFields() throws IOException {
        final int iters = atLeast(10);
        final String primarySortFieldName = "host.name";

        for (int iter = 0; iter < iters; ++iter) {
            String[] hostNames = new String[2];
            for (int i = 0; i < hostNames.length; i++) {
                hostNames[i] = "host-" + i;
            }
            try (Directory dir = newDirectory()) {
                IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
                SortField primarySortField = new SortField(primarySortFieldName, SortField.Type.STRING, false);
                SortField secondarySortField = new SortedNumericSortField(FIELD_NAME, SortField.Type.LONG, true);
                iwc.setIndexSort(new Sort(primarySortField, secondarySortField));

                try (IndexWriter iw = new IndexWriter(dir, iwc)) {
                    final int numDocs = atLeast(100);
                    for (int i = 0; i < numDocs; ++i) {
                        Document doc = new Document();
                        if (hostNames.length > 0) {
                            doc.add(
                                SortedDocValuesField.indexedField(
                                    primarySortFieldName,
                                    new BytesRef(hostNames[random().nextInt(hostNames.length)])
                                )
                            );
                        }
                        final long value = TestUtil.nextLong(random(), -100, 10000);
                        doc.add(SortedNumericDocValuesField.indexedField(FIELD_NAME, value));
                        doc.add(new LongPoint(FIELD_NAME, value));
                        iw.addDocument(doc);
                    }
                }

                try (IndexReader reader = DirectoryReader.open(dir)) {
                    final IndexSearcher searcher = newSearcher(reader);
                    for (int i = 0; i < 100; ++i) {
                        final long min = random().nextBoolean() ? Long.MIN_VALUE : TestUtil.nextLong(random(), -100, 10000);
                        final long max = random().nextBoolean() ? Long.MAX_VALUE : TestUtil.nextLong(random(), -100, 10000);
                        final Query q1 = LongPoint.newRangeQuery(FIELD_NAME, min, max);
                        final Query q2 = createQuery(min, max);
                        assertSameHits(searcher, q1, q2, false);
                    }
                }
            }
        }
    }

    public void testSameHitsAsPointRangeQueryIndexSortByTimestamp() throws IOException {
        final int iters = atLeast(10);
        for (int iter = 0; iter < iters; ++iter) {
            try (Directory dir = newDirectory()) {
                IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
                SortField sortField = new SortedNumericSortField(FIELD_NAME, SortField.Type.LONG, true);
                iwc.setIndexSort(new Sort(sortField));

                try (IndexWriter iw = new IndexWriter(dir, iwc)) {
                    final int numDocs = atLeast(100);
                    for (int i = 0; i < numDocs; ++i) {
                        Document doc = new Document();
                        final long value = TestUtil.nextLong(random(), -100, 10000);
                        doc.add(SortedNumericDocValuesField.indexedField(FIELD_NAME, value));
                        doc.add(new LongPoint(FIELD_NAME, value));
                        iw.addDocument(doc);
                    }
                }

                try (IndexReader reader = DirectoryReader.open(dir)) {
                    final IndexSearcher searcher = newSearcher(reader);
                    for (int i = 0; i < 100; ++i) {
                        final long min = random().nextBoolean() ? Long.MIN_VALUE : TestUtil.nextLong(random(), -100, 10000);
                        final long max = random().nextBoolean() ? Long.MAX_VALUE : TestUtil.nextLong(random(), -100, 10000);
                        final Query q1 = LongPoint.newRangeQuery(FIELD_NAME, min, max);
                        final Query q2 = createQuery(min, max);
                        assertSameHits(searcher, q1, q2, false);
                    }
                }
            }
        }
    }

    private static void assertSameHits(IndexSearcher searcher, Query q1, Query q2, boolean scores) throws IOException {
        final int maxDoc = searcher.getIndexReader().maxDoc();
        final TopDocs td1 = searcher.search(q1, maxDoc, scores ? Sort.RELEVANCE : Sort.INDEXORDER);
        final TopDocs td2 = searcher.search(q2, maxDoc, scores ? Sort.RELEVANCE : Sort.INDEXORDER);
        assertEquals(td1.totalHits.value(), td2.totalHits.value());
        for (int i = 0; i < td1.scoreDocs.length; ++i) {
            assertEquals(td1.scoreDocs[i].doc, td2.scoreDocs[i].doc);
            if (scores) {
                assertEquals(td1.scoreDocs[i].score, td2.scoreDocs[i].score, 10e-7);
            }
        }
    }

    public void testEquals() {
        Query q1 = createQuery(3, 5);
        QueryUtils.checkEqual(q1, createQuery(3, 5));
        QueryUtils.checkUnequal(q1, createQuery(3, 6));
        QueryUtils.checkUnequal(q1, createQuery(4, 5));
    }

    public void testToString() {
        Query q1 = createQuery(3, 5);
        assertEquals("@timestamp:[3 TO 5]", q1.toString());
        assertEquals("@timestamp:[3 TO 5]", q1.toString("foo"));
        assertEquals("@timestamp:[3 TO 5]", q1.toString("bar"));
    }

    public void testIndexSortDocValuesWithEvenLength() throws Exception {
        Directory dir = newDirectory();

        IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
        Sort indexSort = new Sort(new SortedNumericSortField(FIELD_NAME, SortField.Type.LONG, true));
        iwc.setIndexSort(indexSort);
        IndexWriter writer = new IndexWriter(dir, iwc);

        writer.addDocument(createDocument(-80));
        writer.addDocument(createDocument(-5));
        writer.addDocument(createDocument(0));
        writer.addDocument(createDocument(0));
        writer.addDocument(createDocument(30));
        writer.addDocument(createDocument(35));

        DirectoryReader reader = DirectoryReader.open(writer);
        IndexSearcher searcher = newSearcher(reader);

        // Test ranges consisting of one value.
        assertNumberOfHits(searcher, createQuery(-80, -80), 1);
        assertNumberOfHits(searcher, createQuery(-5, -5), 1);
        assertNumberOfHits(searcher, createQuery(0, 0), 2);
        assertNumberOfHits(searcher, createQuery(30, 30), 1);
        assertNumberOfHits(searcher, createQuery(35, 35), 1);

        assertNumberOfHits(searcher, createQuery(-90, -90), 0);
        assertNumberOfHits(searcher, createQuery(5, 5), 0);
        assertNumberOfHits(searcher, createQuery(40, 40), 0);

        // Test the lower end of the document value range.
        assertNumberOfHits(searcher, createQuery(-90, -4), 2);
        assertNumberOfHits(searcher, createQuery(-80, -4), 2);
        assertNumberOfHits(searcher, createQuery(-70, -4), 1);
        assertNumberOfHits(searcher, createQuery(-80, -5), 2);

        // Test the upper end of the document value range.
        assertNumberOfHits(searcher, createQuery(25, 34), 1);
        assertNumberOfHits(searcher, createQuery(25, 35), 2);
        assertNumberOfHits(searcher, createQuery(25, 36), 2);
        assertNumberOfHits(searcher, createQuery(30, 35), 2);

        // Test multiple occurrences of the same value.
        assertNumberOfHits(searcher, createQuery(-4, 4), 2);
        assertNumberOfHits(searcher, createQuery(-4, 0), 2);
        assertNumberOfHits(searcher, createQuery(0, 4), 2);
        assertNumberOfHits(searcher, createQuery(0, 30), 3);

        // Test ranges that span all documents.
        assertNumberOfHits(searcher, createQuery(-80, 35), 6);
        assertNumberOfHits(searcher, createQuery(-90, 40), 6);

        writer.close();
        reader.close();
        dir.close();
    }

    private static void assertNumberOfHits(IndexSearcher searcher, Query query, int numberOfHits) throws IOException {
        assertEquals(numberOfHits, searcher.search(query, DummyTotalHitCountCollector.createManager()).intValue());
        assertEquals(numberOfHits, searcher.count(query));
    }

    public void testIndexSortDocValuesWithOddLength() throws Exception {
        Directory dir = newDirectory();

        IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
        Sort indexSort = new Sort(new SortedNumericSortField(FIELD_NAME, SortField.Type.LONG, true));
        iwc.setIndexSort(indexSort);
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);

        writer.addDocument(createDocument(-80));
        writer.addDocument(createDocument(-5));
        writer.addDocument(createDocument(0));
        writer.addDocument(createDocument(0));
        writer.addDocument(createDocument(5));
        writer.addDocument(createDocument(30));
        writer.addDocument(createDocument(35));

        DirectoryReader reader = writer.getReader();
        IndexSearcher searcher = newSearcher(reader);

        // Test ranges consisting of one value.
        assertNumberOfHits(searcher, createQuery(-80, -80), 1);
        assertNumberOfHits(searcher, createQuery(-5, -5), 1);
        assertNumberOfHits(searcher, createQuery(0, 0), 2);
        assertNumberOfHits(searcher, createQuery(5, 5), 1);
        assertNumberOfHits(searcher, createQuery(30, 30), 1);
        assertNumberOfHits(searcher, createQuery(35, 35), 1);

        assertNumberOfHits(searcher, createQuery(-90, -90), 0);
        assertNumberOfHits(searcher, createQuery(6, 6), 0);
        assertNumberOfHits(searcher, createQuery(40, 40), 0);

        // Test the lower end of the document value range.
        assertNumberOfHits(searcher, createQuery(-90, -4), 2);
        assertNumberOfHits(searcher, createQuery(-80, -4), 2);
        assertNumberOfHits(searcher, createQuery(-70, -4), 1);
        assertNumberOfHits(searcher, createQuery(-80, -5), 2);

        // Test the upper end of the document value range.
        assertNumberOfHits(searcher, createQuery(25, 34), 1);
        assertNumberOfHits(searcher, createQuery(25, 35), 2);
        assertNumberOfHits(searcher, createQuery(25, 36), 2);
        assertNumberOfHits(searcher, createQuery(30, 35), 2);

        // Test multiple occurrences of the same value.
        assertNumberOfHits(searcher, createQuery(-4, 4), 2);
        assertNumberOfHits(searcher, createQuery(-4, 0), 2);
        assertNumberOfHits(searcher, createQuery(0, 4), 2);
        assertNumberOfHits(searcher, createQuery(0, 30), 4);

        // Test ranges that span all documents.
        assertNumberOfHits(searcher, createQuery(-80, 35), 7);
        assertNumberOfHits(searcher, createQuery(-90, 40), 7);

        writer.close();
        reader.close();
        dir.close();
    }

    public void testIndexSortDocValuesWithSingleValue() throws Exception {
        Directory dir = newDirectory();

        IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
        Sort indexSort = new Sort(new SortedNumericSortField(FIELD_NAME, SortField.Type.LONG, true));
        iwc.setIndexSort(indexSort);
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);

        writer.addDocument(createDocument(42));

        DirectoryReader reader = writer.getReader();
        IndexSearcher searcher = newSearcher(reader);

        assertNumberOfHits(searcher, createQuery(42, 43), 1);
        assertNumberOfHits(searcher, createQuery(42, 42), 1);
        assertNumberOfHits(searcher, createQuery(41, 41), 0);
        assertNumberOfHits(searcher, createQuery(43, 43), 0);

        writer.close();
        reader.close();
        dir.close();
    }

    public void testNoDocuments() throws IOException {
        Directory dir = newDirectory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
        writer.addDocument(new Document());
        IndexReader reader = writer.getReader();
        IndexSearcher searcher = newSearcher(reader);
        Query query = createQuery(2, 4);
        Weight w = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE, 1);
        assertNull(w.scorer(searcher.getIndexReader().leaves().get(0)));

        writer.close();
        reader.close();
        dir.close();
    }

    public void testRewriteExhaustiveRange() throws IOException {
        Directory dir = newDirectory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
        writer.addDocument(new Document());
        IndexReader reader = writer.getReader();

        Query query = createQuery(Long.MIN_VALUE, Long.MAX_VALUE);
        Query rewrittenQuery = query.rewrite(newSearcher(reader));
        assertEquals(new FieldExistsQuery(FIELD_NAME), rewrittenQuery);

        writer.close();
        reader.close();
        dir.close();
    }

    public void testRewriteFallbackQuery() throws IOException {
        Directory dir = newDirectory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
        writer.addDocument(new Document());
        IndexReader reader = writer.getReader();

        // Create an (unrealistic) fallback query that is sure to be rewritten.
        Query fallbackQuery = new BooleanQuery.Builder().build();
        Query query = new XIndexSortSortedNumericDocValuesRangeQuery("field", 1, 42, fallbackQuery);

        Query rewrittenQuery = query.rewrite(newSearcher(reader));
        assertNotEquals(query, rewrittenQuery);
        MatcherAssert.assertThat(rewrittenQuery, instanceOf(XIndexSortSortedNumericDocValuesRangeQuery.class));

        XIndexSortSortedNumericDocValuesRangeQuery rangeQuery = (XIndexSortSortedNumericDocValuesRangeQuery) rewrittenQuery;
        assertEquals(new MatchNoDocsQuery(), rangeQuery.getFallbackQuery());

        writer.close();
        reader.close();
        dir.close();
    }

    /** Test that the index sort optimization not activated if there is no index sort. */
    public void testNoIndexSort() throws Exception {
        Directory dir = newDirectory();

        RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
        writer.addDocument(createDocument(0));

        expectThrows(AssertionError.class, () -> testIndexSortOptimizationDeactivated(writer));

        writer.close();
        dir.close();
    }

    /** Test that the index sort optimization is not activated when the sort is on the wrong field. */
    public void testIndexSortOnWrongField() throws Exception {
        Directory dir = newDirectory();

        IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
        Sort indexSort = new Sort(new SortedNumericSortField("other-field", SortField.Type.LONG));
        iwc.setIndexSort(indexSort);

        RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
        writer.addDocument(createDocument(0));

        expectThrows(AssertionError.class, () -> testIndexSortOptimizationDeactivated(writer));

        writer.close();
        dir.close();
    }

    /**
     * Test that the index sort optimization is not activated when some documents have multiple
     * values.
     */
    public void testMultiDocValues() throws Exception {
        Directory dir = newDirectory();

        IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
        Sort indexSort = new Sort(new SortedNumericSortField(FIELD_NAME, SortField.Type.LONG, true));
        iwc.setIndexSort(indexSort);
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);

        Document doc = new Document();
        doc.add(SortedNumericDocValuesField.indexedField(FIELD_NAME, 0));
        doc.add(SortedNumericDocValuesField.indexedField(FIELD_NAME, 10));
        writer.addDocument(doc);

        var e = expectThrows(AssertionError.class, () -> testIndexSortOptimizationDeactivated(writer));
        assertEquals("@timestamp has multiple values per document", e.getMessage());

        writer.close();
        dir.close();
    }

    public void testIndexSortOptimizationDeactivated(RandomIndexWriter writer) throws IOException {
        try (DirectoryReader reader = writer.getReader()) {
            IndexSearcher searcher = newSearcher(reader);

            Query query = createQuery(0, 0);
            Weight weight = query.createWeight(searcher, ScoreMode.TOP_SCORES, 1.0F);

            // Check that the two-phase iterator is not null, indicating that we've fallen
            // back to SortedNumericDocValuesField.newSlowRangeQuery.
            for (LeafReaderContext context : searcher.getIndexReader().leaves()) {
                Scorer scorer = weight.scorer(context);
                assertNotNull(scorer.twoPhaseIterator());
            }
        }
    }

    public void testFallbackCount() throws IOException {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
        Sort indexSort = new Sort(new SortedNumericSortField("field", SortField.Type.LONG));
        iwc.setIndexSort(indexSort);
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
        Document doc = new Document();
        doc.add(new SortedNumericDocValuesField("field", 10));
        writer.addDocument(doc);
        IndexReader reader = writer.getReader();
        IndexSearcher searcher = newSearcher(reader);

        // we use an unrealistic query that exposes its own Weight#count
        Query fallbackQuery = new MatchNoDocsQuery();
        // the index is not sorted on this field, the fallback query is used
        Query query = new XIndexSortSortedNumericDocValuesRangeQuery("another", 1, 42, fallbackQuery);
        Weight weight = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
        for (LeafReaderContext context : searcher.getLeafContexts()) {
            assertEquals(0, weight.count(context));
        }

        writer.close();
        reader.close();
        dir.close();
    }

    public void testCompareCount() throws IOException {
        final int iters = atLeast(10);
        for (int iter = 0; iter < iters; ++iter) {
            Directory dir = newDirectory();
            IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
            SortField sortField = new SortedNumericSortField("field", SortField.Type.LONG);
            boolean enableMissingValue = random().nextBoolean();
            if (enableMissingValue) {
                long missingValue = random().nextBoolean()
                    ? TestUtil.nextLong(random(), -100, 10000)
                    : (random().nextBoolean() ? Long.MIN_VALUE : Long.MAX_VALUE);
                sortField.setMissingValue(missingValue);
            }
            iwc.setIndexSort(new Sort(sortField));

            RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);

            final int numDocs = atLeast(100);
            for (int i = 0; i < numDocs; ++i) {
                Document doc = new Document();
                final int numValues = TestUtil.nextInt(random(), 0, 1);
                for (int j = 0; j < numValues; ++j) {
                    final long value = TestUtil.nextLong(random(), -100, 10000);
                    doc = createSNDVAndPointDocument(value);
                }
                writer.addDocument(doc);
            }

            if (random().nextBoolean()) {
                writer.deleteDocuments(LongPoint.newRangeQuery("field", 0L, 10L));
            }

            final IndexReader reader = writer.getReader();
            final IndexSearcher searcher = newSearcher(reader);
            writer.close();

            for (int i = 0; i < 100; ++i) {
                final long min = random().nextBoolean() ? Long.MIN_VALUE : TestUtil.nextLong(random(), -100, 10000);
                final long max = random().nextBoolean() ? Long.MAX_VALUE : TestUtil.nextLong(random(), -100, 10000);
                final Query q1 = LongPoint.newRangeQuery("field", min, max);

                final Query fallbackQuery = LongPoint.newRangeQuery("field", min, max);
                final Query q2 = new XIndexSortSortedNumericDocValuesRangeQuery("field", min, max, fallbackQuery);
                final Weight weight1 = q1.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
                final Weight weight2 = q2.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
                assertSameCount(weight1, weight2, searcher);
            }

            reader.close();
            dir.close();
        }
    }

    private void assertSameCount(Weight weight1, Weight weight2, IndexSearcher searcher) throws IOException {
        for (LeafReaderContext context : searcher.getLeafContexts()) {
            assertEquals(weight1.count(context), weight2.count(context));
        }
    }

    public void testCountBoundary() throws IOException {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
        SortField sortField = new SortedNumericSortField(FIELD_NAME, SortField.Type.LONG);
        boolean useLower = random().nextBoolean();
        long lowerValue = 1;
        long upperValue = 100;
        sortField.setMissingValue(useLower ? lowerValue : upperValue);
        Sort indexSort = new Sort(sortField);
        iwc.setIndexSort(indexSort);
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);

        writer.addDocument(createSNDVAndPointDocument(random().nextLong(lowerValue, upperValue)));
        writer.addDocument(createSNDVAndPointDocument(random().nextLong(lowerValue, upperValue)));

        IndexReader reader = writer.getReader();
        IndexSearcher searcher = newSearcher(reader);

        Query fallbackQuery = LongPoint.newRangeQuery(FIELD_NAME, lowerValue, upperValue);
        Query query = new XIndexSortSortedNumericDocValuesRangeQuery(FIELD_NAME, lowerValue, upperValue, fallbackQuery);
        Weight weight = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
        int count = 0;
        for (LeafReaderContext context : searcher.getLeafContexts()) {
            count += weight.count(context);
        }
        assertEquals(2, count);

        writer.close();
        reader.close();
        dir.close();
    }

    private Document createSNDVAndPointDocument(long value) {
        Document doc = new Document();
        doc.add(SortedNumericDocValuesField.indexedField(FIELD_NAME, value));
        doc.add(new LongPoint(FIELD_NAME, value));
        return doc;
    }

    private Document createDocument(long value) {
        Document doc = new Document();
        doc.add(SortedNumericDocValuesField.indexedField(FIELD_NAME, value));
        return doc;
    }

    private Query createQuery(long lowerValue, long upperValue) {
        return new TimestampQuery(lowerValue, upperValue);
    }

}
