/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.queries;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TotalHitCountCollectorManager;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.search.QueryUtils;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.cache.query.TrivialQueryCachingPolicy;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Locale;
import java.util.function.IntFunction;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Fork of Lucene's {@code TestDocValuesQueries} coverage for {@code SortedSetDocValuesRangeQuery},
 * adapted to exercise {@link XSortedSetDocValuesRangeQuery}.
 */
public class XSortedSetDocValuesRangeQueryTests extends ESTestCase {

    private static final int EMPTY_RANGE_EXPECTED_HITS = 10;

    private static Codec docValuesCodec(int skipIntervalSize) {
        return TestUtil.alwaysDocValuesFormat(new Lucene90DocValuesFormat(skipIntervalSize));
    }

    public void testBasics() {
        Query query1 = XSortedSetDocValuesRangeQuery.newSlowExactQuery("field", new BytesRef("foo"));
        Query query2 = XSortedSetDocValuesRangeQuery.newSlowExactQuery("field", new BytesRef("foo"));
        Query query3 = XSortedSetDocValuesRangeQuery.newSlowExactQuery("field", new BytesRef("bar"));
        Query query4 = XSortedSetDocValuesRangeQuery.newSlowRangeQuery("field", new BytesRef("a"), new BytesRef("z"), true, true);
        QueryUtils.check(query1);
        QueryUtils.checkEqual(query1, query2);
        QueryUtils.checkUnequal(query1, query3);
        QueryUtils.checkUnequal(query1, query4);
    }

    public void testRewriteUnboundedRange() throws IOException {
        try (IndexReader reader = new MultiReader()) {
            IndexSearcher searcher = newSearcher(reader);
            Query query = XSortedSetDocValuesRangeQuery.newSlowRangeQuery("field", null, null, true, true);
            assertThat(query.rewrite(searcher), instanceOf(FieldExistsQuery.class));
        }
    }

    /**
     * Fork of Lucene's {@code testSortedSetDocValuesRangeQueryCount}.
     */
    public void testSortedSetDocValuesRangeQueryCount() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
            for (int i = 0; i < 100; i++) {
                String val = String.format(Locale.ROOT, "%03d", i);
                Document doc = new Document();
                doc.add(SortedSetDocValuesField.indexedField("with_index", new BytesRef(val)));
                doc.add(new SortedSetDocValuesField("without_index", new BytesRef(val)));
                if (i != 55) {
                    doc.add(SortedSetDocValuesField.indexedField("sparse", new BytesRef(val)));
                }
                iw.addDocument(doc);
            }
            iw.commit();
            iw.forceMerge(1);

            try (IndexReader reader = iw.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);

                assertCount(
                    searcher,
                    XSortedSetDocValuesRangeQuery.newSlowRangeQuery("nonexistent", new BytesRef("000"), new BytesRef("099"), true, true),
                    0
                );
                assertCount(
                    searcher,
                    XSortedSetDocValuesRangeQuery.newSlowRangeQuery("with_index", new BytesRef("000"), new BytesRef("099"), true, true),
                    100
                );
                assertCount(
                    searcher,
                    XSortedSetDocValuesRangeQuery.newSlowRangeQuery("without_index", new BytesRef("000"), new BytesRef("099"), true, true),
                    -1
                );
                assertCount(
                    searcher,
                    XSortedSetDocValuesRangeQuery.newSlowRangeQuery("with_index", new BytesRef("100"), new BytesRef("199"), true, true),
                    0
                );
            }

            iw.deleteDocuments(
                XSortedSetDocValuesRangeQuery.newSlowRangeQuery("with_index", new BytesRef("020"), new BytesRef("030"), true, true)
            );
            iw.commit();

            try (IndexReader reader = iw.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                assertCount(
                    searcher,
                    XSortedSetDocValuesRangeQuery.newSlowRangeQuery("with_index", new BytesRef("000"), new BytesRef("099"), true, true),
                    89
                );
            }
        }
    }

    /**
     * Fork of Lucene's {@code testPrimarySortDenseSortedDocValuesExactMatch}.
     */
    public void testPrimarySortDenseExactMatch() throws IOException {
        doTestPrimarySortDenseExactMatch(
            SortField.Type.STRING,
            i -> SortedDocValuesField.indexedField("dv", new BytesRef(Integer.toString(i))),
            i -> XSortedSetDocValuesRangeQuery.newSlowRangeQuery(
                "dv",
                new BytesRef(Integer.toString(i)),
                new BytesRef(Integer.toString(i)),
                true,
                true
            )
        );
    }

    /**
     * When the queried field is the primary index sort field and dense, {@link XSortedSetDocValuesRangeQuery}
     * should use {@code XSortedSkipperScorerSupplier} and avoid a two-phase iterator.
     */
    public void testSortedSetRangeQueryOptimizesWithDensePrimarySort() throws IOException {
        Directory dir = newDirectory();
        IndexWriterConfig config = new IndexWriterConfig().setCodec(docValuesCodec(randomIntBetween(4, 16)));
        config.setIndexSort(new Sort(new SortField("secondary", SortField.Type.STRING)));
        IndexWriter iw = new IndexWriter(dir, config);
        for (int i = 0; i < 10; i++) {
            Document doc = new Document();
            doc.add(SortedDocValuesField.indexedField("secondary", new BytesRef(String.format(Locale.ROOT, "%03d", i))));
            iw.addDocument(doc);
        }
        iw.forceMerge(1);
        iw.close();
        DirectoryReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = new IndexSearcher(reader);

        Query query = XSortedSetDocValuesRangeQuery.newSlowRangeQuery("secondary", new BytesRef("003"), new BytesRef("007"), true, true);
        Weight weight = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
        Scorer scorer = weight.scorer(reader.leaves().get(0));
        assertNotNull(scorer);
        assertThat("XSortedSkipperScorerSupplier should be used for dense primary sort", scorer.twoPhaseIterator(), nullValue());

        reader.close();
        dir.close();
    }

    /**
     * Fork of Lucene's {@code testSortedSetRangeQueryDoesNotOptimizeWithActivePrimary}.
     */
    public void testSortedSetRangeQueryDoesNotOptimizeWithActivePrimary() throws IOException {
        Directory dir = newDirectory();
        IndexWriterConfig config = new IndexWriterConfig().setCodec(docValuesCodec(randomIntBetween(4, 16)));
        config.setIndexSort(new Sort(new SortField("primary", SortField.Type.STRING), new SortField("secondary", SortField.Type.STRING)));
        IndexWriter iw = new IndexWriter(dir, config);
        for (int i = 0; i < 10; i++) {
            String val = String.format(Locale.ROOT, "%03d", i);
            Document doc = new Document();
            doc.add(SortedDocValuesField.indexedField("primary", new BytesRef(val)));
            doc.add(SortedDocValuesField.indexedField("secondary", new BytesRef(val)));
            iw.addDocument(doc);
        }
        iw.forceMerge(1);
        iw.close();
        DirectoryReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = new IndexSearcher(reader);

        Query query = XSortedSetDocValuesRangeQuery.newSlowRangeQuery("secondary", new BytesRef("003"), new BytesRef("007"), true, true);
        Weight weight = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
        Scorer scorer = weight.scorer(reader.leaves().get(0));
        assertNotNull(scorer);
        assertNotNull("TwoPhaseIterator fallback should be used when secondary is not the effective primary", scorer.twoPhaseIterator());

        reader.close();
        dir.close();
    }

    public void testLuceneFailureIsFixedWithWorkaround() throws IOException {
        Directory dir = newDirectory();
        IndexWriterConfig config = new IndexWriterConfig();// .setCodec(docValuesCodec(4096));
        config.setIndexSort(new Sort(new SortedSetSortField("slice", false)));
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, config);

        for (int i = 0; i < 128; i++) {
            iw.addDocument(doc("aaa", "no"));
        }
        for (int i = 0; i < 8192; i++) {
            iw.addDocument(doc("src", i % 819 == 818 ? "no" : "yes"));
        }
        for (int i = 0; i < 128; i++) {
            iw.addDocument(doc("zzz", "no"));
        }
        iw.commit();
        iw.forceMerge(1);

        try (IndexReader reader = iw.getReader()) {
            ContextIndexSearcher searcher = new ContextIndexSearcher(reader, null, null, TrivialQueryCachingPolicy.NEVER, true);
            searcher.addQueryCancellation(() -> {});
            // fail
            {
                Query query = new BooleanQuery.Builder().add(
                    SortedSetDocValuesField.newSlowExactQuery("slice", new BytesRef("src")),
                    Occur.FILTER
                ).add(new TermQuery(new Term("excluded", "yes")), Occur.MUST_NOT).build();
                expectThrows(
                    IllegalArgumentException.class,
                    "This means that lucene #16546 has been fixed and the workaround can be removed",
                    () -> searcher.search(query, new TotalHitCountCollectorManager(searcher.getSlices()))
                );
            }
            {
                Query query = new BooleanQuery.Builder().add(
                    XSortedSetDocValuesRangeQuery.newSlowExactQuery("slice", new BytesRef("src")),
                    Occur.FILTER
                ).add(new TermQuery(new Term("excluded", "yes")), Occur.MUST_NOT).build();

                int hits = searcher.search(query, new TotalHitCountCollectorManager(searcher.getSlices()));
                assertEquals(EMPTY_RANGE_EXPECTED_HITS, hits);
            }
        }
        iw.close();
        dir.close();
    }

    private void doTestPrimarySortDenseExactMatch(SortField.Type type, IntFunction<IndexableField> fields, IntFunction<Query> queries)
        throws IOException {
        boolean deletes = randomBoolean();
        Directory dir = newDirectory();
        int skipIntervalSize = randomIntBetween(4, 4096);
        IndexWriterConfig config = new IndexWriterConfig().setCodec(docValuesCodec(skipIntervalSize));
        config.setIndexSort(new Sort(new SortField("dv", type, randomBoolean())));
        int numBlocks = randomIntBetween(4, 16);
        int[] sizes = new int[numBlocks];
        for (int i = 0; i < numBlocks; i++) {
            sizes[i] = randomIntBetween(1, 250);
        }
        int[] totalSizes = new int[numBlocks];
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, config);
        for (int i = 0; i < numBlocks; i++) {
            totalSizes[i] = sizes[i];
            for (int j = 0; j < sizes[i]; j++) {
                Document doc = new Document();
                IndexableField dv = fields.apply(i);
                doc.add(dv);
                iw.addDocument(doc);
                if (deletes && randomInt(10) == 0) {
                    doc = new Document();
                    doc.add(dv);
                    doc.add(new StringField("id", "to_delete", Field.Store.NO));
                    iw.addDocument(doc);
                    totalSizes[i]++;
                }
            }
        }
        iw.commit();
        iw.forceMerge(1);

        if (deletes) {
            iw.deleteDocuments(new TermQuery(new Term("id", "to_delete")));
        }

        final IndexReader reader = iw.getReader();
        final IndexSearcher searcher = newSearcher(reader, false);
        iw.close();

        for (int i = 0; i < numBlocks; i++) {
            final Query q = queries.apply(i);
            assertEquals(sizes[i], searcher.count(q));
            assertEquals(sizes[i], searcher.search(q, 1000).totalHits.value());
            assertEquals(1, reader.leaves().size());
            LeafReaderContext ctx = reader.leaves().get(0);
            Query rewritten = searcher.rewrite(q);
            Weight weight = rewritten.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1.0f);
            ScorerSupplier supplier = weight.scorerSupplier(ctx);
            assertThat(supplier.cost(), greaterThanOrEqualTo((long) sizes[i]));
            assertThat(supplier.cost(), lessThanOrEqualTo(totalSizes[i] + 2L * skipIntervalSize));
        }
        reader.close();
        dir.close();
    }

    private static Document doc(String slice, String excluded) {
        Document doc = new Document();
        doc.add(SortedSetDocValuesField.indexedField("slice", new BytesRef(slice)));
        doc.add(new StringField("excluded", excluded, Field.Store.NO));
        return doc;
    }

    private static void assertCount(IndexSearcher searcher, Query query, int expectedCount) throws IOException {
        Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1.0f);
        assertEquals(expectedCount, weight.count(searcher.getIndexReader().leaves().getFirst()));
    }
}
