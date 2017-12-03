/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.query;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.MinDocQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.FilterCollector;
import org.apache.lucene.search.FilterLeafCollector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.internal.ScrollContext;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.test.TestSearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;

public class QueryPhaseTests extends IndexShardTestCase {

    private IndexShard indexShard;

    @Override
    public Settings threadPoolSettings() {
        return Settings.builder().put(super.threadPoolSettings()).put("thread_pool.search.min_queue_size", 10).build();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        indexShard = newShard(true);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        closeShards(indexShard);
    }

    private void countTestCase(Query query, IndexReader reader, boolean shouldCollect) throws Exception {
        TestSearchContext context = new TestSearchContext(null, indexShard);
        context.parsedQuery(new ParsedQuery(query));
        context.setSize(0);
        context.setTask(new SearchTask(123L, "", "", "", null));

        IndexSearcher searcher = new IndexSearcher(reader);
        final AtomicBoolean collected = new AtomicBoolean();
        IndexSearcher contextSearcher = new IndexSearcher(reader) {
            protected void search(List<LeafReaderContext> leaves, Weight weight, Collector collector) throws IOException {
                collected.set(true);
                super.search(leaves, weight, collector);
            }
        };

        final boolean rescore = QueryPhase.execute(context, contextSearcher, checkCancelled -> {}, null);
        assertFalse(rescore);
        assertEquals(searcher.count(query), context.queryResult().topDocs().totalHits);
        assertEquals(shouldCollect, collected.get());
    }

    private void countTestCase(boolean withDeletions) throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
        final int numDocs = scaledRandomIntBetween(100, 200);
        for (int i = 0; i < numDocs; ++i) {
            Document doc = new Document();
            if (randomBoolean()) {
                doc.add(new StringField("foo", "bar", Store.NO));
            }
            if (randomBoolean()) {
                doc.add(new StringField("foo", "baz", Store.NO));
            }
            if (withDeletions && (rarely() || i == 0)) {
                doc.add(new StringField("delete", "yes", Store.NO));
            }
            w.addDocument(doc);
        }
        if (withDeletions) {
            w.deleteDocuments(new Term("delete", "yes"));
        }
        final IndexReader reader = w.getReader();
        Query matchAll = new MatchAllDocsQuery();
        Query matchAllCsq = new ConstantScoreQuery(matchAll);
        Query tq = new TermQuery(new Term("foo", "bar"));
        Query tCsq = new ConstantScoreQuery(tq);
        BooleanQuery bq = new BooleanQuery.Builder()
            .add(matchAll, Occur.SHOULD)
            .add(tq, Occur.MUST)
            .build();

        countTestCase(matchAll, reader, false);
        countTestCase(matchAllCsq, reader, false);
        countTestCase(tq, reader, withDeletions);
        countTestCase(tCsq, reader, withDeletions);
        countTestCase(bq, reader, true);
        reader.close();
        w.close();
        dir.close();
    }

    public void testCountWithoutDeletions() throws Exception {
        countTestCase(false);
    }

    public void testCountWithDeletions() throws Exception {
        countTestCase(true);
    }

    public void testPostFilterDisablesCountOptimization() throws Exception {
        TestSearchContext context = new TestSearchContext(null, indexShard);
        context.parsedQuery(new ParsedQuery(new MatchAllDocsQuery()));
        context.setSize(0);
        context.setTask(new SearchTask(123L, "", "", "", null));

        final AtomicBoolean collected = new AtomicBoolean();
        IndexSearcher contextSearcher = new IndexSearcher(new MultiReader()) {
            protected void search(List<LeafReaderContext> leaves, Weight weight, Collector collector) throws IOException {
                collected.set(true);
                super.search(leaves, weight, collector);
            }
        };

        QueryPhase.execute(context, contextSearcher, checkCancelled -> {}, null);
        assertEquals(0, context.queryResult().topDocs().totalHits);
        assertFalse(collected.get());

        context.parsedPostFilter(new ParsedQuery(new MatchNoDocsQuery()));
        QueryPhase.execute(context, contextSearcher, checkCancelled -> {}, null);
        assertEquals(0, context.queryResult().topDocs().totalHits);
        assertTrue(collected.get());
    }

    public void testMinScoreDisablesCountOptimization() throws Exception {
        TestSearchContext context = new TestSearchContext(null, indexShard);
        context.parsedQuery(new ParsedQuery(new MatchAllDocsQuery()));
        context.setSize(0);
        context.setTask(new SearchTask(123L, "", "", "", null));

        final AtomicBoolean collected = new AtomicBoolean();
        IndexSearcher contextSearcher = new IndexSearcher(new MultiReader()) {
            protected void search(List<LeafReaderContext> leaves, Weight weight, Collector collector) throws IOException {
                collected.set(true);
                super.search(leaves, weight, collector);
            }
        };

        QueryPhase.execute(context, contextSearcher, checkCancelled -> {}, null);
        assertEquals(0, context.queryResult().topDocs().totalHits);
        assertFalse(collected.get());

        context.minimumScore(1);
        QueryPhase.execute(context, contextSearcher, checkCancelled -> {}, null);
        assertEquals(0, context.queryResult().topDocs().totalHits);
        assertTrue(collected.get());
    }

    public void testQueryCapturesThreadPoolStats() throws Exception {
        TestSearchContext context = new TestSearchContext(null, indexShard);
        context.setTask(new SearchTask(123L, "", "", "", null));
        context.parsedQuery(new ParsedQuery(new MatchAllDocsQuery()));

        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
        final int numDocs = scaledRandomIntBetween(100, 200);
        for (int i = 0; i < numDocs; ++i) {
            w.addDocument(new Document());
        }
        w.close();
        IndexReader reader = DirectoryReader.open(dir);
        IndexSearcher contextSearcher = new IndexSearcher(reader);

        QueryPhase.execute(context, contextSearcher, checkCancelled -> {}, null);
        QuerySearchResult results = context.queryResult();
        assertThat(results.serviceTimeEWMA(), greaterThanOrEqualTo(0L));
        assertThat(results.nodeQueueSize(), greaterThanOrEqualTo(0));
        reader.close();
        dir.close();
    }

    public void testInOrderScrollOptimization() throws Exception {
        Directory dir = newDirectory();
        final Sort sort = new Sort(new SortField("rank", SortField.Type.INT));
        IndexWriterConfig iwc = newIndexWriterConfig()
            .setIndexSort(sort);
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
        final int numDocs = scaledRandomIntBetween(100, 200);
        for (int i = 0; i < numDocs; ++i) {
            w.addDocument(new Document());
        }
        w.close();
        final AtomicBoolean collected = new AtomicBoolean();
        IndexReader reader = DirectoryReader.open(dir);
        IndexSearcher contextSearcher = new IndexSearcher(reader) {
            protected void search(List<LeafReaderContext> leaves, Weight weight, Collector collector) throws IOException {
                collected.set(true);
                super.search(leaves, weight, collector);
            }
        };

        TestSearchContext context = new TestSearchContext(null, indexShard);
        context.parsedQuery(new ParsedQuery(new MatchAllDocsQuery()));
        ScrollContext scrollContext = new ScrollContext();
        scrollContext.lastEmittedDoc = null;
        scrollContext.maxScore = Float.NaN;
        scrollContext.totalHits = -1;
        context.scrollContext(scrollContext);
        context.setTask(new SearchTask(123L, "", "", "", null));
        context.setSize(10);

        QueryPhase.execute(context, contextSearcher, checkCancelled -> {}, null);
        assertThat(context.queryResult().topDocs().totalHits, equalTo((long) numDocs));
        assertTrue(collected.get());
        assertNull(context.queryResult().terminatedEarly());
        assertThat(context.terminateAfter(), equalTo(0));
        assertThat(context.queryResult().getTotalHits(), equalTo((long) numDocs));

        QueryPhase.execute(context, contextSearcher, checkCancelled -> {}, null);
        assertThat(context.queryResult().topDocs().totalHits, equalTo((long) numDocs));
        assertTrue(collected.get());
        assertTrue(context.queryResult().terminatedEarly());
        assertThat(context.terminateAfter(), equalTo(10));
        assertThat(context.queryResult().getTotalHits(), equalTo((long) numDocs));
        assertThat(context.queryResult().topDocs().scoreDocs[0].doc, greaterThanOrEqualTo(10));
        reader.close();
        dir.close();
    }

    public void testTerminateAfterEarlyTermination() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
        final int numDocs = scaledRandomIntBetween(100, 200);
        for (int i = 0; i < numDocs; ++i) {
            Document doc = new Document();
            if (randomBoolean()) {
                doc.add(new StringField("foo", "bar", Store.NO));
            }
            if (randomBoolean()) {
                doc.add(new StringField("foo", "baz", Store.NO));
            }
            doc.add(new NumericDocValuesField("rank", numDocs - i));
            w.addDocument(doc);
        }
        w.close();
        TestSearchContext context = new TestSearchContext(null, indexShard);
        context.setTask(new SearchTask(123L, "", "", "", null));
        context.parsedQuery(new ParsedQuery(new MatchAllDocsQuery()));
        context.terminateAfter(1);

        final AtomicBoolean collected = new AtomicBoolean();
        final IndexReader reader = DirectoryReader.open(dir);
        IndexSearcher contextSearcher = new IndexSearcher(reader) {
            protected void search(List<LeafReaderContext> leaves, Weight weight, Collector collector) throws IOException {
                collected.set(true);
                super.search(leaves, weight, collector);
            }
        };

        {
            context.setSize(1);
            QueryPhase.execute(context, contextSearcher, checkCancelled -> {}, null);
            assertTrue(collected.get());
            assertTrue(context.queryResult().terminatedEarly());
            assertThat(context.queryResult().topDocs().totalHits, equalTo(1L));
            assertThat(context.queryResult().topDocs().scoreDocs.length, equalTo(1));

            context.setSize(0);
            QueryPhase.execute(context, contextSearcher, checkCancelled -> {}, null);
            assertTrue(collected.get());
            assertTrue(context.queryResult().terminatedEarly());
            assertThat(context.queryResult().topDocs().totalHits, equalTo(1L));
            assertThat(context.queryResult().topDocs().scoreDocs.length, equalTo(0));
        }

        {
            context.setSize(1);
            QueryPhase.execute(context, contextSearcher, checkCancelled -> {}, null);
            assertTrue(collected.get());
            assertTrue(context.queryResult().terminatedEarly());
            assertThat(context.queryResult().topDocs().totalHits, equalTo(1L));
            assertThat(context.queryResult().topDocs().scoreDocs.length, equalTo(1));
        }
        {
            context.setSize(1);
            BooleanQuery bq = new BooleanQuery.Builder()
                .add(new TermQuery(new Term("foo", "bar")), Occur.SHOULD)
                .add(new TermQuery(new Term("foo", "baz")), Occur.SHOULD)
                .build();
            context.parsedQuery(new ParsedQuery(bq));
            collected.set(false);
            QueryPhase.execute(context, contextSearcher, checkCancelled -> {}, null);
            assertTrue(collected.get());
            assertTrue(context.queryResult().terminatedEarly());
            assertThat(context.queryResult().topDocs().totalHits, equalTo(1L));
            assertThat(context.queryResult().topDocs().scoreDocs.length, equalTo(1));

            context.setSize(0);
            context.parsedQuery(new ParsedQuery(bq));
            collected.set(false);
            QueryPhase.execute(context, contextSearcher, checkCancelled -> {}, null);
            assertTrue(collected.get());
            assertTrue(context.queryResult().terminatedEarly());
            assertThat(context.queryResult().topDocs().totalHits, equalTo(1L));
            assertThat(context.queryResult().topDocs().scoreDocs.length, equalTo(0));
        }
        {
            context.setSize(1);
            collected.set(false);
            TotalHitCountCollector collector = new TotalHitCountCollector();
            context.queryCollectors().put(TotalHitCountCollector.class, collector);
            QueryPhase.execute(context, contextSearcher, checkCancelled -> {}, null);
            assertTrue(collected.get());
            assertTrue(context.queryResult().terminatedEarly());
            assertThat(context.queryResult().topDocs().totalHits, equalTo(1L));
            assertThat(context.queryResult().topDocs().scoreDocs.length, equalTo(1));
        }
        {
            context.setSize(0);
            collected.set(false);
            TotalHitCountCollector collector = new TotalHitCountCollector();
            context.queryCollectors().put(TotalHitCountCollector.class, collector);
            QueryPhase.execute(context, contextSearcher, checkCancelled -> {}, null);
            assertTrue(collected.get());
            assertTrue(context.queryResult().terminatedEarly());
            assertThat(context.queryResult().topDocs().totalHits, equalTo(1L));
            assertThat(context.queryResult().topDocs().scoreDocs.length, equalTo(0));
            assertThat(collector.getTotalHits(), equalTo(1));
        }

        reader.close();
        dir.close();
    }

    public void testIndexSortingEarlyTermination() throws Exception {
        Directory dir = newDirectory();
        final Sort sort = new Sort(new SortField("rank", SortField.Type.INT));
        IndexWriterConfig iwc = newIndexWriterConfig()
            .setIndexSort(sort);
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
        final int numDocs = scaledRandomIntBetween(100, 200);
        for (int i = 0; i < numDocs; ++i) {
            Document doc = new Document();
            if (randomBoolean()) {
                doc.add(new StringField("foo", "bar", Store.NO));
            }
            if (randomBoolean()) {
                doc.add(new StringField("foo", "baz", Store.NO));
            }
            doc.add(new NumericDocValuesField("rank", numDocs - i));
            w.addDocument(doc);
        }
        w.close();

        TestSearchContext context = new TestSearchContext(null, indexShard);
        context.parsedQuery(new ParsedQuery(new MatchAllDocsQuery()));
        context.setSize(1);
        context.setTask(new SearchTask(123L, "", "", "", null));
        context.sort(new SortAndFormats(sort, new DocValueFormat[] {DocValueFormat.RAW}));

        final IndexReader reader = DirectoryReader.open(dir);
        IndexSearcher contextSearcher = new IndexSearcher(reader);
        QueryPhase.execute(context, contextSearcher, checkCancelled -> {}, sort);
        assertThat(context.queryResult().topDocs().totalHits, equalTo((long) numDocs));
        assertThat(context.queryResult().topDocs().scoreDocs.length, equalTo(1));
        assertThat(context.queryResult().topDocs().scoreDocs[0], instanceOf(FieldDoc.class));
        FieldDoc fieldDoc = (FieldDoc) context.queryResult().topDocs().scoreDocs[0];
        assertThat(fieldDoc.fields[0], equalTo(1));

        {
            context.parsedPostFilter(new ParsedQuery(new MinDocQuery(1)));
            QueryPhase.execute(context, contextSearcher, checkCancelled -> {}, sort);
            assertNull(context.queryResult().terminatedEarly());
            assertThat(context.queryResult().topDocs().totalHits, equalTo(numDocs - 1L));
            assertThat(context.queryResult().topDocs().scoreDocs.length, equalTo(1));
            assertThat(context.queryResult().topDocs().scoreDocs[0], instanceOf(FieldDoc.class));
            assertThat(fieldDoc.fields[0], anyOf(equalTo(1), equalTo(2)));
            context.parsedPostFilter(null);

            final TotalHitCountCollector totalHitCountCollector = new TotalHitCountCollector();
            context.queryCollectors().put(TotalHitCountCollector.class, totalHitCountCollector);
            QueryPhase.execute(context, contextSearcher, checkCancelled -> {}, sort);
            assertNull(context.queryResult().terminatedEarly());
            assertThat(context.queryResult().topDocs().totalHits, equalTo((long) numDocs));
            assertThat(context.queryResult().topDocs().scoreDocs.length, equalTo(1));
            assertThat(context.queryResult().topDocs().scoreDocs[0], instanceOf(FieldDoc.class));
            assertThat(fieldDoc.fields[0], anyOf(equalTo(1), equalTo(2)));
            assertThat(totalHitCountCollector.getTotalHits(), equalTo(numDocs));
            context.queryCollectors().clear();
        }

        {
            contextSearcher = getAssertingEarlyTerminationSearcher(reader, 1);
            context.trackTotalHits(false);
            QueryPhase.execute(context, contextSearcher, checkCancelled -> {}, sort);
            assertNull(context.queryResult().terminatedEarly());
            assertThat(context.queryResult().topDocs().scoreDocs.length, equalTo(1));
            assertThat(context.queryResult().topDocs().scoreDocs[0], instanceOf(FieldDoc.class));
            assertThat(fieldDoc.fields[0], anyOf(equalTo(1), equalTo(2)));

            QueryPhase.execute(context, contextSearcher, checkCancelled -> {}, sort);
            assertNull(context.queryResult().terminatedEarly());
            assertThat(context.queryResult().topDocs().scoreDocs.length, equalTo(1));
            assertThat(context.queryResult().topDocs().scoreDocs[0], instanceOf(FieldDoc.class));
            assertThat(fieldDoc.fields[0], anyOf(equalTo(1), equalTo(2)));
        }
        reader.close();
        dir.close();
    }

    public void testIndexSortScrollOptimization() throws Exception {
        Directory dir = newDirectory();
        final Sort indexSort = new Sort(
            new SortField("rank", SortField.Type.INT),
            new SortField("tiebreaker", SortField.Type.INT)
        );
        IndexWriterConfig iwc = newIndexWriterConfig().setIndexSort(indexSort);
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
        final int numDocs = scaledRandomIntBetween(100, 200);
        for (int i = 0; i < numDocs; ++i) {
            Document doc = new Document();
            doc.add(new NumericDocValuesField("rank", random().nextInt()));
            doc.add(new NumericDocValuesField("tiebreaker", i));
            w.addDocument(doc);
        }
        if (randomBoolean()) {
            w.forceMerge(randomIntBetween(1, 10));
        }
        w.close();

        final IndexReader reader = DirectoryReader.open(dir);
        List<SortAndFormats> searchSortAndFormats = new ArrayList<>();
        searchSortAndFormats.add(new SortAndFormats(indexSort, new DocValueFormat[]{DocValueFormat.RAW, DocValueFormat.RAW}));
        // search sort is a prefix of the index sort
        searchSortAndFormats.add(new SortAndFormats(new Sort(indexSort.getSort()[0]), new DocValueFormat[]{DocValueFormat.RAW}));
        for (SortAndFormats searchSortAndFormat : searchSortAndFormats) {
            IndexSearcher contextSearcher = new IndexSearcher(reader);
            TestSearchContext context = new TestSearchContext(null, indexShard);
            context.parsedQuery(new ParsedQuery(new MatchAllDocsQuery()));
            ScrollContext scrollContext = new ScrollContext();
            scrollContext.lastEmittedDoc = null;
            scrollContext.maxScore = Float.NaN;
            scrollContext.totalHits = -1;
            context.scrollContext(scrollContext);
            context.setTask(new SearchTask(123L, "", "", "", null));
            context.setSize(10);
            context.sort(searchSortAndFormat);

            QueryPhase.execute(context, contextSearcher, checkCancelled -> {}, searchSortAndFormat.sort);
            assertThat(context.queryResult().topDocs().totalHits, equalTo((long) numDocs));
            assertNull(context.queryResult().terminatedEarly());
            assertThat(context.terminateAfter(), equalTo(0));
            assertThat(context.queryResult().getTotalHits(), equalTo((long) numDocs));
            int sizeMinus1 = context.queryResult().topDocs().scoreDocs.length - 1;
            FieldDoc lastDoc = (FieldDoc) context.queryResult().topDocs().scoreDocs[sizeMinus1];

            contextSearcher = getAssertingEarlyTerminationSearcher(reader, 10);
            QueryPhase.execute(context, contextSearcher, checkCancelled -> {}, searchSortAndFormat.sort);
            assertNull(context.queryResult().terminatedEarly());
            assertThat(context.queryResult().topDocs().totalHits, equalTo((long) numDocs));
            assertThat(context.terminateAfter(), equalTo(0));
            assertThat(context.queryResult().getTotalHits(), equalTo((long) numDocs));
            FieldDoc firstDoc = (FieldDoc) context.queryResult().topDocs().scoreDocs[0];
            for (int i = 0; i < searchSortAndFormat.sort.getSort().length; i++) {
                @SuppressWarnings("unchecked")
                FieldComparator<Object> comparator = (FieldComparator<Object>) searchSortAndFormat.sort.getSort()[i].getComparator(1, i);
                int cmp = comparator.compareValues(firstDoc.fields[i], lastDoc.fields[i]);
                if (cmp == 0) {
                    continue;
                }
                assertThat(cmp, equalTo(1));
                break;
            }
        }
        reader.close();
        dir.close();
    }

    static IndexSearcher getAssertingEarlyTerminationSearcher(IndexReader reader, int size) {
        return new IndexSearcher(reader) {
            protected void search(List<LeafReaderContext> leaves, Weight weight, Collector collector) throws IOException {
                final Collector in = new AssertingEalyTerminationFilterCollector(collector, size);
                super.search(leaves, weight, in);
            }
        };
    }

    private static class AssertingEalyTerminationFilterCollector extends FilterCollector {
        private final int size;

        AssertingEalyTerminationFilterCollector(Collector in, int size) {
            super(in);
            this.size = size;
        }

        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
            final LeafCollector in = super.getLeafCollector(context);
            return new FilterLeafCollector(in) {
                int collected;

                @Override
                public void collect(int doc) throws IOException {
                    assert collected <= size : "should not collect more than " + size + " doc per segment, got " + collected;
                    ++ collected;
                    super.collect(doc);
                }
            };
        }
    }
}
