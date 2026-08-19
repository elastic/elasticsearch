/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class PointRangeBreakerWeightTests extends ESTestCase {

    private static final String FIELD = "f";
    private static final String KEYWORD_FIELD = "k";
    private static final String RARE_TERM = "rare";
    private static final int RARE_DOCS = 5;
    private static final int NUM_DOCS = 2000;

    private Directory directory;
    private DirectoryReader reader;

    @Before
    public void initDirectoryAndReader() throws Exception {
        directory = newDirectory();
        try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(null))) {
            for (int docId = 0; docId < NUM_DOCS; docId++) {
                Document doc = new Document();
                doc.add(new LongPoint(FIELD, docId));
                doc.add(new SortedNumericDocValuesField(FIELD, docId));
                // A rare indexed term on the lowest doc ids gives a cheap, low-cost lead clause for the
                // conjunction tests that drive IndexOrDocValuesQuery onto its doc-values branch.
                if (docId < RARE_DOCS) {
                    doc.add(new StringField(KEYWORD_FIELD, RARE_TERM, Field.Store.NO));
                }
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
        reader = DirectoryReader.open(directory);
    }

    @After
    public void closeDirectoryAndReader() throws Exception {
        IOUtils.close(reader, directory);
    }

    public void testDenseRangeChargesAndReleasesAcrossSearch() throws IOException {
        Query dense = LongPoint.newRangeQuery(FIELD, 0L, (NUM_DOCS * 3 / 4));
        assertChargesThenReleases(dense);
    }

    public void testIndexOrDocValuesRangeChargesAndReleasesAcrossSearch() throws IOException {
        Query indexQuery = LongPoint.newRangeQuery(FIELD, 0L, (NUM_DOCS * 3 / 4));
        Query dvQuery = SortedNumericDocValuesField.newSlowRangeQuery(FIELD, 0L, NUM_DOCS * 3L / 4L);
        assertChargesThenReleases(new IndexOrDocValuesQuery(indexQuery, dvQuery));
    }

    public void testMatchAllRangeChargesNothing() throws IOException {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
        runSearch(LongPoint.newRangeQuery(FIELD, Long.MIN_VALUE, Long.MAX_VALUE), breaker);
        assertThat("a match-all range allocates no result bitset", breaker.peak(), equalTo(0L));
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testExpensiveRangeTripsBreakerWithoutLeaking() throws IOException {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(100L);
        Query dense = LongPoint.newRangeQuery(FIELD, 0L, (long) (NUM_DOCS * 3 / 4));
        expectThrows(CircuitBreakingException.class, () -> runSearch(dense, breaker));
        assertThat("a tripped reservation must not leak onto the breaker", breaker.getUsed(), equalTo(0L));
    }

    public void testPlainRangeInConjunctionChargesViaScorerGetPath() throws IOException {
        Query points = LongPoint.newRangeQuery(FIELD, 0L, (NUM_DOCS * 3 / 4));
        Query dv = SortedNumericDocValuesField.newSlowRangeQuery(FIELD, 0L, NUM_DOCS * 3L / 4L);
        assertChargesThenReleases(conjunction(points, dv));
    }

    public void testIndexOrDocValuesInConjunctionChargesWhenPointsBranchSelected() throws IOException {
        Query indexOrDocValues = new IndexOrDocValuesQuery(
            LongPoint.newRangeQuery(FIELD, 0L, (NUM_DOCS * 3 / 4)),
            SortedNumericDocValuesField.newSlowRangeQuery(FIELD, 0L, NUM_DOCS * 3L / 4L)
        );
        Query lead = SortedNumericDocValuesField.newSlowRangeQuery(FIELD, 0L, NUM_DOCS * 3L / 4L);
        assertChargesThenReleases(conjunction(indexOrDocValues, lead));
    }

    public void testIndexOrDocValuesInConjunctionSkipsChargeWhenDocValuesBranchSelected() throws IOException {
        Query indexOrDocValues = new IndexOrDocValuesQuery(
            LongPoint.newRangeQuery(FIELD, 0L, (NUM_DOCS * 3 / 4)),
            SortedNumericDocValuesField.newSlowRangeQuery(FIELD, 0L, NUM_DOCS * 3L / 4L)
        );
        Query rareLead = new TermQuery(new Term(KEYWORD_FIELD, RARE_TERM));
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
        int hits = runSearch(conjunction(rareLead, indexOrDocValues), breaker);
        assertThat("the selective lead clause must still match documents so the scorer runs", hits, greaterThan(0));
        assertThat("the doc-values branch allocates no result bitset, so nothing is charged", breaker.peak(), equalTo(0L));
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testOutOfBandChargeReleasedOnClose() throws IOException {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
        ContextIndexSearcher searcher = new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            null,
            IndexSearcher.getDefaultQueryCachingPolicy(),
            false
        );
        searcher.setCircuitBreaker(breaker);
        Query dense = LongPoint.newRangeQuery(FIELD, 0L, (NUM_DOCS * 3 / 4));
        Weight weight = searcher.createWeight(searcher.rewrite(dense), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        for (LeafReaderContext leaf : reader.leaves()) {
            ScorerSupplier scorerSupplier = weight.scorerSupplier(leaf);
            if (scorerSupplier != null) {
                scorerSupplier.get(Long.MAX_VALUE);
            }
        }
        assertThat("the out-of-band scorer must charge execution RAM", breaker.getUsed(), greaterThan(0L));
        searcher.close();
        assertThat("closing the searcher must release the residual out-of-band charge", breaker.getUsed(), equalTo(0L));
    }

    public void testOutOfBandChargeOnAnotherThreadReleasedOnClose() throws Exception {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
        ContextIndexSearcher searcher = new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            null,
            IndexSearcher.getDefaultQueryCachingPolicy(),
            false
        );
        searcher.setCircuitBreaker(breaker);
        Query dense = LongPoint.newRangeQuery(FIELD, 0L, (NUM_DOCS * 3 / 4));
        Weight weight = searcher.createWeight(searcher.rewrite(dense), ScoreMode.COMPLETE_NO_SCORES, 1.0f);

        try (ExecutorService worker = Executors.newSingleThreadExecutor()) {
            worker.submit(() -> {
                for (LeafReaderContext leaf : reader.leaves()) {
                    ScorerSupplier scorerSupplier = weight.scorerSupplier(leaf);
                    if (scorerSupplier != null) {
                        // Force the points branch the way KNN materialises the filter bitset, outside any searchLeaf scope.
                        scorerSupplier.get(Long.MAX_VALUE);
                    }
                }
                return null;
            }).get();
        }

        assertThat("the worker thread's out-of-band scorer must charge execution RAM", breaker.getUsed(), greaterThan(0L));
        // close() runs on the test thread, which never charged anything, so its own thread-local is empty;
        // the release must come from the shared cross-thread counter.
        searcher.close();
        assertThat("closing on a different thread must still release the worker thread's charge", breaker.getUsed(), equalTo(0L));
    }

    private static Query conjunction(Query first, Query second) {
        return new BooleanQuery.Builder().add(first, BooleanClause.Occur.MUST).add(second, BooleanClause.Occur.MUST).build();
    }

    private void assertChargesThenReleases(Query query) throws IOException {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
        int hits = runSearch(query, breaker);
        assertThat("the range must match documents so its scorer actually runs", hits, greaterThan(0));
        assertThat("the point-range scorer must charge execution RAM while it runs", breaker.peak(), greaterThan(0L));
        assertThat("the per-leaf execution charge must be released once the leaf is scored", breaker.getUsed(), equalTo(0L));
    }

    private int runSearch(Query query, CircuitBreaker breaker) throws IOException {
        ContextIndexSearcher searcher = new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            null,
            IndexSearcher.getDefaultQueryCachingPolicy(),
            false
        );
        searcher.setCircuitBreaker(breaker);
        return searcher.search(query, new CountingCollectorManager());
    }

    private static final class TrackingCircuitBreaker extends NoopCircuitBreaker {
        private final long limit;
        private final AtomicLong used = new AtomicLong();
        private final AtomicLong peak = new AtomicLong();

        TrackingCircuitBreaker(long limit) {
            super("request");
            this.limit = limit;
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
            long current = used.addAndGet(bytes);
            if (limit >= 0 && current > limit) {
                used.addAndGet(-bytes);
                throw new CircuitBreakingException("test breaker tripped", bytes, limit, Durability.TRANSIENT);
            }
            peak.accumulateAndGet(current, Math::max);
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            used.addAndGet(bytes);
        }

        @Override
        public long getUsed() {
            return used.get();
        }

        @Override
        public long getLimit() {
            return limit;
        }

        long peak() {
            return peak.get();
        }
    }

    private static final class CountingCollectorManager implements CollectorManager<CountingCollector, Integer> {
        @Override
        public CountingCollector newCollector() {
            return new CountingCollector();
        }

        @Override
        public Integer reduce(Collection<CountingCollector> collectors) {
            int total = 0;
            for (CountingCollector collector : collectors) {
                total += collector.count;
            }
            return total;
        }
    }

    private static final class CountingCollector implements Collector {
        private int count;

        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) {
            return new LeafCollector() {
                @Override
                public void setScorer(Scorable scorer) {}

                @Override
                public void collect(int doc) {
                    count++;
                }
            };
        }

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE_NO_SCORES;
        }
    }
}
