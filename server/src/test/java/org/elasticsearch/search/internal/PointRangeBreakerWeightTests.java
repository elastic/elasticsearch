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
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
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
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class PointRangeBreakerWeightTests extends ESTestCase {

    private static final String FIELD = "f";
    private static final String VECTOR_FIELD = "v";
    private static final int VECTOR_DIMS = 3;
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
        assertChargesThenReleases(denseRangeQuery());
    }

    public void testIndexOrDocValuesRangeChargesAndReleasesAcrossSearch() throws IOException {
        Query dvQuery = SortedNumericDocValuesField.newSlowRangeQuery(FIELD, 0L, NUM_DOCS * 3L / 4L);
        assertChargesThenReleases(new IndexOrDocValuesQuery(denseRangeQuery(), dvQuery));
    }

    public void testMatchAllRangeChargesNothing() throws IOException {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
        runSearch(LongPoint.newRangeQuery(FIELD, Long.MIN_VALUE, Long.MAX_VALUE), breaker);
        assertThat("a match-all range allocates no result bitset", breaker.peak(), equalTo(0L));
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testExpensiveRangeTripsBreakerWithoutLeaking() throws IOException {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(100L);
        expectThrows(CircuitBreakingException.class, () -> runSearch(denseRangeQuery(), breaker));
        assertThat("a tripped reservation must not leak onto the breaker", breaker.getUsed(), equalTo(0L));
    }

    public void testPointRangeAccountingNotAllocatedWithoutPointRangeQuery() throws IOException {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
        ContextIndexSearcher searcher = newContextIndexSearcher(reader);
        searcher.setCircuitBreaker(breaker);
        int hits = searcher.search(new TermQuery(new Term(KEYWORD_FIELD, RARE_TERM)), new CountingCollectorManager());
        assertThat("the term query must match documents so the search actually runs", hits, greaterThan(0));
        assertFalse(
            "a search whose query tree contains no PointRangeQuery must not allocate accounting",
            searcher.hasLeafExecutionAccounting()
        );
    }

    public void testPointRangeAccountingAllocatedLazilyForPointRangeQuery() throws IOException {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
        ContextIndexSearcher searcher = newContextIndexSearcher(reader);
        searcher.setCircuitBreaker(breaker);
        assertFalse("accounting must not be allocated before any query has run", searcher.hasLeafExecutionAccounting());
        searcher.search(denseRangeQuery(), new CountingCollectorManager());
        assertTrue("wrapping a PointRangeQuery must lazily allocate accounting", searcher.hasLeafExecutionAccounting());
    }

    public void testPlainRangeInConjunctionChargesViaScorerGetPath() throws IOException {
        Query dv = SortedNumericDocValuesField.newSlowRangeQuery(FIELD, 0L, NUM_DOCS * 3L / 4L);
        assertChargesThenReleases(conjunction(denseRangeQuery(), dv));
    }

    public void testIndexOrDocValuesInConjunctionChargesWhenPointsBranchSelected() throws IOException {
        Query indexOrDocValues = new IndexOrDocValuesQuery(
            denseRangeQuery(),
            SortedNumericDocValuesField.newSlowRangeQuery(FIELD, 0L, NUM_DOCS * 3L / 4L)
        );
        Query lead = SortedNumericDocValuesField.newSlowRangeQuery(FIELD, 0L, NUM_DOCS * 3L / 4L);
        assertChargesThenReleases(conjunction(indexOrDocValues, lead));
    }

    public void testIndexOrDocValuesInConjunctionSkipsChargeWhenDocValuesBranchSelected() throws IOException {
        Query indexOrDocValues = new IndexOrDocValuesQuery(
            denseRangeQuery(),
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
        DenseSearch denseSearch = newDenseSearch(reader, breaker);
        for (LeafReaderContext leaf : reader.leaves()) {
            chargeAgainst(denseSearch.weight(), leaf);
        }
        assertThat("the out-of-band scorer must charge execution RAM", breaker.getUsed(), greaterThan(0L));
        denseSearch.searcher().close();
        assertThat("closing the searcher must release the residual out-of-band charge", breaker.getUsed(), equalTo(0L));
    }

    public void testOutOfBandChargeOnAnotherThreadReleasedOnClose() throws Exception {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
        DenseSearch denseSearch = newDenseSearch(reader, breaker);

        try (ExecutorService worker = Executors.newSingleThreadExecutor()) {
            worker.submit(() -> {
                for (LeafReaderContext leaf : reader.leaves()) {
                    // Force the points branch the way KNN materialises the filter bitset, outside any searchLeaf scope.
                    chargeAgainst(denseSearch.weight(), leaf);
                }
                return null;
            }).get();
        }

        assertThat("the worker thread's out-of-band scorer must charge execution RAM", breaker.getUsed(), greaterThan(0L));
        // close() runs on the test thread; the release must come from the shared per-leaf array, not from
        // any per-thread state.
        denseSearch.searcher().close();
        assertThat("closing on a different thread must still release the worker thread's charge", breaker.getUsed(), equalTo(0L));
    }

    public void testCloseDuringOpenScopeDoesNotDoubleRelease() throws Exception {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
        DenseSearch denseSearch = newDenseSearch(reader, breaker);
        ContextIndexSearcher searcher = denseSearch.searcher();
        Weight weight = denseSearch.weight();
        LeafReaderContext leaf = reader.leaves().get(0);

        CountDownLatch chargedAndCollecting = new CountDownLatch(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);
        Collector blockingOnFirstDoc = blockOnFirstDoc(chargedAndCollecting, releaseWorker);

        try (ExecutorService worker = Executors.newSingleThreadExecutor()) {
            Future<?> scoring = worker.submit(() -> {
                searcher.searchLeaf(leaf, 0, leaf.reader().maxDoc(), weight, blockingOnFirstDoc);
                return null;
            });
            safeAwait(chargedAndCollecting);
            assertThat("the worker must have charged before we close", breaker.getUsed(), greaterThan(0L));

            searcher.close();
            assertThat(
                "close() draining the still-open leaf's charge is the documented out-of-band release",
                breaker.getUsed(),
                equalTo(0L)
            );

            releaseWorker.countDown();
            scoring.get();
        }
        assertThat("the worker's own release must not double-release what close() already drained", breaker.getUsed(), equalTo(0L));
    }

    public void testNestedSearchLeafOnDifferentLeavesReleaseIndependently() throws Exception {
        withMultiSegmentReader(multiSegmentReader -> {
            TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
            DenseSearch denseSearch = newDenseSearch(multiSegmentReader, breaker);
            ContextIndexSearcher searcher = denseSearch.searcher();
            Weight weight = denseSearch.weight();

            LeafReaderContext outerLeaf = multiSegmentReader.leaves().get(0);
            LeafReaderContext innerLeaf = multiSegmentReader.leaves().get(1);
            CountingCollector innerCollector = new CountingCollector();

            Collector outerCollector = new Collector() {
                @Override
                public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
                    searcher.searchLeaf(innerLeaf, 0, innerLeaf.reader().maxDoc(), weight, innerCollector);
                    assertThat(
                        "the nested leaf's charge must be fully released before the outer leaf charges anything",
                        breaker.getUsed(),
                        equalTo(0L)
                    );
                    return new CountingCollector().getLeafCollector(context);
                }

                @Override
                public ScoreMode scoreMode() {
                    return ScoreMode.COMPLETE_NO_SCORES;
                }
            };

            searcher.searchLeaf(outerLeaf, 0, outerLeaf.reader().maxDoc(), weight, outerCollector);

            assertThat(
                "the inner leaf must have matched documents for the accounting to be exercised",
                innerCollector.count,
                greaterThan(0)
            );
            assertThat("both leaves must have charged execution RAM at some point", breaker.peak(), greaterThan(0L));
            assertThat("the outer leaf's own charge must be released once it is done", breaker.getUsed(), equalTo(0L));
        });
    }

    public void testConcurrentSlicesChargeAndReleaseIndependently() throws Exception {
        withMultiSegmentReader(multiSegmentReader -> {
            int leafCount = multiSegmentReader.leaves().size();
            TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
            try (ExecutorService executor = Executors.newFixedThreadPool(leafCount)) {
                ContextIndexSearcher searcher = new ContextIndexSearcher(
                    multiSegmentReader,
                    IndexSearcher.getDefaultSimilarity(),
                    null,
                    IndexSearcher.getDefaultQueryCachingPolicy(),
                    false,
                    executor,
                    20,
                    1
                );
                searcher.setCircuitBreaker(breaker);
                assertThat(
                    "the slicing parameters must actually put each leaf in its own slice, or this test "
                        + "isn't exercising concurrency across leaves",
                    searcher.getSlices().length,
                    equalTo(leafCount)
                );
                int hits = searcher.search(denseRangeQuery(), new CountingCollectorManager());
                assertThat("the range must match documents across multiple concurrently searched leaves", hits, greaterThan(0));
                assertThat("each leaf's scorer must charge execution RAM", breaker.peak(), greaterThan(0L));
                assertThat("all per-leaf charges must be released once the concurrent search completes", breaker.getUsed(), equalTo(0L));
            }
        });
    }

    public void testConcurrentPartitionsOfSameLeafDoNotDoubleRelease() throws Exception {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
        DenseSearch denseSearch = newDenseSearch(reader, breaker);
        ContextIndexSearcher searcher = denseSearch.searcher();
        Weight weight = denseSearch.weight();
        LeafReaderContext leaf = reader.leaves().get(0);
        int mid = leaf.reader().maxDoc() / 2;

        CountDownLatch firstPartitionEntered = new CountDownLatch(1);
        CountDownLatch releaseFirstPartition = new CountDownLatch(1);
        Collector blockingCollector = blockOnFirstDoc(firstPartitionEntered, releaseFirstPartition);

        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            Future<?> firstPartition = executor.submit(() -> {
                searcher.searchLeaf(leaf, 0, mid, weight, blockingCollector);
                return null;
            });
            safeAwait(firstPartitionEntered);
            assertThat("the first partition must have charged before it blocks in collection", breaker.getUsed(), greaterThan(0L));

            Future<?> secondPartition = executor.submit(() -> {
                searcher.searchLeaf(leaf, mid, leaf.reader().maxDoc(), weight, new CountingCollector());
                return null;
            });
            secondPartition.get();
            // Charges are per leaf, not per partition, so releasing partition 2 drains partition 1's charge
            // too. Only benign while computeSlices emits one whole-segment partition per leaf.
            assertThat("the second partition's release drains the first partition's still-live charge", breaker.getUsed(), equalTo(0L));

            releaseFirstPartition.countDown();
            firstPartition.get();
        }
        assertThat(
            "two partitions of the same leaf scored concurrently must each release only what they charged",
            breaker.getUsed(),
            equalTo(0L)
        );
    }

    public void testSetCircuitBreakerReleasesPreviousOutstandingCharge() throws Exception {
        TrackingCircuitBreaker firstBreaker = new TrackingCircuitBreaker(-1L);
        DenseSearch denseSearch = newDenseSearch(reader, firstBreaker);
        chargeAgainst(denseSearch.weight(), reader.leaves().get(0));
        assertThat("the out-of-band scorer must charge execution RAM", firstBreaker.getUsed(), greaterThan(0L));

        ContextIndexSearcher searcher = denseSearch.searcher();
        searcher.setCircuitBreaker(null);
        assertThat("swapping breakers must release what the old one was still holding", firstBreaker.getUsed(), equalTo(0L));

        searcher.close();
        assertThat("closing after the swap must not touch the old breaker again", firstBreaker.getUsed(), equalTo(0L));
    }

    public void testStaleWeightChargesCurrentBreakerAfterSwap() throws Exception {
        TrackingCircuitBreaker firstBreaker = new TrackingCircuitBreaker(-1L);
        DenseSearch denseSearch = newDenseSearch(reader, firstBreaker);

        TrackingCircuitBreaker secondBreaker = new TrackingCircuitBreaker(-1L);
        ContextIndexSearcher searcher = denseSearch.searcher();
        searcher.setCircuitBreaker(secondBreaker);

        chargeAgainst(denseSearch.weight(), reader.leaves().get(0));
        assertThat("a stale weight must not charge the breaker it was originally built against", firstBreaker.getUsed(), equalTo(0L));
        assertThat(
            "a stale weight must charge whichever breaker is current when it actually runs",
            secondBreaker.getUsed(),
            greaterThan(0L)
        );

        searcher.close();
        assertThat("closing must release the charge from the currently tracked breaker", secondBreaker.getUsed(), equalTo(0L));
    }

    public void testStaleWeightChargingAfterBreakerClearedDoesNotThrow() throws Exception {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
        DenseSearch denseSearch = newDenseSearch(reader, breaker);

        ContextIndexSearcher searcher = denseSearch.searcher();
        searcher.setCircuitBreaker(null);

        chargeAgainst(denseSearch.weight(), reader.leaves().get(0));
        assertThat("a stale weight scored with no breaker installed must not charge anything", breaker.getUsed(), equalTo(0L));
    }

    public void testKnnWithPointRangeFilterChargesOutOfBandThenReleasesToBaseline() throws Exception {
        try (Directory vectorDirectory = newDirectory()) {
            writeMultiSegmentIndex(vectorDirectory, true);
            try (DirectoryReader vectorReader = DirectoryReader.open(vectorDirectory)) {
                assertThat(
                    "the KNN fixture must produce more than one segment so the point-range filter runs on several leaves",
                    vectorReader.leaves().size(),
                    greaterThan(1)
                );

                TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
                ContextIndexSearcher searcher = newContextIndexSearcher(vectorReader);
                searcher.setCircuitBreaker(breaker);

                Query filter = denseRangeQuery();
                KnnFloatVectorQuery knn = new KnnFloatVectorQuery(VECTOR_FIELD, new float[] { 0f, 0f, 0f }, 10, filter);

                int hits = searcher.search(knn, new CountingCollectorManager());
                assertThat("the filtered KNN search must return matches so the accounting is actually exercised", hits, greaterThan(0));
                assertThat(
                    "the point-range filter bitset materialised out-of-band by KNN must charge execution RAM at some point",
                    breaker.peak(),
                    greaterThan(0L)
                );
                assertThat(
                    "the out-of-band KNN filter charge must return to baseline once the search completes",
                    breaker.getUsed(),
                    equalTo(0L)
                );

                searcher.close();
                assertThat("closing after a completed KNN search must leave nothing reserved", breaker.getUsed(), equalTo(0L));
            }
        }
    }

    private static float[] vectorForDoc(int docId) {
        float[] vector = new float[VECTOR_DIMS];
        for (int dim = 0; dim < VECTOR_DIMS; dim++) {
            vector[dim] = docId + dim;
        }
        return vector;
    }

    private static void chargeAgainst(Weight weight, LeafReaderContext ctx) throws IOException {
        ScorerSupplier scorerSupplier = weight.scorerSupplier(ctx);
        if (scorerSupplier != null) {
            scorerSupplier.get(Long.MAX_VALUE);
        }
    }

    private static Collector blockOnFirstDoc(CountDownLatch entered, CountDownLatch release) {
        return new Collector() {
            @Override
            public LeafCollector getLeafCollector(LeafReaderContext context) {
                return new LeafCollector() {
                    private boolean signalled = false;

                    @Override
                    public void setScorer(Scorable scorer) {}

                    @Override
                    public void collect(int doc) {
                        if (signalled == false) {
                            signalled = true;
                            entered.countDown();
                            safeAwait(release);
                        }
                    }
                };
            }

            @Override
            public ScoreMode scoreMode() {
                return ScoreMode.COMPLETE_NO_SCORES;
            }
        };
    }

    private static void withMultiSegmentReader(CheckedConsumer<DirectoryReader, Exception> body) throws Exception {
        try (Directory directory = newDirectory()) {
            writeMultiSegmentIndex(directory, false);
            try (DirectoryReader multiSegmentReader = DirectoryReader.open(directory)) {
                assertThat(
                    "the multi-segment fixture must produce more than one leaf to exercise cross-leaf behaviour",
                    multiSegmentReader.leaves().size(),
                    greaterThan(1)
                );
                body.accept(multiSegmentReader);
            }
        }
    }

    private static void writeMultiSegmentIndex(Directory directory, boolean withVectors) throws IOException {
        IndexWriterConfig config = new IndexWriterConfig(null).setMergePolicy(NoMergePolicy.INSTANCE);
        int segments = 4;
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            for (int segment = 0; segment < segments; segment++) {
                for (int docId = segment; docId < NUM_DOCS; docId += segments) {
                    Document doc = new Document();
                    doc.add(new LongPoint(FIELD, docId));
                    if (withVectors) {
                        doc.add(new KnnFloatVectorField(VECTOR_FIELD, vectorForDoc(docId), VectorSimilarityFunction.EUCLIDEAN));
                    }
                    writer.addDocument(doc);
                }
                writer.commit();
            }
        }
    }

    private static Query conjunction(Query first, Query second) {
        return new BooleanQuery.Builder().add(first, BooleanClause.Occur.MUST).add(second, BooleanClause.Occur.MUST).build();
    }

    private static Query denseRangeQuery() {
        return LongPoint.newRangeQuery(FIELD, 0L, (long) (NUM_DOCS * 3 / 4));
    }

    private void assertChargesThenReleases(Query query) throws IOException {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
        int hits = runSearch(query, breaker);
        assertThat("the range must match documents so its scorer actually runs", hits, greaterThan(0));
        assertThat("the point-range scorer must charge execution RAM while it runs", breaker.peak(), greaterThan(0L));
        assertThat("the per-leaf execution charge must be released once the leaf is scored", breaker.getUsed(), equalTo(0L));
    }

    private int runSearch(Query query, CircuitBreaker breaker) throws IOException {
        ContextIndexSearcher searcher = newContextIndexSearcher(reader);
        searcher.setCircuitBreaker(breaker);
        return searcher.search(query, new CountingCollectorManager());
    }

    private static ContextIndexSearcher newContextIndexSearcher(IndexReader reader) throws IOException {
        return new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            null,
            IndexSearcher.getDefaultQueryCachingPolicy(),
            false
        );
    }

    /** A searcher with a breaker installed and a {@link #denseRangeQuery} weight already built, ready to score. */
    private record DenseSearch(ContextIndexSearcher searcher, Weight weight) {}

    private static DenseSearch newDenseSearch(IndexReader reader, CircuitBreaker breaker) throws IOException {
        ContextIndexSearcher searcher = newContextIndexSearcher(reader);
        searcher.setCircuitBreaker(breaker);
        Weight weight = searcher.createWeight(searcher.rewrite(denseRangeQuery()), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        return new DenseSearch(searcher, weight);
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
