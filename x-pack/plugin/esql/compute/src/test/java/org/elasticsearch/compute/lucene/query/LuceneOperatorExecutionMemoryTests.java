/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.index.cache.query.TrivialQueryCachingPolicy;
import org.elasticsearch.search.internal.ContextIndexSearcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

/**
 * The weights {@link ContextIndexSearcher} creates charge the request circuit breaker for the execution memory of some Lucene
 * scorers. These tests check that {@link LuceneOperator} and {@link LuceneQueryEvaluator}, which drive their scorers themselves,
 * release that memory when they drop a scorer.
 */
public class LuceneOperatorExecutionMemoryTests extends ComputeTestCase {

    private static final String FIELD = "f";
    private static final int SEGMENTS = 4;
    private static final int NUM_DOCS = 2000;

    public void testShardPartitioning() throws IOException {
        assertChargesAreReleasedPerLeaf(DataPartitioning.SHARD);
    }

    public void testSegmentPartitioning() throws IOException {
        assertChargesAreReleasedPerLeaf(DataPartitioning.SEGMENT);
    }

    public void testDocPartitioning() throws IOException {
        assertChargesAreReleasedPerLeaf(DataPartitioning.DOC);
    }

    /**
     * Two operators scoring the same leaf for different queries each hold their own charge: one finishing must not release
     * what the other still holds.
     */
    public void testOperatorsOnTheSameLeafReleaseIndependently() throws IOException {
        Query lower = LongPoint.newRangeQuery(FIELD, 0L, (long) (NUM_DOCS * 3 / 4));
        Query upper = LongPoint.newRangeQuery(FIELD, (long) (NUM_DOCS / 4), (long) NUM_DOCS);
        try (Directory directory = newDirectory()) {
            writeIndex(directory);
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                TrackingCircuitBreaker breaker = new TrackingCircuitBreaker();
                ContextIndexSearcher searcher = newSearcher(reader);
                searcher.setCircuitBreaker(breaker);
                LuceneSourceOperator.Factory factory = factory(searcher, List.of(lower, upper), DataPartitioning.SHARD, 2);

                LuceneSourceOperator first = (LuceneSourceOperator) factory.get(driverContext());
                LuceneSourceOperator second = (LuceneSourceOperator) factory.get(driverContext());
                try {
                    releasePage(first.getOutput());
                    long firstCharge = breaker.used.get();
                    assertThat("the first operator must hold the charge of the leaf it is scoring", firstCharge, greaterThan(0L));
                    releasePage(second.getOutput());
                    long secondCharge = breaker.used.get() - firstCharge;
                    assertThat("the second operator must hold a charge of its own", secondCharge, greaterThan(0L));

                    drain(first);
                    assertThat(
                        "the second operator's charge must survive the first one finishing",
                        breaker.used.get(),
                        equalTo(secondCharge)
                    );
                    drain(second);
                    assertThat(breaker.used.get(), equalTo(0L));
                } finally {
                    first.close();
                    second.close();
                }
                searcher.close();
                assertThat(breaker.used.get(), equalTo(0L));
            }
        }
    }

    public void testOperatorClosedBeforeFinishingReleasesItsCharge() throws IOException {
        Query query = LongPoint.newRangeQuery(FIELD, 0L, (long) (NUM_DOCS * 3 / 4));
        try (Directory directory = newDirectory()) {
            writeIndex(directory);
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                TrackingCircuitBreaker breaker = new TrackingCircuitBreaker();
                ContextIndexSearcher searcher = newSearcher(reader);
                searcher.setCircuitBreaker(breaker);
                LuceneSourceOperator.Factory factory = factory(searcher, List.of(query), DataPartitioning.SHARD, 1);

                try (LuceneSourceOperator operator = (LuceneSourceOperator) factory.get(driverContext())) {
                    releasePage(operator.getOutput());
                    assertThat(operator.isFinished(), equalTo(false));
                    assertThat(breaker.used.get(), greaterThan(0L));
                }
                assertThat(breaker.used.get(), equalTo(0L));
                searcher.close();
            }
        }
    }

    /**
     * A driver that resumes on another thread rebuilds its scorer. The rebuilt scorer replaces the previous one, so the operator
     * must still hold a single charge.
     */
    public void testRebuildOnAnotherThreadHoldsOneCharge() throws Exception {
        Query query = LongPoint.newRangeQuery(FIELD, 0L, (long) (NUM_DOCS * 3 / 4));
        try (Directory directory = newDirectory()) {
            writeIndex(directory);
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                TrackingCircuitBreaker breaker = new TrackingCircuitBreaker();
                ContextIndexSearcher searcher = newSearcher(reader);
                searcher.setCircuitBreaker(breaker);
                LuceneSourceOperator.Factory factory = factory(searcher, List.of(query), DataPartitioning.SHARD, 1);

                try (LuceneSourceOperator operator = (LuceneSourceOperator) factory.get(driverContext())) {
                    releasePage(operator.getOutput());
                    long singleCharge = breaker.used.get();
                    assertThat(singleCharge, greaterThan(0L));

                    Thread otherThread = new Thread(() -> releasePage(operator.getOutput()));
                    otherThread.start();
                    otherThread.join();
                    assertThat("the scorer must have been rebuilt", breaker.charges.get(), equalTo(2L));
                    assertThat(breaker.used.get(), equalTo(singleCharge));
                }
                assertThat(breaker.used.get(), equalTo(0L));
                searcher.close();
            }
        }
    }

    /**
     * {@link LuceneQueryEvaluator} keeps the scorers of every segment it has evaluated. It must hold one charge for each of them,
     * however often it rebuilds them, and release them all when it is closed.
     */
    public void testQueryEvaluatorHoldsOneChargePerSegment() throws Exception {
        Query query = LongPoint.newRangeQuery(FIELD, 0L, (long) (NUM_DOCS * 3 / 4));
        try (Directory directory = newDirectory()) {
            writeIndex(directory);
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                long chargeForAllLeaves = chargeForAllLeaves(reader, query);
                TrackingCircuitBreaker breaker = new TrackingCircuitBreaker();
                ContextIndexSearcher searcher = newSearcher(reader);
                searcher.setCircuitBreaker(breaker);

                List<Page> pages = new ArrayList<>();
                LuceneSourceOperator.Factory source = factory(searcher, List.of(new MatchAllDocsQuery()), DataPartitioning.SHARD, 1);
                try (LuceneSourceOperator operator = (LuceneSourceOperator) source.get(driverContext())) {
                    while (operator.isFinished() == false) {
                        Page page = operator.getOutput();
                        if (page != null) {
                            pages.add(page);
                        }
                    }
                }
                assertThat("match all must not charge execution memory", breaker.charges.get(), equalTo(0L));

                LuceneQueryExpressionEvaluator evaluator = new LuceneQueryExpressionEvaluator(
                    blockFactory(),
                    new IndexedByShardIdFromSingleton<>(new LuceneQueryEvaluator.ShardConfig(searcher.rewrite(query), searcher))
                );
                try {
                    evaluate(evaluator, pages);
                    assertThat("every segment's scorer is live", breaker.used.get(), equalTo(chargeForAllLeaves));
                    long builds = breaker.charges.get();

                    Thread otherThread = new Thread(() -> evaluate(evaluator, pages));
                    otherThread.start();
                    otherThread.join();
                    assertThat("the scorers must have been rebuilt", breaker.charges.get(), greaterThan(builds));
                    assertThat(breaker.used.get(), equalTo(chargeForAllLeaves));

                    // Positions with gaps are scored with a Scorer, which the evaluator keeps next to the segment's BulkScorer.
                    Page sparse = pages.get(0).filter(false, 0, 2, 4);
                    try {
                        evaluator.eval(sparse).close();
                        long withScorer = breaker.used.get();
                        assertThat(withScorer, greaterThan(chargeForAllLeaves));
                        Thread sparseThread = new Thread(() -> evaluator.eval(sparse).close());
                        sparseThread.start();
                        sparseThread.join();
                        assertThat("a rebuilt Scorer must replace the charge of the previous one", breaker.used.get(), equalTo(withScorer));
                    } finally {
                        sparse.releaseBlocks();
                    }
                } finally {
                    evaluator.close();
                    pages.forEach(Page::releaseBlocks);
                }
                assertThat("a closed evaluator must not hold execution memory", breaker.used.get(), equalTo(0L));
                searcher.close();
            }
        }
    }

    private static void evaluate(LuceneQueryExpressionEvaluator evaluator, List<Page> pages) {
        for (Page page : pages) {
            evaluator.eval(page).close();
        }
    }

    private void assertChargesAreReleasedPerLeaf(DataPartitioning dataPartitioning) throws IOException {
        Query query = LongPoint.newRangeQuery(FIELD, 0L, (long) (NUM_DOCS * 3 / 4));
        try (Directory directory = newDirectory()) {
            writeIndex(directory);
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                assertThat(reader.leaves().size(), equalTo(SEGMENTS));
                long chargeForAllLeaves = chargeForAllLeaves(reader, query);

                TrackingCircuitBreaker breaker = new TrackingCircuitBreaker();
                ContextIndexSearcher searcher = newSearcher(reader);
                searcher.setCircuitBreaker(breaker);
                LuceneSourceOperator.Factory factory = factory(searcher, List.of(query), dataPartitioning, 1);

                int rows;
                try (LuceneSourceOperator operator = (LuceneSourceOperator) factory.get(driverContext())) {
                    rows = drain(operator);
                    assertThat("a finished operator must not hold execution memory", breaker.used.get(), equalTo(0L));
                }
                assertThat(rows, equalTo(NUM_DOCS * 3 / 4 + 1));
                assertThat("the scorers must charge execution memory while they run", breaker.peak.get(), greaterThan(0L));
                assertThat(
                    "the charge for a leaf must be released before the next leaf is scored",
                    breaker.peak.get(),
                    lessThan(chargeForAllLeaves)
                );
                searcher.close();
                assertThat(breaker.used.get(), equalTo(0L));
            }
        }
    }

    private static LuceneSourceOperator.Factory factory(
        ContextIndexSearcher searcher,
        List<Query> queries,
        DataPartitioning dataPartitioning,
        int taskConcurrency
    ) {
        List<LuceneSliceQueue.QueryAndTags> queryAndTags = queries.stream()
            .map(query -> new LuceneSliceQueue.QueryAndTags(query, List.of()))
            .toList();
        return new LuceneSourceOperator.Factory(
            new IndexedByShardIdFromSingleton<>(new LuceneSourceOperatorTests.MockShardContext(searcher, 0)),
            ctx -> queryAndTags,
            dataPartitioning,
            DataPartitioning.AutoStrategy.DEFAULT,
            LuceneOperator.SMALL_INDEX_BOUNDARY,
            taskConcurrency,
            100,
            LuceneOperator.NO_LIMIT,
            false,
            () -> 0L,
            LuceneSliceQueue.MIN_DOCS_PER_SLICE,
            QueryWarnings.EMIT
        );
    }

    private static int drain(LuceneSourceOperator operator) {
        int rows = 0;
        while (operator.isFinished() == false) {
            rows += releasePage(operator.getOutput());
        }
        return rows;
    }

    private static int releasePage(Page page) {
        if (page == null) {
            return 0;
        }
        int rows = page.getPositionCount();
        page.releaseBlocks();
        return rows;
    }

    /** What the breaker is charged when every leaf's scorer is built and none is released. */
    private static long chargeForAllLeaves(DirectoryReader reader, Query query) throws IOException {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker();
        ContextIndexSearcher searcher = newSearcher(reader);
        searcher.setCircuitBreaker(breaker);
        Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        for (LeafReaderContext leaf : reader.leaves()) {
            weight.bulkScorer(leaf);
        }
        long charged = breaker.used.get();
        searcher.close();
        assertThat(charged, greaterThan(0L));
        return charged;
    }

    private static ContextIndexSearcher newSearcher(DirectoryReader reader) throws IOException {
        return new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            TrivialQueryCachingPolicy.NEVER,
            false
        );
    }

    private static void writeIndex(Directory directory) throws IOException {
        IndexWriterConfig config = new IndexWriterConfig(null).setMergePolicy(NoMergePolicy.INSTANCE);
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            for (int segment = 0; segment < SEGMENTS; segment++) {
                for (int docId = segment; docId < NUM_DOCS; docId += SEGMENTS) {
                    Document doc = new Document();
                    doc.add(new LongPoint(FIELD, docId));
                    writer.addDocument(doc);
                }
                writer.commit();
            }
        }
    }

    private DriverContext driverContext() {
        return new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory(), null);
    }

    private static final class TrackingCircuitBreaker extends NoopCircuitBreaker {
        private final AtomicLong used = new AtomicLong();
        private final AtomicLong peak = new AtomicLong();
        private final AtomicLong charges = new AtomicLong();

        TrackingCircuitBreaker() {
            super("request");
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
            charges.incrementAndGet();
            peak.accumulateAndGet(used.addAndGet(bytes), Math::max);
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            used.addAndGet(bytes);
        }

        @Override
        public long getUsed() {
            return used.get();
        }
    }
}
