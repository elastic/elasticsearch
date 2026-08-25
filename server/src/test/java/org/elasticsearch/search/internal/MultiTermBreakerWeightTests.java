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
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.lucene.search.cost.TermsQueryCostEstimator;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class MultiTermBreakerWeightTests extends ESTestCase {

    private static final String FIELD = "f";
    private static final int NUM_DOCS = 2000;
    private static final int NUM_TERMS = 500;

    private Directory directory;
    private DirectoryReader reader;

    @Before
    public void initDirectoryAndReader() throws Exception {
        directory = newDirectory();
        try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(null))) {
            for (int docId = 0; docId < NUM_DOCS; docId++) {
                Document doc = new Document();
                doc.add(new StringField(FIELD, term(docId % NUM_TERMS), Field.Store.NO));
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

    private static String term(int i) {
        return String.format(java.util.Locale.ROOT, "term-%04d", i);
    }

    private static Query termInSetQuery() {
        List<BytesRef> terms = new ArrayList<>();
        for (int i = 0; i < NUM_TERMS; i++) {
            terms.add(new BytesRef(term(i)));
        }
        return new TermInSetQuery(FIELD, terms);
    }

    private static Query termRangeQuery() {
        return new TermRangeQuery(FIELD, new BytesRef(term(0)), new BytesRef(term(NUM_TERMS - 1)), true, true);
    }

    public void testTermInSetQueryChargesAndReleasesAcrossSearch() throws IOException {
        assertChargesThenReleases(termInSetQuery());
    }

    public void testTermRangeQueryChargesAndReleasesAcrossSearch() throws IOException {
        assertChargesThenReleases(termRangeQuery());
    }

    public void testConstantScoreWrappedTermInSetChargedExactlyOnce() throws IOException {
        assertChargesThenReleases(new ConstantScoreQuery(termInSetQuery()));
    }

    public void testBoostQueryWrappedTermInSetChargedExactlyOnce() throws IOException {
        Query boosted = new BoostQuery(termInSetQuery(), 2.0f);
        long expectedTotal = expectedDrivenCharge(boosted, ScoreMode.COMPLETE);
        assertThat("test setup: the boosted query must have a positive execution charge", expectedTotal, greaterThan(0L));

        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
        ContextIndexSearcher searcher = newContextIndexSearcher(reader);
        searcher.setCircuitBreaker(breaker);
        Weight weight = searcher.createWeight(searcher.rewrite(boosted), ScoreMode.COMPLETE, 1.0f);
        for (LeafReaderContext leaf : reader.leaves()) {
            chargeAgainst(weight, leaf);
        }
        assertThat("a boosted multi-term query must be charged exactly once (no double-charge)", breaker.getUsed(), equalTo(expectedTotal));
        searcher.close();
        assertThat("closing the searcher must release the residual charge", breaker.getUsed(), equalTo(0L));
    }

    public void testExpensiveMultiTermQueryTripsBreakerWithoutLeaking() throws IOException {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(100L);
        expectThrows(CircuitBreakingException.class, () -> runSearch(termInSetQuery(), breaker));
        assertThat("a tripped reservation must not leak onto the breaker", breaker.getUsed(), equalTo(0L));
    }

    public void testAccountingNotAllocatedWithoutMultiTermQuery() throws IOException {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
        ContextIndexSearcher searcher = newContextIndexSearcher(reader);
        searcher.setCircuitBreaker(breaker);
        int hits = searcher.search(new TermQuery(new Term(FIELD, term(0))), new CountingCollectorManager());
        assertThat("the term query must match documents so the search actually runs", hits, greaterThan(0));
        assertFalse(
            "a search whose query tree contains no costly multi-term query must not allocate accounting",
            searcher.hasLeafExecutionAccounting()
        );
    }

    public void testAccountingAllocatedLazilyForMultiTermQuery() throws IOException {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
        ContextIndexSearcher searcher = newContextIndexSearcher(reader);
        searcher.setCircuitBreaker(breaker);
        assertFalse("accounting must not be allocated before any query has run", searcher.hasLeafExecutionAccounting());
        searcher.search(termInSetQuery(), new CountingCollectorManager());
        assertTrue("wrapping a costly multi-term query must lazily allocate accounting", searcher.hasLeafExecutionAccounting());
    }

    public void testNoCircuitBreakerConfiguredIsNoop() throws IOException {
        ContextIndexSearcher searcher = newContextIndexSearcher(reader);
        int hits = searcher.search(termInSetQuery(), new CountingCollectorManager());
        assertThat("the query must still match documents when no breaker is configured", hits, greaterThan(0));
        assertFalse("no breaker means no accounting is allocated", searcher.hasLeafExecutionAccounting());
    }

    public void testOutOfBandChargeReleasedOnClose() throws IOException {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
        ContextIndexSearcher searcher = newContextIndexSearcher(reader);
        searcher.setCircuitBreaker(breaker);
        Weight weight = searcher.createWeight(searcher.rewrite(termInSetQuery()), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        for (LeafReaderContext leaf : reader.leaves()) {
            chargeAgainst(weight, leaf);
        }
        assertThat("the out-of-band scorer must charge execution RAM", breaker.getUsed(), greaterThan(0L));
        searcher.close();
        assertThat("closing the searcher must release the residual out-of-band charge", breaker.getUsed(), equalTo(0L));
    }

    private static void chargeAgainst(Weight weight, LeafReaderContext ctx) throws IOException {
        ScorerSupplier scorerSupplier = weight.scorerSupplier(ctx);
        if (scorerSupplier != null) {
            scorerSupplier.get(Long.MAX_VALUE);
        }
    }

    private void assertChargesThenReleases(Query query) throws IOException {
        long expectedPeak = expectedSearchPerLeafCharges(query).stream().mapToLong(Long::longValue).max().orElse(0L);
        assertThat("test setup: the query must have a positive per-leaf execution charge", expectedPeak, greaterThan(0L));

        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
        int hits = runSearch(query, breaker);
        assertThat("the query must match documents so its scorer actually runs", hits, greaterThan(0));
        assertThat(
            "the multi-term scorer must charge exactly the per-leaf execution RAM once (no double-charge)",
            breaker.peak(),
            equalTo(expectedPeak)
        );
        assertThat("the per-leaf execution charge must be released once the leaf is scored", breaker.getUsed(), equalTo(0L));
    }

    private List<Long> expectedSearchPerLeafCharges(Query query) throws IOException {
        ContextIndexSearcher searcher = newContextIndexSearcher(reader);
        Weight weight = searcher.createWeight(searcher.rewrite(new ConstantScoreQuery(query)), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        List<Long> charges = new ArrayList<>();
        for (LeafReaderContext leaf : reader.leaves()) {
            ScorerSupplier scorerSupplier = weight.scorerSupplier(leaf);
            if (scorerSupplier != null) {
                charges.add(TermsQueryCostEstimator.executionBytesForLeaf(scorerSupplier.cost(), leaf.reader().maxDoc()));
            }
        }
        return charges;
    }

    private long expectedDrivenCharge(Query query, ScoreMode scoreMode) throws IOException {
        ContextIndexSearcher searcher = newContextIndexSearcher(reader);
        Weight weight = searcher.createWeight(searcher.rewrite(query), scoreMode, 1.0f);
        long total = 0L;
        for (LeafReaderContext leaf : reader.leaves()) {
            ScorerSupplier scorerSupplier = weight.scorerSupplier(leaf);
            if (scorerSupplier != null) {
                total += TermsQueryCostEstimator.executionBytesForLeaf(scorerSupplier.cost(), leaf.reader().maxDoc());
            }
        }
        return total;
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
