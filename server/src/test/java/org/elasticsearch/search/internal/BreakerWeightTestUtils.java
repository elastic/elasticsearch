/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.tests.util.RamUsageTester;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Helpers shared by the tests of the {@link ContextIndexSearcher} weights that charge per-leaf execution RAM
 * to the request breaker, and of the estimators that size those charges.
 */
final class BreakerWeightTestUtils {

    private BreakerWeightTestUtils() {}

    static ContextIndexSearcher newContextIndexSearcher(IndexReader reader) throws IOException {
        return new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            null,
            IndexSearcher.getDefaultQueryCachingPolicy(),
            false
        );
    }

    /** Runs {@code query} with {@code breaker} installed and returns the number of hits. */
    static int runSearch(IndexReader reader, Query query, CircuitBreaker breaker) throws IOException {
        ContextIndexSearcher searcher = newContextIndexSearcher(reader);
        searcher.setCircuitBreaker(breaker);
        return searcher.search(query, new CountingCollectorManager());
    }

    /** Builds a scorer for {@code ctx} outside of {@link ContextIndexSearcher#searchLeaf}, so its charge is not released per leaf. */
    static void chargeAgainst(Weight weight, LeafReaderContext ctx) throws IOException {
        ScorerSupplier scorerSupplier = weight.scorerSupplier(ctx);
        if (scorerSupplier != null) {
            scorerSupplier.get(Long.MAX_VALUE);
        }
    }

    static Query conjunction(Query first, Query second) {
        return new BooleanQuery.Builder().add(first, BooleanClause.Occur.MUST).add(second, BooleanClause.Occur.MUST).build();
    }

    /**
     * Checks that what {@link ExecutionCharge#forQuery} charges for {@code query} on each leaf of {@code searcher} is at least the
     * heap of the doc id set the leaf's scorer keeps.
     */
    static void assertChargeIsCeilingOnRetainedSet(IndexSearcher searcher, Query query) throws IOException {
        LeafExecutionEstimator estimator = ExecutionCharge.forQuery(query).estimator();
        Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);
        for (LeafReaderContext ctx : searcher.getIndexReader().leaves()) {
            ScorerSupplier supplier = weight.scorerSupplier(ctx);
            if (supplier == null) {
                continue;
            }
            long charged = estimator.bytesForLeaf(ctx, supplier, Long.MAX_VALUE);
            boolean matchesNothing = supplier.cost() == 0;
            long retained = RamUsageTester.ramUsed(supplier.get(Long.MAX_VALUE).iterator());
            // An intersection that matches nothing keeps an empty array, which is not charged.
            if (matchesNothing == false) {
                assertThat(
                    String.format(Locale.ROOT, "%s [maxDoc=%d, charged=%d, retained=%d]", query, ctx.reader().maxDoc(), charged, retained),
                    charged,
                    greaterThanOrEqualTo(retained)
                );
            }
        }
    }

    /** Request breaker that keeps track of what is in use, the peak, and every accepted reservation. A negative limit never trips. */
    static final class TrackingCircuitBreaker extends NoopCircuitBreaker {
        private final long limit;
        private final AtomicLong used = new AtomicLong();
        private final AtomicLong peak = new AtomicLong();
        private final List<Long> charges = new CopyOnWriteArrayList<>();

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
            charges.add(bytes);
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

        /** Every reservation that was accepted, in the order it was made. */
        List<Long> charges() {
            return charges;
        }
    }

    static final class CountingCollectorManager implements CollectorManager<CountingCollector, Integer> {
        @Override
        public CountingCollector newCollector() {
            return new CountingCollector();
        }

        @Override
        public Integer reduce(Collection<CountingCollector> collectors) {
            int total = 0;
            for (CountingCollector collector : collectors) {
                total += collector.count();
            }
            return total;
        }
    }

    static final class CountingCollector implements Collector {
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

        int count() {
            return count;
        }
    }
}
