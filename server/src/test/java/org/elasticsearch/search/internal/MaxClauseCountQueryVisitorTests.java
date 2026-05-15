/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.IndexSortSortedNumericDocValuesRangeQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class MaxClauseCountQueryVisitorTests extends ESTestCase {

    public void testChargesAccountableQueryByRamBytesUsed() {
        MaxClauseCountQueryVisitor visitor = new MaxClauseCountQueryVisitor(IndexSearcher.getMaxClauseCount());
        long expected = randomLongBetween(1L, 1_000_000L);

        new AccountableTestQuery(expected).visit(visitor);

        assertEquals(expected, visitor.getEstimatedBytes());
        assertEquals(1, visitor.getNumClauses());
    }

    public void testChargesNonAccountableQueryByShallowSizePlusFloor() {
        MaxClauseCountQueryVisitor visitor = new MaxClauseCountQueryVisitor(IndexSearcher.getMaxClauseCount());
        Query termQuery = new TermQuery(new Term("field", "value"));

        termQuery.visit(visitor);

        long expected = RamUsageEstimator.shallowSizeOf(termQuery) + MaxClauseCountQueryVisitor.LEAF_BASE_BYTES;
        assertEquals(expected, visitor.getEstimatedBytes());
        assertEquals(1, visitor.getNumClauses());
    }

    public void testNonAccountableEstimateIsAtLeastTheFloor() {
        MaxClauseCountQueryVisitor visitor = new MaxClauseCountQueryVisitor(IndexSearcher.getMaxClauseCount());

        new TermQuery(new Term("field", "value")).visit(visitor);

        assertThat(visitor.getEstimatedBytes(), greaterThanOrEqualTo(MaxClauseCountQueryVisitor.LEAF_BASE_BYTES));
    }

    public void testAccumulatesBytesAcrossAllLeavesInABooleanQuery() {
        MaxClauseCountQueryVisitor visitor = new MaxClauseCountQueryVisitor(IndexSearcher.getMaxClauseCount());
        long perLeaf = randomLongBetween(1_000L, 10_000L);
        int leaves = randomIntBetween(2, 50);

        BooleanQuery.Builder bool = new BooleanQuery.Builder();
        for (int i = 0; i < leaves; i++) {
            bool.add(new AccountableTestQuery(perLeaf), BooleanClause.Occur.SHOULD);
        }
        bool.build().visit(visitor);

        // Each accountable leaf contributes exactly its ramBytesUsed() — the inner queries built
        // by a compound Lucene query (mirroring what Lucene's QueryParser produces inside a
        // single LeafQueryBuilder) must all be counted, not just the outer container.
        assertEquals(perLeaf * leaves, visitor.getEstimatedBytes());
        assertEquals(leaves, visitor.getNumClauses());
    }

    public void testIndexOrDocValuesQueryIsChargedAsASingleClause() {
        MaxClauseCountQueryVisitor visitor = new MaxClauseCountQueryVisitor(IndexSearcher.getMaxClauseCount());
        IndexOrDocValuesQuery iodv = new IndexOrDocValuesQuery(new MatchAllDocsQuery(), new MatchAllDocsQuery());

        iodv.visit(visitor);

        assertEquals(1, visitor.getNumClauses());
        long expected = RamUsageEstimator.shallowSizeOf(iodv) + MaxClauseCountQueryVisitor.LEAF_BASE_BYTES;
        assertEquals(expected, visitor.getEstimatedBytes());
    }

    public void testIndexOrDocValuesInnerQueriesAreNotChargedSeparately() {
        MaxClauseCountQueryVisitor visitor = new MaxClauseCountQueryVisitor(IndexSearcher.getMaxClauseCount());
        long inner = randomLongBetween(10_000L, 100_000L);
        IndexOrDocValuesQuery iodv = new IndexOrDocValuesQuery(new AccountableTestQuery(inner), new AccountableTestQuery(inner));

        iodv.visit(visitor);

        // The IODV branch in getSubVisitor returns EMPTY_VISITOR, so the inner Accountable
        // ramBytesUsed() must never be added to the running total — only the IODV wrapper is.
        long iodvOnly = RamUsageEstimator.shallowSizeOf(iodv) + MaxClauseCountQueryVisitor.LEAF_BASE_BYTES;
        assertEquals(iodvOnly, visitor.getEstimatedBytes());
        assertEquals(1, visitor.getNumClauses());
    }

    public void testIndexSortSortedNumericDocValuesRangeQueryIsSkipped() {
        MaxClauseCountQueryVisitor visitor = new MaxClauseCountQueryVisitor(IndexSearcher.getMaxClauseCount());
        Query skipped = new IndexSortSortedNumericDocValuesRangeQuery("field", 0L, 10L, new MatchAllDocsQuery());

        visitor.visitLeaf(skipped);

        // The Lucene primary IndexSortSortedNumeric query is just an optimisation around its
        // fallback; the fallback gets its own walk, so the wrapper must never count itself.
        assertEquals(0, visitor.getNumClauses());
        assertEquals(0L, visitor.getEstimatedBytes());
    }

    public void testVisitLeafThrowsWhenClauseCountExceeded() {
        MaxClauseCountQueryVisitor visitor = new MaxClauseCountQueryVisitor(2);
        visitor.visitLeaf(new MatchAllDocsQuery());
        visitor.visitLeaf(new MatchAllDocsQuery());

        expectThrows(IndexSearcher.TooManyNestedClauses.class, () -> visitor.visitLeaf(new MatchAllDocsQuery()));
    }

    public void testConsumeTermsCountsEveryTermAndThrowsOnOverflow() {
        MaxClauseCountQueryVisitor visitor = new MaxClauseCountQueryVisitor(2);
        Term[] tooMany = new Term[] { new Term("f", "a"), new Term("f", "b"), new Term("f", "c") };

        Query parent = new TermQuery(tooMany[0]);
        expectThrows(IndexSearcher.TooManyNestedClauses.class, () -> visitor.consumeTerms(parent, tooMany));
    }

    public void testConsumeTermsMatchingThrowsOnOverflow() {
        MaxClauseCountQueryVisitor visitor = new MaxClauseCountQueryVisitor(1);
        visitor.consumeTermsMatching(new MatchAllDocsQuery(), "f", () -> null);

        expectThrows(
            IndexSearcher.TooManyNestedClauses.class,
            () -> visitor.consumeTermsMatching(new MatchAllDocsQuery(), "f", () -> null)
        );
    }

    public void testIndexOrDocValuesQueryThrowsOnOverflow() {
        MaxClauseCountQueryVisitor visitor = new MaxClauseCountQueryVisitor(0);
        IndexOrDocValuesQuery iodv = new IndexOrDocValuesQuery(new MatchAllDocsQuery(), new MatchAllDocsQuery());

        expectThrows(IndexSearcher.TooManyNestedClauses.class, () -> iodv.visit(visitor));
    }

    public void testResetClearsBothClauseCountAndBytes() {
        MaxClauseCountQueryVisitor visitor = new MaxClauseCountQueryVisitor(IndexSearcher.getMaxClauseCount());
        new AccountableTestQuery(randomLongBetween(1L, 10_000L)).visit(visitor);
        assertTrue("precondition: visitor accumulated state", visitor.getNumClauses() > 0 && visitor.getEstimatedBytes() > 0);

        visitor.reset();

        assertEquals(0, visitor.getNumClauses());
        assertEquals(0L, visitor.getEstimatedBytes());
    }

    public void testMergeAccumulatesClausesAndBytes() {
        MaxClauseCountQueryVisitor outer = new MaxClauseCountQueryVisitor(IndexSearcher.getMaxClauseCount());
        MaxClauseCountQueryVisitor inner = new MaxClauseCountQueryVisitor(IndexSearcher.getMaxClauseCount());
        long innerBytes = randomLongBetween(100L, 10_000L);
        new AccountableTestQuery(innerBytes).visit(inner);

        outer.merge(inner);

        assertEquals(inner.getNumClauses(), outer.getNumClauses());
        assertEquals(inner.getEstimatedBytes(), outer.getEstimatedBytes());
    }

    public void testMergeThrowsOnClauseOverflow() {
        MaxClauseCountQueryVisitor outer = new MaxClauseCountQueryVisitor(1);
        outer.visitLeaf(new MatchAllDocsQuery());

        MaxClauseCountQueryVisitor inner = new MaxClauseCountQueryVisitor(IndexSearcher.getMaxClauseCount());
        inner.visitLeaf(new MatchAllDocsQuery());

        expectThrows(IndexSearcher.TooManyNestedClauses.class, () -> outer.merge(inner));
    }

    public void testNullBreakerNeverTrips() {
        // The two-arg ctor with a null breaker disables the early-trip peek entirely.
        MaxClauseCountQueryVisitor visitor = new MaxClauseCountQueryVisitor(IndexSearcher.getMaxClauseCount(), null);
        new AccountableTestQuery(Long.MAX_VALUE / 2).visit(visitor);
        // No exception, and the running total is still tracked.
        assertEquals(Long.MAX_VALUE / 2, visitor.getEstimatedBytes());
    }

    public void testNoopCircuitBreakerNeverTrips() {
        // Negative sentinel limit must short-circuit maybeTripBreaker.
        MaxClauseCountQueryVisitor visitor = new MaxClauseCountQueryVisitor(
            IndexSearcher.getMaxClauseCount(),
            new NoopCircuitBreaker(CircuitBreaker.REQUEST)
        );
        new AccountableTestQuery(Long.MAX_VALUE / 2).visit(visitor);
        assertEquals(Long.MAX_VALUE / 2, visitor.getEstimatedBytes());
    }

    public void testBreakerTripsWhenProjectedExceedsLimit() {
        long limit = 1_000L;
        FakeCircuitBreaker breaker = new FakeCircuitBreaker(limit, 0L);
        MaxClauseCountQueryVisitor visitor = new MaxClauseCountQueryVisitor(IndexSearcher.getMaxClauseCount(), breaker);

        // First leaf well under the limit — no trip.
        new AccountableTestQuery(500L).visit(visitor);
        assertFalse("first leaf should fit inside the limit", breaker.tripped);

        // Second leaf pushes the running total above the limit — must trip.
        expectThrows(CircuitBreakingException.class, () -> new AccountableTestQuery(600L).visit(visitor));
        assertTrue("second leaf should have tripped the breaker", breaker.tripped);
    }

    public void testBreakerBaselineIsCapturedAtConstructionTime() {
        long limit = 1_000L;
        long preExisting = 900L;
        FakeCircuitBreaker breaker = new FakeCircuitBreaker(limit, preExisting);
        MaxClauseCountQueryVisitor visitor = new MaxClauseCountQueryVisitor(IndexSearcher.getMaxClauseCount(), breaker);

        // Only 200 bytes of new memory, but combined with the captured 900-byte baseline that
        // already exists on the breaker the projected total (1100) exceeds the 1000-byte limit.
        expectThrows(CircuitBreakingException.class, () -> new AccountableTestQuery(200L).visit(visitor));
        assertTrue(breaker.tripped);
    }

    public void testMergeRoutesThroughEarlyTripPeek() {
        long limit = 1_000L;
        FakeCircuitBreaker breaker = new FakeCircuitBreaker(limit, 0L);
        MaxClauseCountQueryVisitor outer = new MaxClauseCountQueryVisitor(IndexSearcher.getMaxClauseCount(), breaker);

        MaxClauseCountQueryVisitor inner = new MaxClauseCountQueryVisitor(IndexSearcher.getMaxClauseCount());
        new AccountableTestQuery(2_000L).visit(inner);

        // Even though the outer visitor never saw a leaf, merging from an inner visitor must
        // route bytes through addEstimatedBytes() and trigger the breaker peek.
        expectThrows(CircuitBreakingException.class, () -> outer.merge(inner));
        assertTrue("merge must trip the breaker once projected total exceeds the limit", breaker.tripped);
    }

    /**
     * Minimal breaker that reports a configurable limit and a configurable baseline. {@link
     * #circuitBreak(String, long)} throws so the visitor's early-trip path is observable; all
     * other operations are inherited as no-ops from {@link NoopCircuitBreaker}.
     */
    private static final class FakeCircuitBreaker extends NoopCircuitBreaker {
        private final long limit;
        private final long used;
        boolean tripped;

        FakeCircuitBreaker(long limit, long used) {
            super(CircuitBreaker.REQUEST);
            this.limit = limit;
            this.used = used;
        }

        @Override
        public long getLimit() {
            return limit;
        }

        @Override
        public long getUsed() {
            return used;
        }

        @Override
        public void circuitBreak(String fieldName, long bytesNeeded) {
            tripped = true;
            throw new CircuitBreakingException("Data too large, " + fieldName + " needed=" + bytesNeeded, Durability.PERMANENT);
        }
    }

    /**
     * Lucene query that reports a configurable {@link #ramBytesUsed()} so the visitor's
     * Accountable-aware accounting can be exercised deterministically.
     */
    private static final class AccountableTestQuery extends Query implements Accountable {
        private final long ramBytes;

        AccountableTestQuery(long ramBytes) {
            this.ramBytes = ramBytes;
        }

        @Override
        public long ramBytesUsed() {
            return ramBytes;
        }

        @Override
        public String toString(String field) {
            return "AccountableTestQuery[" + ramBytes + "]";
        }

        @Override
        public boolean equals(Object obj) {
            return obj == this;
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(this);
        }

        @Override
        public void visit(QueryVisitor visitor) {
            visitor.visitLeaf(this);
        }
    }
}
