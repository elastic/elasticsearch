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
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.IndexSortSortedNumericDocValuesRangeQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.lucene.search.FuzzyQueries;
import org.elasticsearch.lucene.search.cost.PointRangeQueryCostEstimator;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.elasticsearch.common.lucene.search.Queries.ALL_DOCS_INSTANCE;
import static org.hamcrest.Matchers.greaterThan;
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

    public void testChargesFuzzyQueryByFuzzyQueriesEstimateBytes() {
        MaxClauseCountQueryVisitor visitor = new MaxClauseCountQueryVisitor(IndexSearcher.getMaxClauseCount());
        FuzzyQuery fq = new FuzzyQuery(new Term("field", "value0"), 2, 1, 50, true);

        fq.visit(visitor);

        long expected = FuzzyQueries.estimateBytes(fq);
        assertEquals(expected, visitor.getEstimatedBytes());
        assertEquals(1, visitor.getNumClauses());
        assertThat(
            "fuzzy estimate must dominate the generic per-clause floor or this test loses its bite",
            expected,
            greaterThanOrEqualTo(MaxClauseCountQueryVisitor.LEAF_BASE_BYTES)
        );
    }

    public void testBooleanOfFuzzyClausesSumsPerClauseEstimates() {
        MaxClauseCountQueryVisitor visitor = new MaxClauseCountQueryVisitor(IndexSearcher.getMaxClauseCount());
        int clauses = randomIntBetween(2, 20);

        BooleanQuery.Builder bool = new BooleanQuery.Builder();
        long expected = 0L;
        for (int i = 0; i < clauses; i++) {
            FuzzyQuery fq = new FuzzyQuery(new Term("field", "value" + i), 2, 1, 50, true);
            bool.add(fq, BooleanClause.Occur.SHOULD);
            expected += FuzzyQueries.estimateBytes(fq);
        }
        bool.build().visit(visitor);

        assertEquals(
            "each fuzzy clause must be visited and summed by FuzzyQueries.estimateBytes, not floored once",
            expected,
            visitor.getEstimatedBytes()
        );
        assertEquals(clauses, visitor.getNumClauses());
    }

    public void testFuzzyQueryVisitDoesNotInvokeAutomatonSupplier() {
        MaxClauseCountQueryVisitor visitor = new MaxClauseCountQueryVisitor(IndexSearcher.getMaxClauseCount());
        AtomicInteger supplierInvocations = new AtomicInteger();
        FuzzyQuery fq = new FuzzyQuery(new Term("field", "value0"), 2, 1, 50, true) {
            @Override
            public void visit(QueryVisitor v) {
                if (v.acceptField(getField())) {
                    Supplier<ByteRunAutomaton> counting = () -> {
                        supplierInvocations.incrementAndGet();
                        return getAutomata().runAutomaton;
                    };
                    v.consumeTermsMatching(this, getField(), counting);
                }
            }
        };

        fq.visit(visitor);

        assertEquals(
            "MaxClauseCountQueryVisitor must not invoke the automaton supplier — that would force "
                + "FuzzyQuery#getAutomata() and defeat the once-per-phase \"charge before the expensive "
                + "automaton is built\" property",
            0,
            supplierInvocations.get()
        );
        assertEquals("the visit must still register the fuzzy clause for accounting", 1, visitor.getNumClauses());
        assertEquals(
            "the visit must still produce the parameter-driven byte estimate",
            FuzzyQueries.estimateBytes(fq),
            visitor.getEstimatedBytes()
        );
    }

    public void testFuzzyClauseChargeGrowsWithSegmentCount() {
        FuzzyQuery fq = new FuzzyQuery(new Term("field", "value0"), 2, 1, 50, true);

        MaxClauseCountQueryVisitor singleSegment = new MaxClauseCountQueryVisitor(IndexSearcher.getMaxClauseCount(), null, null, 1);
        MaxClauseCountQueryVisitor manySegments = new MaxClauseCountQueryVisitor(IndexSearcher.getMaxClauseCount(), null, null, 128);

        fq.visit(singleSegment);
        fq.visit(manySegments);

        assertEquals(FuzzyQueries.estimateBytes(fq, 1), singleSegment.getEstimatedBytes());
        assertEquals(FuzzyQueries.estimateBytes(fq, 128), manySegments.getEstimatedBytes());
        assertThat(
            "a fuzzy clause charged against a 128-segment reader must cost strictly more than against a 1-segment reader",
            manySegments.getEstimatedBytes(),
            greaterThan(singleSegment.getEstimatedBytes())
        );
    }

    public void testSegmentCountOrDefaultFallsBackToDefaultWhenReaderIsNull() {
        assertEquals(FuzzyQueries.DEFAULT_SEGMENT_COUNT_WHEN_UNKNOWN, MaxClauseCountQueryVisitor.segmentCountOrDefault(null));
    }

    public void testSegmentCountOrDefaultUsesRealReaderLeafCount() throws IOException {
        int segments = randomIntBetween(2, 5);
        try (Directory directory = new ByteBuffersDirectory()) {
            IndexWriterConfig writerConfig = new IndexWriterConfig(null).setMergePolicy(NoMergePolicy.INSTANCE);
            try (IndexWriter writer = new IndexWriter(directory, writerConfig)) {
                for (int i = 0; i < segments; i++) {
                    writer.addDocument(new Document());
                    writer.flush();
                }
                writer.commit();
            }
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                assertEquals(segments, MaxClauseCountQueryVisitor.segmentCountOrDefault(reader));
            }
        }
    }

    public void testChargesPointRangeQueryStructuralOnly() {
        PointRangeQuery prq = (PointRangeQuery) LongPoint.newRangeQuery("f", 1L, 100L);
        MaxClauseCountQueryVisitor visitor = new MaxClauseCountQueryVisitor(IndexSearcher.getMaxClauseCount());
        prq.visit(visitor);

        long structuralOnly = new PointRangeQueryCostEstimator(prq.getNumDims(), prq.getBytesPerDim()).estimate();
        assertEquals(structuralOnly, visitor.getEstimatedBytes());
        assertEquals(1, visitor.getNumClauses());
    }

    public void testPointRangeStructuralChargeIsIndependentOfReaderSize() {
        PointRangeQuery prq = (PointRangeQuery) IntPoint.newRangeQuery("f", 1, 100);

        MaxClauseCountQueryVisitor first = new MaxClauseCountQueryVisitor(IndexSearcher.getMaxClauseCount(), null);
        prq.visit(first);
        MaxClauseCountQueryVisitor second = new MaxClauseCountQueryVisitor(IndexSearcher.getMaxClauseCount());
        prq.visit(second);

        assertEquals(second.getEstimatedBytes(), first.getEstimatedBytes());
        assertEquals(new PointRangeQueryCostEstimator(prq.getNumDims(), prq.getBytesPerDim()).estimate(), first.getEstimatedBytes());
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

        assertEquals(perLeaf * leaves, visitor.getEstimatedBytes());
        assertEquals(leaves, visitor.getNumClauses());
    }

    public void testIndexOrDocValuesQueryIsChargedAsASingleClause() {
        MaxClauseCountQueryVisitor visitor = new MaxClauseCountQueryVisitor(IndexSearcher.getMaxClauseCount());
        IndexOrDocValuesQuery iodv = new IndexOrDocValuesQuery(ALL_DOCS_INSTANCE, ALL_DOCS_INSTANCE);

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

        long iodvOnly = RamUsageEstimator.shallowSizeOf(iodv) + MaxClauseCountQueryVisitor.LEAF_BASE_BYTES;
        assertEquals(iodvOnly, visitor.getEstimatedBytes());
        assertEquals(1, visitor.getNumClauses());
    }

    public void testIndexSortSortedNumericDocValuesRangeQueryIsSkipped() {
        MaxClauseCountQueryVisitor visitor = new MaxClauseCountQueryVisitor(IndexSearcher.getMaxClauseCount());
        Query skipped = new IndexSortSortedNumericDocValuesRangeQuery("field", 0L, 10L, ALL_DOCS_INSTANCE);

        visitor.visitLeaf(skipped);

        assertEquals(0, visitor.getNumClauses());
        assertEquals(0L, visitor.getEstimatedBytes());
    }

    public void testVisitLeafThrowsWhenClauseCountExceeded() {
        MaxClauseCountQueryVisitor visitor = new MaxClauseCountQueryVisitor(2);
        visitor.visitLeaf(ALL_DOCS_INSTANCE);
        visitor.visitLeaf(ALL_DOCS_INSTANCE);

        expectThrows(IndexSearcher.TooManyNestedClauses.class, () -> visitor.visitLeaf(ALL_DOCS_INSTANCE));
    }

    public void testConsumeTermsCountsEveryTermAndThrowsOnOverflow() {
        MaxClauseCountQueryVisitor visitor = new MaxClauseCountQueryVisitor(2);
        Term[] tooMany = new Term[] { new Term("f", "a"), new Term("f", "b"), new Term("f", "c") };

        Query parent = new TermQuery(tooMany[0]);
        expectThrows(IndexSearcher.TooManyNestedClauses.class, () -> visitor.consumeTerms(parent, tooMany));
    }

    public void testConsumeTermsChargesBytesProportionalToN() {
        MaxClauseCountQueryVisitor visitor = new MaxClauseCountQueryVisitor(IndexSearcher.getMaxClauseCount());
        int n = randomIntBetween(2, 32);
        Term[] terms = new Term[n];
        for (int i = 0; i < n; i++) {
            terms[i] = new Term("f", "v" + i);
        }
        Query parent = new TermQuery(terms[0]);

        visitor.consumeTerms(parent, terms);

        long expected = RamUsageEstimator.shallowSizeOf(parent) + MaxClauseCountQueryVisitor.LEAF_BASE_BYTES * n;
        assertEquals(expected, visitor.getEstimatedBytes());
        assertEquals(n, visitor.getNumClauses());
    }

    public void testConsumeTermsAccountableParentChargedOnce() {
        MaxClauseCountQueryVisitor visitor = new MaxClauseCountQueryVisitor(IndexSearcher.getMaxClauseCount());
        int n = randomIntBetween(2, 32);
        Term[] terms = new Term[n];
        for (int i = 0; i < n; i++) {
            terms[i] = new Term("f", "v" + i);
        }
        long parentRamBytes = randomLongBetween(1_000L, 1_000_000L);
        AccountableTestQuery parent = new AccountableTestQuery(parentRamBytes);

        visitor.consumeTerms(parent, terms);

        assertEquals(parentRamBytes, visitor.getEstimatedBytes());
        assertEquals(n, visitor.getNumClauses());
    }

    public void testConsumeTermsMatchingThrowsOnOverflow() {
        MaxClauseCountQueryVisitor visitor = new MaxClauseCountQueryVisitor(1);
        visitor.consumeTermsMatching(ALL_DOCS_INSTANCE, "f", () -> null);
        expectThrows(IndexSearcher.TooManyNestedClauses.class, () -> visitor.consumeTermsMatching(ALL_DOCS_INSTANCE, "f", () -> null));
    }

    public void testIndexOrDocValuesQueryThrowsOnOverflow() {
        MaxClauseCountQueryVisitor visitor = new MaxClauseCountQueryVisitor(0);
        IndexOrDocValuesQuery iodv = new IndexOrDocValuesQuery(ALL_DOCS_INSTANCE, ALL_DOCS_INSTANCE);
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

    public void testPostResetTripsReflectLiveBreakerUsage() {
        long limit = 1_000L;
        FakeCircuitBreaker breaker = new FakeCircuitBreaker(limit, 0L);
        MaxClauseCountQueryVisitor visitor = new MaxClauseCountQueryVisitor(IndexSearcher.getMaxClauseCount(), breaker);

        new AccountableTestQuery(600L).visit(visitor);
        assertFalse("precondition: initial visit must not trip", breaker.tripped);

        breaker.setUsed(900L);

        visitor.reset();

        expectThrows(CircuitBreakingException.class, () -> new AccountableTestQuery(200L).visit(visitor));
        assertTrue("post-reset trips must reflect the live breaker usage read at check time", breaker.tripped);
    }

    public void testResetWithoutBreakerIsANoOpForBaseline() {
        MaxClauseCountQueryVisitor visitor = new MaxClauseCountQueryVisitor(IndexSearcher.getMaxClauseCount(), null);
        new AccountableTestQuery(123L).visit(visitor);

        visitor.reset();

        assertEquals(0, visitor.getNumClauses());
        assertEquals(0L, visitor.getEstimatedBytes());

        new AccountableTestQuery(456L).visit(visitor);
        assertEquals(456L, visitor.getEstimatedBytes());
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
        outer.visitLeaf(ALL_DOCS_INSTANCE);

        MaxClauseCountQueryVisitor inner = new MaxClauseCountQueryVisitor(IndexSearcher.getMaxClauseCount());
        inner.visitLeaf(ALL_DOCS_INSTANCE);

        expectThrows(IndexSearcher.TooManyNestedClauses.class, () -> outer.merge(inner));
    }

    public void testNullBreakerNeverTrips() {
        MaxClauseCountQueryVisitor visitor = new MaxClauseCountQueryVisitor(IndexSearcher.getMaxClauseCount(), null);
        new AccountableTestQuery(Long.MAX_VALUE / 2).visit(visitor);
        assertEquals(Long.MAX_VALUE / 2, visitor.getEstimatedBytes());
    }

    public void testNoopCircuitBreakerNeverTrips() {
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

        new AccountableTestQuery(500L).visit(visitor);
        assertFalse("first leaf should fit inside the limit", breaker.tripped);

        expectThrows(CircuitBreakingException.class, () -> new AccountableTestQuery(600L).visit(visitor));
        assertTrue("second leaf should have tripped the breaker", breaker.tripped);
    }

    public void testPreExistingBreakerUsageIsIncludedInProjection() {
        long limit = 1_000L;
        long preExisting = 900L;
        FakeCircuitBreaker breaker = new FakeCircuitBreaker(limit, preExisting);
        MaxClauseCountQueryVisitor visitor = new MaxClauseCountQueryVisitor(IndexSearcher.getMaxClauseCount(), breaker);

        expectThrows(CircuitBreakingException.class, () -> new AccountableTestQuery(200L).visit(visitor));
        assertTrue(breaker.tripped);
    }

    public void testPreChargedBytesCountTowardMidWalkTripViaLiveUsed() {
        long limit = 1000L;
        FakeCircuitBreaker breaker = new FakeCircuitBreaker(limit, 0L);
        Query preChargedAutomaton = new AccountableTestQuery(10000L);
        MaxClauseCountQueryVisitor visitor = new MaxClauseCountQueryVisitor(
            IndexSearcher.getMaxClauseCount(),
            breaker,
            q -> q == preChargedAutomaton
        );

        breaker.setUsed(800L);
        preChargedAutomaton.visit(visitor);
        assertEquals("pre-charged bytes must never enter the committed estimate", 0L, visitor.getEstimatedBytes());
        assertEquals("the pre-charged leaf must still be counted as a clause", 1, visitor.getNumClauses());
        assertFalse("visiting the pre-charged leaf must not trip on its own", breaker.tripped);

        expectThrows(CircuitBreakingException.class, () -> new AccountableTestQuery(300L).visit(visitor));
        assertTrue("mid-walk projection must include pre-charged bytes via live breaker.getUsed()", breaker.tripped);
    }

    public void testMergeRoutesThroughEarlyTripPeek() {
        long limit = 1_000L;
        FakeCircuitBreaker breaker = new FakeCircuitBreaker(limit, 0L);
        MaxClauseCountQueryVisitor outer = new MaxClauseCountQueryVisitor(IndexSearcher.getMaxClauseCount(), breaker);

        MaxClauseCountQueryVisitor inner = new MaxClauseCountQueryVisitor(IndexSearcher.getMaxClauseCount());
        new AccountableTestQuery(2_000L).visit(inner);

        expectThrows(CircuitBreakingException.class, () -> outer.merge(inner));
        assertTrue("merge must trip the breaker once projected total exceeds the limit", breaker.tripped);
    }

    public void testMidWalkTripDoesNotMutateBreakerAccounting() {
        long limit = 1_000L;
        long preExisting = 100L;
        FakeCircuitBreaker breaker = new FakeCircuitBreaker(limit, preExisting);
        MaxClauseCountQueryVisitor visitor = new MaxClauseCountQueryVisitor(IndexSearcher.getMaxClauseCount(), breaker);

        expectThrows(CircuitBreakingException.class, () -> new AccountableTestQuery(2_000L).visit(visitor));

        assertTrue("mid-walk projection over the limit must trip the breaker", breaker.tripped);
        assertEquals(
            "circuitBreak must be throw-only: the running estimate is committed once at the end of toQuery, "
                + "so an unwinding throw must leave the breaker's used counter unchanged",
            preExisting,
            breaker.getUsed()
        );
        assertEquals("the visitor must never charge bytes via addEstimateBytesAndMaybeBreak", 0, breaker.addEstimateCalls);
        assertEquals("the visitor must never refund bytes via addWithoutBreaking", 0, breaker.addWithoutBreakingCalls);
    }

    /**
     * Minimal breaker that reports a configurable limit and a configurable baseline. {@link
     * #circuitBreak(String, long)} throws so the visitor's early-trip path is observable; all
     * other operations are inherited as no-ops from {@link NoopCircuitBreaker}.
     */
    private static final class FakeCircuitBreaker extends NoopCircuitBreaker {
        private final long limit;
        private long used;
        boolean tripped;
        int addEstimateCalls;
        int addWithoutBreakingCalls;

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

        void setUsed(long used) {
            this.used = used;
        }

        @Override
        public void circuitBreak(String fieldName, long bytesNeeded) {
            tripped = true;
            throw new CircuitBreakingException("Data too large, " + fieldName + " needed=" + bytesNeeded, Durability.PERMANENT);
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) {
            addEstimateCalls++;
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            addWithoutBreakingCalls++;
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
