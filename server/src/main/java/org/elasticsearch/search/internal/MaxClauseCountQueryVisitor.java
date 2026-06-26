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
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.IndexSortSortedNumericDocValuesRangeQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.lucene.search.FuzzyQueries;
import org.elasticsearch.lucene.search.cost.PointRangeQueryCostEstimator;

import java.util.function.Supplier;

/**
 * {@link QueryVisitor} that counts visited clauses and throws {@link IndexSearcher.TooManyNestedClauses}
 * when the configured maximum is exceeded.
 * <p>
 * It also accumulates a memory estimate for every visited sub-query, exposed via
 * {@link #getEstimatedBytes()}, so {@link org.elasticsearch.index.query.AbstractQueryBuilder#toQuery}
 * can charge the request circuit breaker once per top-level call instead of once per leaf builder.
 * <p>
 * When a non-null {@link CircuitBreaker} is supplied, the visitor trips it mid-walk as soon as the
 * projected total (breaker baseline plus running estimate) exceeds the limit, so pathological
 * queries fail before their full Lucene tree is materialised.
 * <p>
 * {@link IndexOrDocValuesQuery} is counted as one clause and its inner queries are ignored;
 * {@link IndexSortSortedNumericDocValuesRangeQuery} is skipped so only its fallback query is counted.
 */
public final class MaxClauseCountQueryVisitor extends QueryVisitor {

    /**
     * Per-clause floor charged for visited queries that don't implement {@link Accountable}.
     */
    static final long LEAF_BASE_BYTES = 1024L;

    private int numClauses;
    private long estimatedBytes;
    private final int maxClauseCount;

    @Nullable
    private final CircuitBreaker breaker;
    private long breakerBaseline;

    public MaxClauseCountQueryVisitor(int maxClauseCount) {
        this(maxClauseCount, null);
    }

    public MaxClauseCountQueryVisitor(int maxClauseCount, @Nullable CircuitBreaker breaker) {
        this.maxClauseCount = maxClauseCount;
        this.breaker = breaker;
        this.breakerBaseline = breaker == null ? 0L : breaker.getUsed();
    }

    public int getMaxClauseCount() {
        return maxClauseCount;
    }

    public int getNumClauses() {
        return numClauses;
    }

    private void addEstimatedBytes(long bytes) {
        estimatedBytes += bytes;
        maybeTripBreaker();
    }

    /**
     * @return the accumulated per-clause memory estimate over the walk so far.
     */
    public long getEstimatedBytes() {
        return estimatedBytes;
    }

    /**
     * Clears the accumulated clause count and byte estimate, and recaptures the breaker baseline
     * from {@code breaker.getUsed()} when a breaker is configured.
     */
    public void reset() {
        numClauses = 0;
        estimatedBytes = 0L;
        if (breaker != null) {
            breakerBaseline = breaker.getUsed();
        }
    }

    public void merge(MaxClauseCountQueryVisitor other) {
        numClauses += other.numClauses;
        if (numClauses > maxClauseCount) {
            throw new IndexSearcher.TooManyNestedClauses();
        }
        addEstimatedBytes(other.estimatedBytes);
    }

    private void chargeBytesFor(Query query) {
        chargeBytesFor(query, 1);
    }

    /**
     * Charge the byte estimate for {@code query}, scaling the non-{@link Accountable} per-clause
     * floor by {@code termMultiplier}.
     */
    private void chargeBytesFor(Query query, int termMultiplier) {
        assert termMultiplier > 0 : "termMultiplier must be positive, got " + termMultiplier;
        long bytes;
        if (query instanceof FuzzyQuery fq) {
            bytes = FuzzyQueries.estimateBytes(fq);
        } else if (query instanceof PointRangeQuery prq) {
            bytes = new PointRangeQueryCostEstimator(prq.getNumDims(), prq.getBytesPerDim()).estimate();
        } else if (query instanceof Accountable a) {
            bytes = a.ramBytesUsed();
        } else {
            bytes = RamUsageEstimator.shallowSizeOf(query) + LEAF_BASE_BYTES * termMultiplier;
        }
        addEstimatedBytes(bytes);
    }

    private void maybeTripBreaker() {
        if (breaker == null) {
            return;
        }

        long limit = breaker.getLimit();
        if (limit < 0) {
            return;
        }

        long projected = breakerBaseline + estimatedBytes;
        if (projected > limit) {
            // Throw-only: circuitBreak bumps trippedCount and throws, but does NOT touch the breaker's
            // used counter. The accumulated estimate is committed in a single addCircuitBreakerMemory
            // call at the end of AbstractQueryBuilder#toQuery, which the throw unwinds before reaching.
            breaker.circuitBreak("query", projected);
        }
    }

    @Override
    public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
        if (parent instanceof IndexOrDocValuesQuery) {
            if (++numClauses > maxClauseCount) {
                throw new IndexSearcher.TooManyNestedClauses();
            }
            chargeBytesFor(parent);
            // ignore the subqueries inside IndexOrDocValuesQuery
            return QueryVisitor.EMPTY_VISITOR;
        }
        // Return this instance even for MUST_NOT and not an empty QueryVisitor
        return this;
    }

    @Override
    public void visitLeaf(Query query) {
        if (query instanceof IndexSortSortedNumericDocValuesRangeQuery) {
            // ignore so we only count the fallback query
            return;
        }
        if (++numClauses > maxClauseCount) {
            throw new IndexSearcher.TooManyNestedClauses();
        }
        chargeBytesFor(query);
    }

    @Override
    public void consumeTerms(Query query, Term... terms) {
        numClauses += terms.length;
        if (numClauses > maxClauseCount) {
            throw new IndexSearcher.TooManyNestedClauses();
        }
        chargeBytesFor(query, terms.length);
    }

    @Override
    public void consumeTermsMatching(Query query, String field, Supplier<ByteRunAutomaton> automaton) {
        if (++numClauses > maxClauseCount) {
            throw new IndexSearcher.TooManyNestedClauses();
        }
        chargeBytesFor(query);
    }
}
