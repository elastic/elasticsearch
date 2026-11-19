/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.lucene.search.CostlyMultiTermQueries;

/**
 * Maps the queries whose scorers allocate per-leaf execution memory to the {@link ExecutionCharge} that
 * {@link ExecutionBreakerWeight} applies for them. {@link ContextIndexSearcher#createWeight} consults it for every query in
 * the tree, so a query is matched only at the node that allocates.
 */
final class ExecutionEstimators {

    /**
     * What {@link ExecutionBreakerWeight} charges for one query.
     *
     * @param estimator sizes the query's execution memory on each leaf
     * @param label     the request circuit breaker label the memory is charged under
     */
    record ExecutionCharge(LeafExecutionEstimator estimator, String label) {}

    private ExecutionEstimators() {}

    /**
     * @return the charge for {@code query}'s execution memory, or {@code null} if it allocates none worth charging
     */
    @Nullable
    static ExecutionCharge forQuery(Query query) {
        if (query instanceof PointRangeQuery pointRangeQuery) {
            return new ExecutionCharge(new PointRangeExecutionEstimator(pointRangeQuery), PointRangeExecutionEstimator.BREAKER_LABEL);
        }
        if (query instanceof IndexOrDocValuesQuery iodvq && iodvq.getIndexQuery() instanceof PointRangeQuery pointRangeQuery) {
            return new ExecutionCharge(
                indexSideOnly(new PointRangeExecutionEstimator(pointRangeQuery)),
                PointRangeExecutionEstimator.BREAKER_LABEL
            );
        }
        final Query unboosted = unwrapBoost(query);
        if (CostlyMultiTermQueries.isCostlyMultiTermQuery(unboosted)) {
            return new ExecutionCharge(new MultiTermExecutionEstimator(unboosted), MultiTermExecutionEstimator.BREAKER_LABEL);
        }
        if (unboosted instanceof IndexOrDocValuesQuery iodvq && CostlyMultiTermQueries.isCostlyMultiTermQuery(iodvq.getIndexQuery())) {
            return new ExecutionCharge(
                indexSideOnly(new MultiTermExecutionEstimator(iodvq.getIndexQuery())),
                MultiTermExecutionEstimator.BREAKER_LABEL
            );
        }
        return null;
    }

    /**
     * Sizes the index side of an {@link IndexOrDocValuesQuery}, which allocates only on the leaves where Lucene picks that
     * side: when its cost / 8 is at most the lead cost. On the other leaves doc values run and allocate nothing.
     */
    private static LeafExecutionEstimator indexSideOnly(LeafExecutionEstimator indexEstimator) {
        return (ctx, inner, leadCost) -> (inner.cost() >>> 3) <= leadCost ? indexEstimator.bytesForLeaf(ctx, inner, leadCost) : 0L;
    }

    /**
     * Unwraps {@link BoostQuery} only. {@link ConstantScoreQuery#createWeight} recurses through
     * {@link ContextIndexSearcher#createWeight}, so its child is already wrapped once; unwrapping it here too would double-charge.
     */
    private static Query unwrapBoost(Query query) {
        while (query instanceof BoostQuery boostQuery) {
            query = boostQuery.getQuery();
        }
        return query;
    }
}
