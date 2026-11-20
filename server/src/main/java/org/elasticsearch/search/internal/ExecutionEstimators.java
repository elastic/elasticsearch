/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.apache.lucene.document.RangeFieldQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.PointInSetQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.lucene.search.CostlyMultiTermQueries;
import org.elasticsearch.lucene.search.IndexedSpatialQueries;

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
        final Query unboosted = unwrapBoost(query);
        if (unboosted instanceof IndexOrDocValuesQuery iodvq) {
            // IndexOrDocValuesQuery creates its index query's weight directly, so that query is never looked up on its own.
            final ExecutionCharge indexCharge = forAllocatingQuery(iodvq.getIndexQuery());
            return indexCharge == null ? null : new ExecutionCharge(indexSideOnly(indexCharge.estimator()), indexCharge.label());
        }
        return forAllocatingQuery(unboosted);
    }

    @Nullable
    private static ExecutionCharge forAllocatingQuery(Query query) {
        if (query instanceof PointRangeQuery pointRangeQuery) {
            return new ExecutionCharge(new PointRangeExecutionEstimator(pointRangeQuery), PointRangeExecutionEstimator.BREAKER_LABEL);
        }
        if (CostlyMultiTermQueries.isCostlyMultiTermQuery(query)) {
            return new ExecutionCharge(new MultiTermExecutionEstimator(query), MultiTermExecutionEstimator.BREAKER_LABEL);
        }
        if (query instanceof PointInSetQuery pointInSetQuery) {
            return points(pointInSetQuery.getField());
        }
        if (query instanceof RangeFieldQuery) {
            return points(fieldOf(query));
        }
        final IndexedSpatialQueries.Kind spatialKind = IndexedSpatialQueries.kindOf(query);
        if (spatialKind == null) {
            return null;
        }
        return switch (spatialKind) {
            case POINT_IN_GEOMETRY -> points(fieldOf(query));
            case POINT_DISTANCE -> points(new LatLonPointDistanceExecutionEstimator(fieldOf(query)));
            case SPATIAL -> points(new SpatialExecutionEstimator(fieldOf(query)));
        };
    }

    private static ExecutionCharge points(String field) {
        return points(new PointsExecutionEstimator(field));
    }

    private static ExecutionCharge points(LeafExecutionEstimator estimator) {
        return new ExecutionCharge(estimator, PointsExecutionEstimator.BREAKER_LABEL);
    }

    /** The field of a query that has no getter for it, as reported by its {@link Query#visit}. */
    static String fieldOf(Query query) {
        final String[] field = new String[1];
        query.visit(new QueryVisitor() {
            @Override
            public boolean acceptField(String visitedField) {
                field[0] = visitedField;
                return false;
            }
        });
        return field[0];
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
