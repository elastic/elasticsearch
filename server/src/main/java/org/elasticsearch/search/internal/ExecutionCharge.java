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
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSortSortedNumericDocValuesRangeQuery;
import org.apache.lucene.search.PointInSetQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.lucene.search.CostlyMultiTermQueries;
import org.elasticsearch.lucene.search.IndexedSpatialQueries;

import java.io.IOException;

/**
 * What {@link ExecutionBreakerWeight} charges for one query whose scorer allocates per-leaf execution memory.
 * {@link #forQuery} maps each such query to its charge.
 * <p>
 * {@link ContextIndexSearcher#createWeight} consults {@link #forQuery} only for the queries that reach the searcher: the root, and
 * children whose parent builds their weight through {@code searcher.createWeight} (e.g. {@link BooleanQuery}
 * clauses and {@link ConstantScoreQuery}). Wrappers such as {@link BoostQuery}, {@link IndexSortSortedNumericDocValuesRangeQuery}
 * and {@link IndexOrDocValuesQuery} call {@code query.createWeight} on their children directly, so those children never come
 * back through the searcher and {@link #forQuery} has to look through the wrapper itself.
 *
 * @param estimator sizes the query's execution memory on each leaf
 * @param label     the request circuit breaker label the memory is charged under
 */
record ExecutionCharge(LeafExecutionEstimator estimator, String label) {
    /**
     * Looks through the wrappers that create their child's weight directly: {@link BoostQuery},
     * {@link IndexSortSortedNumericDocValuesRangeQuery} and {@link IndexOrDocValuesQuery}. Their children never reach the searcher
     * on their own, so this does not double-charge.
     * {@link ConstantScoreQuery} must not be unwrapped: it creates its child's weight through
     * {@link ContextIndexSearcher#createWeight}, so the child is already charged there, and charging it here would double-charge.
     * {@code NamedMatches.NamedQuery} hides its child in a private class with no getter, so a named query is not charged.
     *
     * @return the charge for {@code query}'s execution memory, or {@code null} if it allocates none worth charging
     */
    @Nullable
    static ExecutionCharge forQuery(Query query) {
        if (query instanceof BoostQuery boostQuery) {
            return forQuery(boostQuery.getQuery());
        }
        if (query instanceof IndexSortSortedNumericDocValuesRangeQuery indexSortQuery) {
            final ExecutionCharge fallbackCharge = forQuery(indexSortQuery.getFallbackQuery());
            return fallbackCharge == null
                ? null
                : new ExecutionCharge(fallbackSideOnly(fieldOf(indexSortQuery), fallbackCharge.estimator()), fallbackCharge.label());
        }
        if (query instanceof IndexOrDocValuesQuery iodvq) {
            // IndexOrDocValuesQuery creates its index query's weight directly, so that query is never looked up on its own.
            final ExecutionCharge indexCharge = forAllocatingQuery(iodvq.getIndexQuery());
            return indexCharge == null ? null : new ExecutionCharge(indexSideOnly(indexCharge.estimator()), indexCharge.label());
        }
        return forAllocatingQuery(query);
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
        return (ctx, scorer, leadCost) -> (scorer.cost() >>> 3) <= leadCost ? indexEstimator.bytesForLeaf(ctx, scorer, leadCost) : 0L;
    }

    /**
     * Sizes the fallback of an {@link IndexSortSortedNumericDocValuesRangeQuery}, which allocates only on the leaves it cannot
     * answer from the index sort. On the others it iterates a doc id range and allocates nothing.
     */
    private static LeafExecutionEstimator fallbackSideOnly(String field, LeafExecutionEstimator fallbackEstimator) {
        return (ctx, scorer, leadCost) -> answeredFromIndexSort(ctx.reader(), field)
            ? 0L
            : fallbackEstimator.bytesForLeaf(ctx, scorer, leadCost);
    }

    /**
     * Whether {@link IndexSortSortedNumericDocValuesRangeQuery} answers on {@code reader} without running its fallback: single-valued
     * doc values on a field that is the primary {@code INT} or {@code LONG} index sort. Lucene also takes the shortcut on some leaves
     * this misses (e.g. through the points of another sort type); those are charged as if the fallback ran, which only over-charges.
     */
    private static boolean answeredFromIndexSort(LeafReader reader, String field) throws IOException {
        final Sort indexSort = reader.getMetaData().sort();
        if (indexSort == null || indexSort.getSort().length == 0) {
            return false;
        }
        final SortField primarySort = indexSort.getSort()[0];
        if (primarySort.getField().equals(field) == false) {
            return false;
        }
        final SortField.Type type = primarySort instanceof SortedNumericSortField sortedNumericSort
            ? sortedNumericSort.getNumericType()
            : primarySort.getType();
        if (type != SortField.Type.INT && type != SortField.Type.LONG) {
            return false;
        }
        return DocValues.unwrapSingleton(DocValues.getSortedNumeric(reader, field)) != null;
    }
}
