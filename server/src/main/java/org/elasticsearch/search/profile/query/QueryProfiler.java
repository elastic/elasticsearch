/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.profile.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.profile.AbstractProfiler;
import org.elasticsearch.search.profile.Timer;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * This class acts as a thread-local storage for profiling a query.  It also
 * builds a representation of the query tree which is built constructed
 * "online" as the weights are wrapped by ContextIndexSearcher.  This allows us
 * to know the relationship between nodes in tree without explicitly
 * walking the tree or pre-wrapping everything
 *
 * A Profiler is associated with every Search, not per Search-Request. E.g. a
 * request may execute two searches (query + global agg).  A Profiler just
 * represents one of those
 */
public final class QueryProfiler extends AbstractProfiler<QueryProfileBreakdown, Query> {

    /**
     * The root CollectorResult used in the search
     */
    private CollectorResult collectorResult;

    private long vectorOpsCount;

    /**
     * One entry per kNN query subtree that self-published to this profiler. A single search can carry
     * several kNN queries (e.g. a {@code bool} with multiple {@code knn} clauses), so breakdowns are
     * accumulated rather than overwritten. Wrapper queries (rescore) augment the most recently appended
     * entry via {@link #getLastKnnProfileBreakdown()} / {@link #setLastKnnProfileBreakdown(Map)}.
     */
    private final List<Map<String, Object>> knnProfileBreakdowns = new ArrayList<>();

    public QueryProfiler() {
        super(new InternalQueryProfileTree());
    }

    /**
     * Adds a number of vector operations to the current count
     * @param vectorOpsCount number of vector ops to add to the profiler
     */
    public void addVectorOpsCount(long vectorOpsCount) {
        this.vectorOpsCount += vectorOpsCount;
    }

    /**
     * Retrieves the number of vector operations performed by the queries
     * @return number of vector operations performed by the queries
     */
    public long getVectorOpsCount() {
        return this.vectorOpsCount;
    }

    /**
     * Appends a new kNN profile breakdown for a distinct kNN query subtree. Called by each ES kNN query
     * (and {@link org.elasticsearch.search.vectors.PostFilterKnnQuery}) when it self-publishes.
     */
    public void addKnnProfileBreakdown(Map<String, Object> knnProfileBreakdown) {
        this.knnProfileBreakdowns.add(knnProfileBreakdown);
    }

    /**
     * Returns the most recently appended kNN profile breakdown, or {@code null} if none. Used by wrapper
     * queries (rescore) to merge their section into the breakdown their inner query just published.
     */
    public Map<String, Object> getLastKnnProfileBreakdown() {
        return knnProfileBreakdowns.isEmpty() ? null : knnProfileBreakdowns.get(knnProfileBreakdowns.size() - 1);
    }

    /**
     * Replaces the most recently appended kNN profile breakdown (used after a wrapper merges its section).
     * If none exists yet, the breakdown is appended so a wrapper over a non-ES inner query is not lost.
     */
    public void setLastKnnProfileBreakdown(Map<String, Object> knnProfileBreakdown) {
        if (knnProfileBreakdowns.isEmpty()) {
            knnProfileBreakdowns.add(knnProfileBreakdown);
        } else {
            knnProfileBreakdowns.set(knnProfileBreakdowns.size() - 1, knnProfileBreakdown);
        }
    }

    /**
     * Returns all kNN profile breakdowns published to this profiler, in publish order (may be empty).
     */
    public List<Map<String, Object>> getKnnProfileBreakdowns() {
        return knnProfileBreakdowns;
    }

    /**
     * Returns the accumulated kNN breakdowns collapsed into the single {@code knn_profile} map that is
     * serialized: {@code null} when none, the single breakdown when there is exactly one, and a
     * {@code {"knn_queries": [...]}} wrapper when a single search carried several kNN queries (e.g. a
     * {@code bool} with multiple {@code knn} clauses).
     */
    @Nullable
    public Map<String, Object> getKnnProfileBreakdown() {
        if (knnProfileBreakdowns.isEmpty()) {
            return null;
        }
        if (knnProfileBreakdowns.size() == 1) {
            return knnProfileBreakdowns.get(0);
        }
        Map<String, Object> combined = new LinkedHashMap<>();
        combined.put("knn_queries", new ArrayList<>(knnProfileBreakdowns));
        return combined;
    }

    /** Set the collector result that is associated with this profiler. */
    public void setCollectorResult(CollectorResult collectorResult) {
        if (this.collectorResult != null) {
            throw new IllegalStateException("The collector result can only be set once.");
        }
        this.collectorResult = requireNonNull(collectorResult);
    }

    /**
     * Begin timing the rewrite phase of a request.  All rewrites are accumulated together into a
     * single metric
     */
    public Timer startRewriteTime() {
        return ((InternalQueryProfileTree) profileTree).startRewriteTime();
    }

    /**
     * Stop recording the current rewrite and add it's time to the total tally, returning the
     * cumulative time so far.
     *
     * @return cumulative rewrite time
     */
    public long stopAndAddRewriteTime(Timer rewriteTimer) {
        return ((InternalQueryProfileTree) profileTree).stopAndAddRewriteTime(requireNonNull(rewriteTimer));
    }

    /**
     * @return total time taken to rewrite all queries in this profile
     */
    public long getRewriteTime() {
        return ((InternalQueryProfileTree) profileTree).getRewriteTime();
    }

    /**
     * Return the current root Collector for this search
     */
    public CollectorResult getCollectorResult() {
        return this.collectorResult;
    }

}
