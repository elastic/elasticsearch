/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile;

import org.elasticsearch.search.fetch.FetchProfiler;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.profile.aggregation.AggregationProfileShardResult;
import org.elasticsearch.search.profile.aggregation.AggregationProfiler;
import org.elasticsearch.search.profile.dfs.DfsProfiler;
import org.elasticsearch.search.profile.query.QueryProfileShardResult;
import org.elasticsearch.search.profile.query.QueryProfiler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Wrapper around all the profilers that makes management easier. */
public final class Profilers {

    private final ContextIndexSearcher searcher;
    private final List<QueryProfiler> queryProfilers = new ArrayList<>();
    private final AggregationProfiler aggProfiler = new AggregationProfiler();
    private DfsProfiler dfsProfiler;

    public Profilers(ContextIndexSearcher searcher) {
        this.searcher = searcher;
        addQueryProfiler();
    }

    /**
     * Begin profiling a new query.
     */
    public QueryProfiler addQueryProfiler() {
        QueryProfiler profiler = new QueryProfiler();
        searcher.setProfiler(profiler);
        queryProfilers.add(profiler);
        return profiler;
    }

    /**
     * Get the profiler for the query we are currently processing.
     */
    public QueryProfiler getCurrentQueryProfiler() {
        return queryProfilers.get(queryProfilers.size() - 1);
    }

    /**
     * The list of all {@link QueryProfiler}s created so far.
     */
    public List<QueryProfiler> getQueryProfilers() {
        return Collections.unmodifiableList(queryProfilers);
    }

    public AggregationProfiler getAggregationProfiler() {
        return aggProfiler;
    }

    /**
     * Build a profiler for the dfs phase or get the existing one.
     */
    public DfsProfiler getDfsProfiler() {
        if (dfsProfiler == null) {
            dfsProfiler = new DfsProfiler();
        }

        return dfsProfiler;
    }

    /**
     * Build a profiler for the fetch phase.
     */
    public static FetchProfiler startProfilingFetchPhase() {
        return new FetchProfiler();
    }

    /**
     * Build the results for the query phase.
     */
    public SearchProfileQueryPhaseResult buildQueryPhaseResults() {
        List<QueryProfileShardResult> queryResults = new ArrayList<>(queryProfilers.size());
        for (QueryProfiler queryProfiler : queryProfilers) {
            QueryProfileShardResult result = new QueryProfileShardResult(
                queryProfiler.getTree(),
                queryProfiler.getRewriteTime(),
                queryProfiler.getCollectorResult()
            );
            queryResults.add(result);
        }
        AggregationProfileShardResult aggResults = new AggregationProfileShardResult(aggProfiler.getTree());
        return new SearchProfileQueryPhaseResult(queryResults, aggResults);
    }
}
