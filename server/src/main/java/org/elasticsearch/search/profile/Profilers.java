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

import java.util.Collections;

/** Wrapper around all the profilers that makes management easier. */
public final class Profilers {

    private final ContextIndexSearcher searcher;
    private final QueryProfiler queryProfiler;
    private final AggregationProfiler aggProfiler = new AggregationProfiler();
    private DfsProfiler dfsProfiler;

    public Profilers(ContextIndexSearcher searcher) {
        this.searcher = searcher;
        QueryProfiler profiler = new QueryProfiler();
        searcher.setProfiler(profiler);
        queryProfiler = profiler;
    }

    /**
     * Get the profiler for the query we are currently processing.
     */
    public QueryProfiler getCurrentQueryProfiler() {
        return queryProfiler;
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
        QueryProfileShardResult result = new QueryProfileShardResult(
            queryProfiler.getTree(),
            queryProfiler.getRewriteTime(),
            queryProfiler.getCollector()
        );
        AggregationProfileShardResult aggResults = new AggregationProfileShardResult(aggProfiler.getTree());
        return new SearchProfileQueryPhaseResult(Collections.singletonList(result), aggResults);
    }
}
