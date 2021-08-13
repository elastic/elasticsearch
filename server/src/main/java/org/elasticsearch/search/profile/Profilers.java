/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile;

import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.profile.aggregation.AggregationProfiler;
import org.elasticsearch.search.profile.query.QueryProfiler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Wrapper around all the profilers that makes management easier. */
public final class Profilers {

    private final ContextIndexSearcher searcher;
    private final List<QueryProfiler> queryProfilers;
    private final AggregationProfiler aggProfiler;

    /** Sole constructor. This {@link Profilers} instance will initially wrap one {@link QueryProfiler}. */
    public Profilers(ContextIndexSearcher searcher) {
        this.searcher = searcher;
        this.queryProfilers = new ArrayList<>();
        this.aggProfiler = new AggregationProfiler();
        addQueryProfiler();
    }

    /** Switch to a new profile. */
    public QueryProfiler addQueryProfiler() {
        QueryProfiler profiler = new QueryProfiler();
        searcher.setProfiler(profiler);
        queryProfilers.add(profiler);
        return profiler;
    }

    /** Get the current profiler. */
    public QueryProfiler getCurrentQueryProfiler() {
        return queryProfilers.get(queryProfilers.size() - 1);
    }

    /** Return the list of all created {@link QueryProfiler}s so far. */
    public List<QueryProfiler> getQueryProfilers() {
        return Collections.unmodifiableList(queryProfilers);
    }

    /** Return the {@link AggregationProfiler}. */
    public AggregationProfiler getAggregationProfiler() {
        return aggProfiler;
    }

}
