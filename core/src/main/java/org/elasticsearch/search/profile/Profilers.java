/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
