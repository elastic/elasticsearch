package org.elasticsearch.search.profile;

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.profile.aggregation.AggregationProfileShardResult;
import org.elasticsearch.search.profile.aggregation.AggregationProfiler;
import org.elasticsearch.search.profile.query.QueryProfileShardResult;
import org.elasticsearch.search.profile.query.QueryProfiler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A container class to hold all the profile results across all shards.  Internally
 * holds a map of shard ID -&gt; Profiled results
 */
public final class SearchProfileShardResults implements Writeable {  // Rename to SearchProfileQueryPhaseResults
    private Map<String, SearchProfileQueryPhaseResult> shardResults;

    public SearchProfileShardResults(Map<String, SearchProfileQueryPhaseResult> shardResults) {
        this.shardResults =  Collections.unmodifiableMap(shardResults);
    }

    public SearchProfileShardResults(StreamInput in) throws IOException {
        int size = in.readInt();
        shardResults = new HashMap<>(size);

        for (int i = 0; i < size; i++) {
            String key = in.readString();
            SearchProfileQueryPhaseResult shardResult = new SearchProfileQueryPhaseResult(in);
            shardResults.put(key, shardResult);
        }
        shardResults = Collections.unmodifiableMap(shardResults);
    }

    /**
     * Merge the profiling information from some fetch results into this
     * profiling information.
     */
    public SearchProfileResults merge(Collection<? extends SearchPhaseResult> fetchResults) {
        Map<String, SearchProfileShardResult> mergedShardResults = new HashMap<>(shardResults.size());
        for (SearchPhaseResult r : fetchResults) {
            FetchSearchResult fr = r.fetchResult();
            String key = fr.getSearchShardTarget().toString();
            SearchProfileQueryPhaseResult search = shardResults.get(key);
            if (search == null) {
                throw new IllegalStateException(
                    "Profile returned fetch information for ["
                        + key
                        + "] but didn't return search information. Search keys were "
                        + shardResults.keySet()
                );
            }
            mergedShardResults.put(key, new SearchProfileShardResult(search, fr.profileResult()));
        }
        for (Map.Entry<String, SearchProfileQueryPhaseResult> e : shardResults.entrySet()) {
            if (false == mergedShardResults.containsKey(e.getKey())) {
                mergedShardResults.put(e.getKey(), new SearchProfileShardResult(e.getValue(), null));
            }
        }
        return new SearchProfileResults(mergedShardResults);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(shardResults.size());
        for (Map.Entry<String, SearchProfileQueryPhaseResult> entry : shardResults.entrySet()) {
            out.writeString(entry.getKey());
            entry.getValue().writeTo(out);
        }
    }

    /**
     * Helper method to convert Profiler into InternalProfileShardResults, which
     * can be serialized to other nodes, emitted as JSON, etc.
     *
     * @param profilers
     *            The {@link Profilers} to convert into results
     * @return A {@link SearchProfileQueryPhaseResult} representing the results for this
     *         shard
     */
    public static SearchProfileQueryPhaseResult buildShardResults(Profilers profilers) {
        List<QueryProfiler> queryProfilers = profilers.getQueryProfilers();
        AggregationProfiler aggProfiler = profilers.getAggregationProfiler();
        List<QueryProfileShardResult> queryResults = new ArrayList<>(queryProfilers.size());
        for (QueryProfiler queryProfiler : queryProfilers) {
            QueryProfileShardResult result = new QueryProfileShardResult(queryProfiler.getTree(), queryProfiler.getRewriteTime(),
                    queryProfiler.getCollector());
            queryResults.add(result);
        }
        AggregationProfileShardResult aggResults = new AggregationProfileShardResult(aggProfiler.getTree());
        return new SearchProfileQueryPhaseResult(queryResults, aggResults);
    }
}
