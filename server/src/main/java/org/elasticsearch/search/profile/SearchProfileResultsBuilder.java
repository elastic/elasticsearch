/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.search.SearchPhaseResult;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Profile results for the query phase run on all shards.
 */
public class SearchProfileResultsBuilder {

    private final Map<String, ProfileResult> dfsProfileResults;
    private final Map<String, SearchProfileQueryPhaseResult> queryProfileResults;
    private final Map<String, ProfileResult> fetchProfileResults;

    public SearchProfileResultsBuilder() {
        this.dfsProfileResults = new HashMap<>();
        this.queryProfileResults = new HashMap<>();
        this.fetchProfileResults = new HashMap<>();
    }

    public SearchProfileResultsBuilder setQueryProfileResults(Collection<? extends SearchPhaseResult> queryResults) {
        for (SearchPhaseResult result : queryResults) {
            String key = result.getSearchShardTarget().toString();
            SearchProfileQueryPhaseResult value = result.queryResult().consumeProfileResult();
            queryProfileResults.put(key, value);

            if (value.getDfsProfileResult() != null) {
                dfsProfileResults.put(key, value.getDfsProfileResult());
            }
        }

        return this;
    }

    public SearchProfileResultsBuilder setFetchProfileResults(Collection<? extends SearchPhaseResult> fetchResults) {
        for (SearchPhaseResult result : fetchResults) {
            String key = result.getSearchShardTarget().toString();
            ProfileResult value = result.fetchResult().profileResult();
            fetchProfileResults.put(key, value);
        }

        return this;
    }

    /**
     * Merge the profiling information for all results.
     */
    public SearchProfileResults build() {
        Map<String, SearchProfileShardResult> mergedShardResults = Maps.newMapWithExpectedSize(queryProfileResults.size());

        for (Map.Entry<String, ProfileResult> fetchProfileEntry : fetchProfileResults.entrySet()) {
            SearchProfileQueryPhaseResult queryProfileResult = queryProfileResults.get(fetchProfileEntry.getKey());

            if (queryProfileResult == null) {
                throw new IllegalStateException(
                    "Profile returned fetch phase information for ["
                        + fetchProfileEntry.getKey()
                        + "] but didn't return query phase information. Query phase keys were "
                        + queryProfileResults.keySet()
                );
            }

            ProfileResult dfsProfileResult = dfsProfileResults.get(fetchProfileEntry.getKey());

            mergedShardResults.put(
                fetchProfileEntry.getKey(),
                new SearchProfileShardResult(dfsProfileResult, queryProfileResult, fetchProfileEntry.getValue())
            );
        }

        for (Map.Entry<String, SearchProfileQueryPhaseResult> queryProfileEntry : queryProfileResults.entrySet()) {
            if (false == mergedShardResults.containsKey(queryProfileEntry.getKey())) {
                ProfileResult dfsProfileResult = dfsProfileResults.get(queryProfileEntry.getKey());
                mergedShardResults.put(
                    queryProfileEntry.getKey(),
                    new SearchProfileShardResult(dfsProfileResult, queryProfileEntry.getValue(), null)
                );
            }
        }

        return new SearchProfileResults(mergedShardResults);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchProfileResultsBuilder that = (SearchProfileResultsBuilder) o;
        return Objects.equals(dfsProfileResults, that.dfsProfileResults)
            && Objects.equals(queryProfileResults, that.queryProfileResults)
            && Objects.equals(fetchProfileResults, that.fetchProfileResults);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dfsProfileResults, queryProfileResults, fetchProfileResults);
    }
}
