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
import org.elasticsearch.search.fetch.FetchSearchResult;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * Profile results for the query phase run on all shards.
 */
public class SearchProfileResultsBuilder {
    private final Map<String, SearchProfileQueryPhaseResult> queryPhaseResults;

    public SearchProfileResultsBuilder(Map<String, SearchProfileQueryPhaseResult> queryPhaseResults) {
        this.queryPhaseResults = Collections.unmodifiableMap(queryPhaseResults);
    }

    /**
     * Merge the profiling information from some fetch results into this
     * profiling information.
     */
    public SearchProfileResults build(Collection<? extends SearchPhaseResult> fetchResults) {
        Map<String, SearchProfileShardResult> mergedShardResults = Maps.newMapWithExpectedSize(queryPhaseResults.size());
        for (SearchPhaseResult r : fetchResults) {
            FetchSearchResult fr = r.fetchResult();
            String key = fr.getSearchShardTarget().toString();
            SearchProfileQueryPhaseResult queryPhase = queryPhaseResults.get(key);
            if (queryPhase == null) {
                throw new IllegalStateException(
                    "Profile returned fetch phase information for ["
                        + key
                        + "] but didn't return query phase information. Query phase keys were "
                        + queryPhaseResults.keySet()
                );
            }
            mergedShardResults.put(key, new SearchProfileShardResult(queryPhase, fr.profileResult()));
        }
        for (Map.Entry<String, SearchProfileQueryPhaseResult> e : queryPhaseResults.entrySet()) {
            if (false == mergedShardResults.containsKey(e.getKey())) {
                mergedShardResults.put(e.getKey(), new SearchProfileShardResult(e.getValue(), null));
            }
        }
        return new SearchProfileResults(mergedShardResults);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        SearchProfileResultsBuilder other = (SearchProfileResultsBuilder) obj;
        return queryPhaseResults.equals(other.queryPhaseResults);
    }

    @Override
    public int hashCode() {
        return queryPhaseResults.hashCode();
    }
}
