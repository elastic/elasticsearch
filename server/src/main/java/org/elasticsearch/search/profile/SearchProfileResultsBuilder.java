package org.elasticsearch.search.profile;

import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.fetch.FetchSearchResult;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Profile results for the query phase run on all shards.
 */
public final class SearchProfileResultsBuilder {
    private final Map<String, SearchProfileQueryPhaseResult> shardResults;

    public SearchProfileResultsBuilder(Map<String, SearchProfileQueryPhaseResult> shardResults) {
        this.shardResults = Collections.unmodifiableMap(shardResults);
    }

    /**
     * Merge the profiling information from some fetch results into this
     * profiling information.
     */
    public SearchProfileResults build(Collection<? extends SearchPhaseResult> fetchResults) {
        Map<String, SearchProfileShardResult> mergedShardResults = new HashMap<>(shardResults.size());
        for (SearchPhaseResult r : fetchResults) {
            FetchSearchResult fr = r.fetchResult();
            String key = fr.getSearchShardTarget().toString();
            SearchProfileQueryPhaseResult queryPhase = shardResults.get(key);
            if (queryPhase == null) {
                throw new IllegalStateException(
                    "Profile returned fetch phase information for ["
                        + key
                        + "] but didn't return query phase information. Query phase keys were "
                        + shardResults.keySet()
                );
            }
            mergedShardResults.put(key, new SearchProfileShardResult(queryPhase, fr.profileResult()));
        }
        for (Map.Entry<String, SearchProfileQueryPhaseResult> e : shardResults.entrySet()) {
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
        return shardResults.equals(other.shardResults);
    }

    @Override
    public int hashCode() {
        return shardResults.hashCode();
    }
}
