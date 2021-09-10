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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Profile results for the query phase run on all shards.
 */
public final class SearchProfileResultsBuilder implements Writeable { // This is Writeable for backwards compatibility
    private Map<String, SearchProfileQueryPhaseResult> shardResults;

    public SearchProfileResultsBuilder(Map<String, SearchProfileQueryPhaseResult> shardResults) {
        this.shardResults =  Collections.unmodifiableMap(shardResults);
    }

    public SearchProfileResultsBuilder(StreamInput in) throws IOException {
        int size = in.readInt();
        shardResults = new HashMap<>(size);

        for (int i = 0; i < size; i++) {
            String key = in.readString();
            SearchProfileQueryPhaseResult shardResult = new SearchProfileQueryPhaseResult(in);
            shardResults.put(key, shardResult);
        }
        shardResults = Collections.unmodifiableMap(shardResults);
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
