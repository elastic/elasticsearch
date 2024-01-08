/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.profile.SearchProfileResults;
import org.elasticsearch.search.profile.SearchProfileShardResult;
import org.elasticsearch.search.suggest.Suggest;

import java.util.Collections;
import java.util.Map;

/**
 * Holds some sections that a search response is composed of (hits, aggs, suggestions etc.) during some steps of the search response
 * building.
 */
public class SearchResponseSections {

    protected final SearchHits hits;
    protected final Aggregations aggregations;
    protected final Suggest suggest;
    protected final SearchProfileResults profileResults;
    protected final boolean timedOut;
    protected final Boolean terminatedEarly;
    protected final int numReducePhases;

    public SearchResponseSections(
        SearchHits hits,
        Aggregations aggregations,
        Suggest suggest,
        boolean timedOut,
        Boolean terminatedEarly,
        SearchProfileResults profileResults,
        int numReducePhases
    ) {
        this.hits = hits;
        this.aggregations = aggregations;
        this.suggest = suggest;
        this.profileResults = profileResults;
        this.timedOut = timedOut;
        this.terminatedEarly = terminatedEarly;
        this.numReducePhases = numReducePhases;
    }

    public final boolean timedOut() {
        return this.timedOut;
    }

    public final Boolean terminatedEarly() {
        return this.terminatedEarly;
    }

    public final SearchHits hits() {
        return hits;
    }

    public final Aggregations aggregations() {
        return aggregations;
    }

    public final Suggest suggest() {
        return suggest;
    }

    /**
     * Returns the number of reduce phases applied to obtain this search response
     */
    public final int getNumReducePhases() {
        return numReducePhases;
    }

    /**
     * Returns the profile results for this search response (including all shards).
     * An empty map is returned if profiling was not enabled
     *
     * @return Profile results
     */
    public final Map<String, SearchProfileShardResult> profile() {
        if (profileResults == null) {
            return Collections.emptyMap();
        }
        return profileResults.getShardResults();
    }
}
