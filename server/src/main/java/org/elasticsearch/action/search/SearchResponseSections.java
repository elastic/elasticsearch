/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.SimpleRefCounted;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.profile.SearchProfileResults;
import org.elasticsearch.search.profile.SearchProfileShardResult;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.transport.LeakTracker;

import java.util.Collections;
import java.util.Map;

/**
 * Holds some sections that a search response is composed of (hits, aggs, suggestions etc.) during some steps of the search response
 * building.
 */
public class SearchResponseSections implements RefCounted {

    public static final SearchResponseSections EMPTY_WITH_TOTAL_HITS = new SearchResponseSections(
        SearchHits.EMPTY_WITH_TOTAL_HITS,
        null,
        null,
        false,
        null,
        null,
        1
    );
    public static final SearchResponseSections EMPTY_WITHOUT_TOTAL_HITS = new SearchResponseSections(
        SearchHits.EMPTY_WITHOUT_TOTAL_HITS,
        null,
        null,
        false,
        null,
        null,
        1
    );
    protected final SearchHits hits;
    protected final InternalAggregations aggregations;
    protected final Suggest suggest;
    protected final SearchProfileResults profileResults;
    protected final boolean timedOut;
    protected final Boolean terminatedEarly;
    protected final int numReducePhases;

    private final RefCounted refCounted;

    public SearchResponseSections(
        SearchHits hits,
        InternalAggregations aggregations,
        Suggest suggest,
        boolean timedOut,
        Boolean terminatedEarly,
        SearchProfileResults profileResults,
        int numReducePhases
    ) {
        this.hits = hits;
        hits.incRef();
        this.aggregations = aggregations;
        this.suggest = suggest;
        this.profileResults = profileResults;
        this.timedOut = timedOut;
        this.terminatedEarly = terminatedEarly;
        this.numReducePhases = numReducePhases;
        refCounted = hits.getHits().length > 0 ? LeakTracker.wrap(new SimpleRefCounted()) : ALWAYS_REFERENCED;
    }

    public final SearchHits hits() {
        return hits;
    }

    public final Suggest suggest() {
        return suggest;
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

    @Override
    public void incRef() {
        refCounted.incRef();
    }

    @Override
    public boolean tryIncRef() {
        return refCounted.tryIncRef();
    }

    @Override
    public boolean decRef() {
        if (refCounted.decRef()) {
            hits.decRef();
            return true;
        }
        return false;
    }

    @Override
    public boolean hasReferences() {
        return refCounted.hasReferences();
    }
}
